#include "ktruss.h"
#include <algorithm>
#include <emu_cxx_utils/intrinsics.h>
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>

#define VERBOSE_LOGGING

#ifdef VERBOSE_LOGGING
#define DEBUG(...) LOG(__VA_ARGS__)
#else
#define DEBUG(...)
#endif

using namespace emu;
using namespace emu::parallel;

ktruss::ktruss(ktruss_graph & g)
: g_(&g)
, active_edges_end_(g.num_vertices())
, vertex_max_k_(g.num_vertices())
, worklist_(g.num_vertices())
{}

ktruss::ktruss(const ktruss& other, emu::shallow_copy shallow)
: g_(other.g_)
, active_edges_end_(other.active_edges_end_, shallow)
, vertex_max_k_(other.vertex_max_k_, shallow)
, worklist_(other.worklist_, shallow)
{}

void
ktruss::clear()
{
    g_->for_each_vertex(dyn, [this](long src) {
        // Shrink edge lists to only consider edges that connect to vertices
        // with lower vertex ID's. Essentially making this a directed graph
        active_edges_end_[src] = std::lower_bound(
            g_->out_edges_begin(src), g_->out_edges_end(src), src);
        // Init vertex K values to zero
        vertex_max_k_[src] = 0;
        // Init all edge property values to zero
        g_->for_each_out_edge(src, [](ktruss_edge_slot &dst) {
            dst.TC = 0;
            dst.qrC = 0;
            dst.KTE = 0;
        });
    });
}

/**
 * Searches for an edge in the active graph using binary search
 * @param src Source vertex (assumes src > dst)
 * @param dst Destination vertex (assumes src > dst)
 * @return Pointer to edge, or else null if the edge is not present
 */
ktruss_graph::edge_type *
ktruss::find_edge(long src, long dst)
{
    assert(src > dst);
    auto begin = g_->out_edges_begin(src);
    auto end = active_edges_end_[src];
    auto edge = std::lower_bound(begin, end, dst);
    if (edge == end || *edge != dst) {
        return nullptr;
    } else {
        return edge;
    }
}

/**
 * Gets a pointer to the in-edge from q to p
 *
 * For most of this algorithm, we treat the graph as directed, traversing
 * edges only in the direction from higher to lower vertex ID.
 *
 * But we still built an undirected graph, which means that the reverse
 * edges are still present. We use these reverse edges to store the pRefC
 * counter.
 *
 * This function should never fail to find the edge. It is used
 * in situations where we already have the edge p->q, but we want to
 * updating a counter in q->p for efficient reverse iteration. Also note that
 * reverse edges are never deleted, so the user should verify that p->q has
 * not been removed before relying on this.
 * @param q
 * @param p
 * @return Pointer to edge p in q's edge list
 */
ktruss_graph::edge_type *
ktruss::find_reverse_edge(long q, long p)
{
    assert(p > q);
    auto begin = g_->out_edges_begin(q);
    auto end = g_->out_edges_end(q);
    auto edge = std::lower_bound(begin, end, p);
    assert(edge != end);
    assert(*edge == p);
    return edge;
}

void
ktruss::build_worklist()
{
    worklist_.clear_all();
    g_->for_each_vertex(fixed, [this](long p) {
        // Add all active edges of p to the worklist
        auto q_begin = g_->out_edges_begin(p);
        auto q_end = active_edges_end_[p];
        if (q_begin != q_end) {
            worklist_.append(p, q_begin, q_end);
        }
    });
}

void
ktruss::count_initial_triangles()
{
    build_worklist();
    // Process all p->q edges with a thread pool
    worklist_.process_all(dyn, [this](long p, ktruss_edge_slot& pq) {
        long q = pq.dst;
        // Range of q's neighbors that are less than q
        auto qr_begin = g_->out_edges_begin(q);
        auto qr_end = active_edges_end_[q];
        // Iterator over edges of p
        auto pr = g_->out_edges_begin(p);

        for (auto qr = qr_begin; qr != qr_end; ++qr){
            while (*pr < *qr) { pr++; }
            if (*qr == *pr) {
                // Found the triangle p->q->r
                DEBUG("Triangle %li->%li->%li\n", p, q, qr->dst);
                emu::remote_add(&qr->TC, 1);
                emu::remote_add(&pq.TC, 1);
                emu::remote_add(&pr->TC, 1);
                emu::remote_add(&qr->qrC, 1);
                emu::remote_add(&find_reverse_edge(q, p)->pRefC, 1);
            }
        }
    });
}

/**
 * Recall p>q>r
 *
 * If we find an edge p->q that will be removed,
 * Let r be the first neighbor of p that is less than q
 * Search and unroll all triangles
 *
 * If there is a p->q that will NOT be removed, there may
 * still be a p->r that will remove the triangle p->q->r
 *
 * @param k
 */

void
ktruss::unroll_wedges(long k)
{
    build_worklist();
    // Process all p->q edges with a thread pool
    worklist_.process_all(dyn, [this, k](long p, ktruss_edge_slot& pq)
    {
        long q = pq.dst;
        auto qr = g_->out_edges_begin(q);
        auto qr_end = active_edges_end_[q];
#ifdef VERBOSE_LOGGING
        if (pq.TC  < k - 2) { LOG("(p->q) remove %li->%li\n", p, q); }
#endif
        // Iterate over all wedges p->(q, r) where q > r
        for (auto pr = g_->out_edges_begin(p); *pr < pq.dst; ++pr) {
            // Will p->q or p->r be removed?
            if (pq.TC < k - 2 || pr->TC < k - 2) {
#ifdef VERBOSE_LOGGING
                if (pq.TC >= k - 2 && pr->TC < k - 2) { LOG("(p->r) remove %li->%li\n", p, pr->dst); }
#endif
                // Search for q->r edge to complete the triangle
                while (*qr < *pr) { ++qr; }
                if (qr >= qr_end) { break; }
                if (*qr == *pr) {
                    // Unroll triangle
                    DEBUG("Unrolling %li->%li->%li\n", p, q, pr->dst);
                    emu::remote_add(&qr->TC, -1);
                    emu::remote_add(&pq.TC, -1);
                    emu::remote_add(&pr->TC, -1);
                    emu::remote_add(&qr->qrC, -1);
                    emu::remote_add(&find_reverse_edge(q, p)->pRefC, -1);
                }
            }
        }
    });
}

void
ktruss::unroll_supported_triangles(long k)
{
    build_worklist();
    // Process all q->r edges with a thread pool
    worklist_.process_all(dyn, [this, k](long q, ktruss_edge_slot& qr)
    {
        long r = qr.dst;
        assert(qr.TC >= 0);
        assert(qr.qrC >= 0);
        // Will this edge be removed?
        // Does it support any triangles as a q->r edge?
        if (qr.TC < k - 2 && qr.qrC > 0) {
            DEBUG("(q->r) remove %li->%li\n", q, qr.dst);
            // Iterate over q->p edges
            // This is the range of q's neighbors that are GREATER than q
            // i.e. we are looking at edges in the other direction
            auto qp_end = g_->out_edges_end(q);
            auto qp_begin = std::upper_bound(g_->out_edges_begin(q), qp_end, q);
            for (auto qp = qp_begin; qp != qp_end; ++qp){
                if (qp->pRefC > 0) {
                    long p = qp->dst;
                    // Find the edge p->r to complete the p->q->r triangle
                    auto pr = find_edge(p, r);
                    // We also need to find pq, since edges won't be removed
                    // from the reverse set
                    auto pq = find_edge(p, q);
                    // TODO we can exit early if pr is not found
                    // TODO we can reuse the same iterator to find pr and pq
                    if (pr && pq) {
                        // Unroll supported triangle
                        DEBUG("Unrolling %li->%li->%li\n", p, q, r);
                        emu::remote_add(&qr.TC, -1);
                        emu::remote_add(&pq->TC, -1);
                        emu::remote_add(&pr->TC, -1);
                        emu::remote_add(&qr.qrC, -1);
                        // TODO should we decrement the reference count?
                    }
                }
            }
        }
    });
}

long
ktruss::remove_edges(long k)
{
    // TODO use reduction variable here?
    long num_removed = 0;
    g_->for_each_vertex(emu::dyn, [&](long v){
        // Get the edge list for this vertex
        auto begin = g_->out_edges_begin(v);
        auto end = active_edges_end_[v];
        // Move all edges with TC == 0 to the end of the list
        auto remove_begin = std::stable_partition(begin, end,
            [](ktruss_edge_slot& e) {
                return e.TC != 0;
            }
        );
        if (remove_begin != end) {
            // Set K for the edges we are about to remove
            for_each(remove_begin, end, [=](ktruss_edge_slot &e) {
                assert(e.TC == 0);
                assert(e.qrC == 0);
                DEBUG("Removed %li->%li, KTE=%li\n", v, e.dst, k-1);
                e.KTE = k - 1;
            });
            // Resize the edge list to drop the edges off the end
            active_edges_end_[v] = remove_begin;
            // Increment count of edges that were removed
            emu::remote_add(&num_removed, end - remove_begin);
        }
    });
    return num_removed;
}

ktruss::stats
ktruss::compute_truss_sizes(long max_k)
{
    stats s(max_k);

    // Build the worklist by hand to include all edges that were removed
    worklist_.clear_all();
    g_->for_each_vertex(fixed, [this](long p) {
        // Use binary search to find neighbors of p that are less than p
        auto q_begin = g_->out_edges_begin(p);
        auto q_end = std::lower_bound(q_begin, g_->out_edges_end(p), p);
        // Add these edges to the work list
        if (q_begin != q_end) {
            worklist_.append(p, q_begin, q_end);
        }
    });

    // Process all p->q edges with a thread pool
    worklist_.process_all(dyn, [&](long src, ktruss_edge_slot& e)
    {
        long dst = e.dst;
        assert(e.KTE >= 2);
        assert(e.KTE <= max_k);
        // This vertex is a member of all trusses up through k
        emu::remote_max(&vertex_max_k_[src], e.KTE);
        emu::remote_max(&vertex_max_k_[dst], e.KTE);
        // This edge is a member of all trusses up through k
        for (long k = 2; k <= e.KTE; ++k) {
            emu::remote_add(&s.edges_per_truss[k-2], 1);
        }
    });

    g_->for_each_vertex(fixed, [&](long v) {
        // This edge is a member of all trusses up through k
        for (long k = 2; k <= vertex_max_k_[v]; ++k) {
            emu::remote_add(&s.vertices_per_truss[k-2], 1);
        }
    });
    return s;
}

ktruss::stats
ktruss::run()
{
    long num_edges = g_->num_edges();
    DEBUG("Initial graph has %li directed edges...\n", num_edges);
    long num_removed = 0;
    count_initial_triangles();
    long k = 3;
    do {
        LOG("Searching for the %li-truss...\n", k);
        do {
            unroll_wedges(k);
            unroll_supported_triangles(k);
            num_removed = remove_edges(k);
            num_edges -= num_removed;
            DEBUG("Removed %li edges, %li edges remaining\n",
                num_removed, num_edges);
        } while (num_removed > 0 && num_edges > 0);
        ++k;
    } while (num_edges > 0);
    // The last iteration removed all edges and incremented k
    // That means there was no k-1 truss found, max k is actually k-2
    k -= 2;

    return compute_truss_sizes(k);
}

// Do serial ktruss computation
bool
ktruss::check()
{
    bool success = true;
    // Reserve memory for holding intersection of two vertices
    std::vector<long> intersection;
    intersection.reserve(g_->num_vertices());
    // For all edges u->v where u > v
    for (long u = 0; u < g_->num_vertices(); ++u) {
        for (auto uv = g_->out_edges_begin(u); uv < g_->out_edges_end(u); ++uv){
            long v = uv->dst;
            if (v > u) { break; }
            // Find all neighbors w that u and v have in common
            std::set_intersection(
                g_->out_edges_begin(u),
                g_->out_edges_end(u),
                g_->out_edges_begin(v),
                g_->out_edges_end(v),
                std::back_inserter(intersection));
            // Get the value of k that the algorithm assigned to this edge
            long expected_k = uv->KTE;
            // Count how many of these triangles involve other edges with
            // the same k
            long actual_num_tris = std::count_if(
                intersection.begin(), intersection.end(), [=](long w) {
                    // Look up the edge in the right direction
                    // The other one doesn't have KTE set
                    auto uw = g_->find_out_edge(std::max(u, w), std::min(u, w));
                    auto vw = g_->find_out_edge(std::max(v, w), std::min(v, w));
                    return uw->KTE >= expected_k && vw->KTE >= expected_k;
                }
            );

            if (actual_num_tris < expected_k - 2) {
                LOG("Edge %li -> %li has k of %li, but only %li tris in the %li-truss\n",
                    u, v, expected_k, actual_num_tris, expected_k);
                success = false;
            }
            // Reset for next edge
            intersection.clear();
        }
    }
    return success;
}

void
ktruss::dump_graph()
{
    // Dump the active edges of the graph
    for (long src = 0; src < g_->num_vertices(); ++src) {
        auto edges_begin = g_->out_edges_begin(src);
        auto edges_end = active_edges_end_[src];
        if (edges_end - edges_begin > 0) {
            LOG("%li ->", src);
            for (auto e = edges_begin; e < edges_end; ++e) {
                if (e->dst < src) {
                    LOG(" %li", e->dst);
                }
            }
            LOG("\n");
        }
    }
}