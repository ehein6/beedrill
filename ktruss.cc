#include "ktruss.h"
#include <algorithm>
#include <emu_cxx_utils/intrinsics.h>
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>

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
                emu::remote_add(&qr->TC, 1);
                emu::remote_add(&pq.TC, 1);
                emu::remote_add(&pr->TC, 1);
                emu::remote_add(&qr->qrC, 1);
                emu::remote_add(&g_->find_out_edge(q, p)->pRefC, 1);
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
        // If the edge p->q will be removed, unroll all affected triangles
        // Basically triangle count in reverse
        assert(pq.TC >= 0);
        if (pq.TC < k - 2) {
            // Iterator over edges of p
            auto pr = g_->out_edges_begin(p);
            // Range of q's neighbors that are less than q
            auto qr_begin = g_->out_edges_begin(q);
            auto qr_end = active_edges_end_[q];
            for (auto qr = qr_begin; qr != qr_end; ++qr){
                while (*pr < *qr) { pr++; }
                if (*qr == *pr) {
                    // Unroll triangle
                    emu::remote_add(&qr->TC, -1);
                    emu::remote_add(&pq.TC, -1);
                    emu::remote_add(&pr->TC, -1);
                    emu::remote_add(&qr->qrC, -1);
                    emu::remote_add(&g_->find_out_edge(q, p)->pRefC, -1);
                }
            }

        // p->q will not be removed, but what about p->r?
        // Look at all wedges p->(q, r)
        // If any p->r will be deleted, check for q->r and unroll
        } else {
            // Iterator over neighbors of q
            auto qr = g_->out_edges_begin(q);
            // For each vertex r (neighbors of p that are less than q)
            auto pr_begin = g_->out_edges_begin(p);
            auto pr_end = std::lower_bound(pr_begin, active_edges_end_[p], q);
            for (auto pr = pr_begin; pr != pr_end; ++pr) {
                // Will the edge be removed?
                if (pr->TC < k - 2) {
                    // Look for edge q->r to complete the triangle
                    while (*qr < *pr) { ++qr; }
                    if (*qr == *pr) {
                        // Unroll triangle
                        emu::remote_add(&qr->TC, -1);
                        emu::remote_add(&pq.TC, -1);
                        emu::remote_add(&pr->TC, -1);
                        emu::remote_add(&qr->qrC, -1);
                        emu::remote_add(&g_->find_out_edge(q, p)->pRefC, -1);
                    }
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
        assert(qr.TC >= 0);
        assert(qr.qrC >= 0);
        if (qr.TC < k - 2 && qr.qrC > 0) {
            // Range of q's neighbors that are GREATER than q
            // i.e. we are looking at edges in the other direction
            auto qp_end = g_->out_edges_end(q);
            auto qp_begin = std::upper_bound(g_->out_edges_begin(q), qp_end, q);

            for (auto qp = qp_begin; qp != qp_end; ++qp){
                if (qp->pRefC > 0) {
                    // Unroll supported triangle
                    long r = qr.dst;
                    long p = qp->dst;
                    emu::remote_add(&qr.TC, -1);
                    emu::remote_add(&g_->find_out_edge(p, q)->TC, -1);
                    emu::remote_add(&g_->find_out_edge(p, r)->TC, -1);
                    emu::remote_add(&qr.qrC, -1);
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
            for_each(remove_begin, end, [k](ktruss_edge_slot &e) {
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
    LOG("Initial graph has %li directed edges...\n", num_edges);
    g_->dump();
    long num_removed = 0;
    count_initial_triangles();
    long k = 3;
    do {
        do {
            unroll_wedges(k);
            unroll_supported_triangles(k);
            num_removed = remove_edges(k);
            num_edges -= num_removed;
            LOG("Removed %li edges, %li edges remaining\n",
                num_removed, num_edges);
        } while (num_removed > 0);
        LOG("Found the %li-truss\n", k);
        ++k;
        dump_graph();
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
    // FIXME
    return false;
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