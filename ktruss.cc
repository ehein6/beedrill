#include "ktruss.h"
#include <algorithm>
#include <emu_cxx_utils/intrinsics.h>
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>

// Uncomment to print a line for each triangle found and each edge removed
//#define VERBOSE_LOGGING

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
, num_removed_(0)
, worklist_(g.num_vertices())
{}

ktruss::ktruss(const ktruss& other, emu::shallow_copy shallow)
: g_(other.g_)
, active_edges_end_(other.active_edges_end_, shallow)
, vertex_max_k_(other.vertex_max_k_, shallow)
, num_removed_(other.num_removed_)
, worklist_(other.worklist_, shallow)
{}

void
ktruss::clear()
{
    g_->for_each_vertex(dyn, [this](long src) {
        // Look up edge list for this vertex
        auto edges_begin = g_->out_edges_begin(src);
        auto edges_end = g_->out_edges_end(src);
        // Sort edge list in ascending order
        std::sort(edges_begin, edges_end, std::less<>());
        // Shrink edge lists to only consider edges that connect to vertices
        // with lower vertex ID's. Essentially making this a directed graph
        active_edges_end_[src] = std::lower_bound(edges_begin, edges_end, src);
        // Init vertex K values to zero
        vertex_max_k_[src] = 0;
        // Init all edge property values to zero
        g_->for_each_out_edge(src, [](ktruss_edge_slot &dst) {
            dst.TC = 0;
        });
    });
}

static inline void
record_triangle(
    ktruss_graph::edge_iterator pq,
    ktruss_graph::edge_iterator qr,
    ktruss_graph::edge_iterator pr)
{
    remote_add(&pq->TC, 1);
    remote_add(&qr->TC, 1);
    remote_add(&pr->TC, 1);
}

void
scan_intersection(
    ktruss_graph::edge_iterator pq,
    ktruss_graph::edge_iterator qr, ktruss_graph::edge_iterator qr_end,
    ktruss_graph::edge_iterator pr)
{
    // Handle one at a time until the number of edges is divisible by 4
    for (;(qr_end - qr) % 4 != 0; ++qr) {
        while (*pr < *qr) { ++pr; }
        if (*qr == *pr) { record_triangle(pq, qr, pr); }
    }

    for (long r1, r2, r3, r4; qr < qr_end;) {
        // Pick up four neighbors of v
        r1 = *qr++;
        r2 = *qr++;
        r3 = *qr++;
        r4 = *qr++;
        // Now we have u->v and v->w
        // Scan through neighbors of u, looking for w
        while (*pr < r1) { ++pr; }
        if (r1 == *pr) { record_triangle(pq, qr-4, pr); }
        while (*pr < r2) { ++pr; }
        if (r2 == *pr) { record_triangle(pq, qr-3, pr); }
        while (*pr < r3) { ++pr; }
        if (r3 == *pr) { record_triangle(pq, qr-2, pr); }
        while (*pr < r4) { ++pr; }
        if (r4 == *pr) { record_triangle(pq, qr-1, pr); }
        RESIZE();
    }
}

void
ktruss::count_triangles()
{
    worklist_.clear_all();
    g_->for_each_vertex(fixed, [this](long p) {
        // Add all active edges of p to the worklist
        auto q_begin = g_->out_edges_begin(p);
        auto q_end = active_edges_end_[p];
        if (q_begin != q_end) {
            worklist_.append(p, q_begin, q_end);
        }
        // Init all triangle counts to zero
        std::for_each(q_begin, q_end, [](ktruss_edge_slot &dst) {
            dst.TC = 0;
        });
    });

    // Process all p->q edges with a thread pool
    worklist_.process_all_edges(dyn, [this](long p, ktruss_edge_slot& pq) {
        long q = pq.dst;
        // Range of q's neighbors that are less than q
        auto qr_begin = g_->out_edges_begin(q);
        auto qr_end = active_edges_end_[q];
        // Iterator over edges of p
        auto pr = g_->out_edges_begin(p);

        scan_intersection(&pq, qr_begin, qr_end, pr);
    });
}

long
ktruss::remove_edges(long k)
{
    num_removed_ = 0;
    g_->for_each_vertex(emu::dyn, [&](long v){
        // Get the edge list for this vertex
        auto begin = g_->out_edges_begin(v);
        auto end = active_edges_end_[v];
        // Move all edges with TC < k-2 to the end of the list
        auto remove_begin = std::stable_partition(begin, end,
            [k](ktruss_edge_slot& e) {
                return e.TC >= k-2;
            }
        );
        if (remove_begin != end) {
            // Set K for the edges we are about to remove
            for_each(remove_begin, end, [=](ktruss_edge_slot &e) {
                DEBUG("Removed %li->%li, KTE=%li\n", v, e.dst, k-1);
                e.KTE = k - 1;
            });
            // Resize the edge list to drop the edges off the end
            active_edges_end_[v] = remove_begin;
            // Increment count of edges that were removed
            emu::remote_add(&num_removed_, end - remove_begin);
        }
    });
    return emu::repl_reduce(num_removed_, std::plus<>());
}

ktruss::stats
ktruss::compute_truss_sizes(long max_k)
{
    stats s(max_k);

    // Rebuild the worklist to include all edges that were removed
    worklist_.clear_all();
    g_->for_each_vertex(dyn, [this](long p) {
        // Sort the edge list
        auto edges_begin = g_->out_edges_begin(p);
        auto edges_end = g_->out_edges_end(p);
        std::sort(edges_begin, edges_end);
        // Use binary search to find neighbors of p that are less than p
        edges_end = std::lower_bound(edges_begin, edges_end, p);
        // Add these edges to the work list
        if (edges_begin != edges_end) {
            worklist_.append(p, edges_begin, edges_end);
        }
    });

    // Process all p->q edges with a thread pool
    worklist_.process_all_edges(dyn, [&](long src, ktruss_edge_slot& dst) {
        assert(dst.KTE >= 2);
        assert(dst.KTE <= max_k);
        // This vertex is a member of all trusses up through k
        emu::remote_max(&vertex_max_k_[src], dst.KTE);
        emu::remote_max(&vertex_max_k_[dst], dst.KTE);
        // This edge is a member of all trusses up through k
        for (long k = 2; k <= dst.KTE; ++k) {
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
    long num_removed = 0;
    count_triangles();
    long k = 3;
    do {
        LOG("Searching for the %li-truss. %li edges remaining...\n",
            k, num_edges);
        do {
            num_removed = remove_edges(k);
            num_edges -= num_removed;
            DEBUG("Removed %li edges, %li edges remaining\n",
                num_removed, num_edges);
            if (num_removed) { count_triangles(); }
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
    // Sort the edge lists
    g_->sort_edge_lists(std::less<>());
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