#include "tc.h"
#include <algorithm>
#include <emu_cxx_utils/intrinsics.h>
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/reducers.h>

using namespace emu;
using namespace emu::parallel;

triangle_count::triangle_count(graph & g)
: g_(&g)
, worklist_(g.num_vertices())
{
    clear();
}

triangle_count::triangle_count(const triangle_count& other, emu::shallow_copy)
: g_(other.g_)
, num_triangles_(other.num_triangles_)
, num_twopaths_(other.num_twopaths_)
, worklist_(other.worklist_, emu::shallow_copy())
{}

void
triangle_count::clear()
{
    num_triangles_ = 0;
    num_twopaths_ = 0;
}

triangle_count::stats
triangle_count::run()
{
    worklist_.clear_all();
    g_->for_each_vertex(fixed, [this](long u) {
        // Use binary search to find neighbors of u that are less than u.
        auto v_begin = g_->out_edges_begin(u);
        auto v_end = std::lower_bound(v_begin, g_->out_edges_end(u), u);
        // Add these edges to the work list
        if (v_begin != v_end) {
            worklist_.append(u, v_begin, v_end);
        }
    });

    // Process all the edges in the work list in parallel
    worklist_.process_all_edges(dyn, [this](long u, long v) {
        // At this point we have one side of the triangle, from u to v
        // For each edge v->w, see if we also have u->w to complete the
        // triangle. Once again, we limit ourselves to the neighbors of v
        // that are less than v using a binary search.
        auto vw_begin = g_->out_edges_begin(v);
        auto vw_end = std::lower_bound(vw_begin, g_->out_edges_end(v), v);
        // Record each u->v->w as a two-path
        remote_add(&num_twopaths_, std::distance(vw_begin, vw_end));
        // Iterator over edges of u
        auto uw = g_->out_edges_begin(u);
        // Reducer for counting up the number of triangles
        reducer_opadd<long> count(&num_triangles_);
        // Count number of neighbors that u and v have in common
        for_each(unroll, vw_begin, vw_end, [uw, count](long w) mutable {
            while (*uw < w) { ++uw; } if (w == *uw) { ++count; }
        });
    });
    // Reduce number of triangles and number of two-paths and return
    stats s;
    repl_for_each(parallel_policy<8>(), *this, [&s](triangle_count& self) {
        remote_add(&s.num_triangles, self.num_triangles_);
        remote_add(&s.num_twopaths, self.num_twopaths_);
    });
    return s;
}

// Do serial triangle count
bool
triangle_count::check()
{
    // Do a serial triangle count
    long correct_num_triangles = 0;

    // For all vertices...
    for (long u = 0; u < g_->num_vertices(); ++u) {
        // For v in u.neighbors, where u > v...
        for (auto p_v = g_->out_edges_begin(u); p_v < g_->out_edges_end(u); ++p_v) {
            long v = *p_v;
            if (v > u) break;
            auto p_u = g_->out_edges_begin(u);
            // For w in v.neighbors, where v > w...
            for (auto p_w = g_->out_edges_begin(v); p_w < g_->out_edges_end(v); ++p_w) {
                long w = *p_w;
                if (w > v) break;
                // Scan through u.neighbors, looking for w
                while (*p_u < w) { ++p_u; }
                if (w == *p_u) {
                    // LOG("Found triangle %li->%li->%li\n", u, v, w);
                    correct_num_triangles += 1;
                }
            }
        }
    }

    // Compare with the parallel results
    bool success = repl_reduce(num_triangles_, std::plus<>()) == correct_num_triangles;
    if (!success) {
        LOG("Should have found %li triangles\n", correct_num_triangles);
    }
    return success;
}
