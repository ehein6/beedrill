#include "tc.h"
#include <algorithm>
#include <emu_cxx_utils/intrinsics.h>
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>

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
}

void
triangle_count::check_edge(long u, long v)
{
    // At this point we have one side of the triangle, from u to v
    // For each edge v->w, see if we also have u->w to complete the triangle
    // Once again, we limit ourselves to the neighbors of v that are less than v
    // using a binary search
    auto vw_begin = g_->out_edges_begin(v);
    auto vw_end = std::lower_bound(vw_begin, g_->out_edges_end(v), v);
    // Record each u->v->w as a two-path
    remote_add(&num_twopaths_, std::distance(vw_begin, vw_end));
    // Iterator over edges of u
    auto p_uw = g_->out_edges_begin(u);
    for_each(seq, vw_begin, vw_end, [this, &p_uw](long w){
        // Now we have u->v and v->w
        // Scan through neighbors of u, looking for w
        while (*p_uw < w) { p_uw++; }
        if (w == *p_uw) {
            // Found the triangle u->v->w
            // LOG("Found triangle %li->%li->%li\n", u, v, w);
            remote_add(&num_triangles_, 1);
        }
    });
}

triangle_count::stats
triangle_count::run()
{
    worklist_.clear_all();
    g_->for_each_vertex(fixed, [this](long u) {
        // Use binary search to find neighbors of u that are less than u.
        auto v_begin = g_->out_edges_begin(u);
        auto v_end = std::lower_bound(v_begin, g_->out_edges_end(u), u);
        if (v_begin != v_end) {
            worklist_.append(u, v_begin, v_end);
        }
    });

    worklist_.process_all(seq,
        [this](long u, long v) { check_edge(u, v); }
    );

    stats s;
    s.num_triangles = cilk_spawn repl_reduce(num_triangles_, std::plus<>());
    s.num_twopaths =             repl_reduce(num_twopaths_, std::plus<>());
    cilk_sync;
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
