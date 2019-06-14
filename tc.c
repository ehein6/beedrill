#include "tc.h"
#include <stdlib.h>
#include <assert.h>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <stdio.h>
#include "cursor.h"

tc_data TC;

void
tc_init()
{
    tc_data_clear();
}

void
tc_data_clear()
{
    TC.num_triangles = 0;
}

void
tc_deinit()
{
}

// Returns an iterator pointing to the first element in the range [first, last)
// that is not less than (i.e. greater or equal to) value, or last if no such element is found.
// Adapted from C++ reference implementation at http://en.cppreference.com/w/cpp/algorithm/lower_bound
long *
lower_bound(long * first, long * last, long value)
{
    long * it;
    ptrdiff_t count, step;
    count = last - first;

    while (count > 0) {
        it = first;
        step = count / 2;
        it += step;
        if (*it < value) {
            first = ++it;
            count -= step + 1;
        } else count = step;
    }
    return first;
}

// Look for triangles with first side u->v, where v1 <= v < v2
void
count_triangles_worker(long u, long * v1, long * v2)
{
    long num_triangles = 0;
    for (long * p_v = v1; p_v < v2; ++p_v) {
        long v = *p_v;
        // At this point we have one side of the triangle, from u to v
        // For each edge v->w, see if we also have u->w to complete the triangle
        long * vw_begin = G.vertex_out_neighbors[v].local_edges;
        long * vw_end = vw_begin + G.vertex_out_degree[v];
        // Once again, we limit ourselves to the neighbors of v that are less than v
        // using a binary search
        vw_end = lower_bound(vw_begin, vw_end, v);
        // Iterator over edges of u
        long * p_uw = G.vertex_out_neighbors[u].local_edges;
        for (long * p_w = vw_begin; p_w < vw_end; ++p_w) {
            // Now we have u->v and v->w
            long w = *p_w;
            // Scan through neighbors of u, looking for w
            while (*p_uw < w) { p_uw++; } // TODO this could use lower_bound
            if (w == *p_uw) {
                // Found the triangle u->v->w
                // LOG("Found triangle %li->%li->%li\n", u, v, w);
                ++num_triangles;
            }
        }
    }
    REMOTE_ADD(&TC.num_triangles, num_triangles);
}

// Count triangles that start at vertex u
void
count_triangles(long u)
{
    long grain = 16;

    // Use binary search to find neighbors of u that are less than u.
    long * v_begin = G.vertex_out_neighbors[u].local_edges;
    long * v_end = v_begin + G.vertex_out_degree[u];
    v_end = lower_bound(v_begin, v_end, u);
    long v_n = v_end - v_begin;
    if (v_n <= grain) {
        count_triangles_worker(u, v_begin, v_end);
    } else {
        for (long * v1 = v_begin; v1 < v_end; v1 += grain) {
            long * v2 = v1 + grain;
            if (v2 > v_end) { v2 = v_end; }
            cilk_spawn count_triangles_worker(u, v1, v2);
        }
    }
}

void
count_triangles_spawner(long * array, long begin, long end, va_list args)
{
    for (long u = begin; u < end; u += NODELETS()) {
        count_triangles(u);
    }
}

long
tc_run()
{
    emu_1d_array_apply(G.vertex_out_degree, G.num_vertices, 1,
        count_triangles_spawner
    );
    return TC.num_triangles;
}


// Do serial triangle count
bool
tc_check()
{
    // Do a serial triangle count
    long correct_num_triangles = 0;

    // For all vertices...
    for (long u = 0; u < G.num_vertices; ++u) {
        // For v in u.neighbors, where u > v...
        cursor cu;
        for (cursor_init_out(&cu, u); cursor_valid(&cu); cursor_next(&cu)) {
            long v = *cu.e;
            if (v > u) break;
            cursor cu2; cursor_init_out(&cu2, u);
            // For w in v.neighbors, where v > w...
            cursor cv;
            for (cursor_init_out(&cv, v); cursor_valid(&cv); cursor_next(&cv)) {
                long w = *cv.e;
                if (w > v) break;
                // Scan through u.neighbors, looking for w
                while (*cu2.e < w) { cursor_next(&cu2); }
                if (w == *cu2.e) {
                    // LOG("Found triangle %li->%li->%li\n", u, v, w);
                    correct_num_triangles += 1;
                }
            }
        }
    }

    // Compare with the parallel results
    bool success = TC.num_triangles == correct_num_triangles;
    if (!success) {
        LOG("Should have found %li triangles\n", correct_num_triangles);
    }
    return success;
}
