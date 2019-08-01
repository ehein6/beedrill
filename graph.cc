#include <cilk/cilk.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/fill.h>

#include "graph.h"
#include "dist_edge_list.h"

static inline long *
grab_edges(long * volatile * ptr, long num_edges)
{
    // Atomic add only works on long integers, we need to use it on a long*
    return (long*)ATOMIC_ADDMS((volatile long *)ptr, num_edges * sizeof(long));
}

// Compare the edge list with the constructed graph
// VERY SLOW, use only for testing
bool
graph::check(dist_edge_list &dist_el) {
    long ok = 1;
    dist_el.forall_edges([&] (long src, long dst) {
        if (!out_edge_exists(src, dst)) {
            LOG("Missing out edge for %li->%li\n", src, dst);
            ok = 0;
        }
        if (!out_edge_exists(dst, src)) {
            LOG("Missing out edge for %li->%li\n", src, dst);
            ok = 0;
        }
    });
    return (bool)ok;
}


std::unique_ptr<emu::repl_copy<graph>>
graph::from_edge_list(dist_edge_list & dist_el)
{
    LOG("Initializing distributed vertex list...\n");
    auto g = emu::make_repl_copy<graph>(dist_el.num_vertices_, dist_el.num_edges_);

        // Compute degree of each vertex
    LOG("Computing degree of each vertex...\n");
    hooks_region_begin("calculate_degrees");
    // Initialize the degree of each vertex to zero
    emu::parallel::fill(
        g->vertex_out_degree_.begin(), g->vertex_out_degree_.end(), 0L);
    // Scan the edge list and do remote atomic adds into vertex_out_degree
    dist_el.forall_edges([=, &g] (long src, long dst) {
        assert(src >= 0 && src < g->num_vertices());
        assert(dst >= 0 && dst < g->num_vertices());
        REMOTE_ADD(&g->vertex_out_degree_[src], 1);
        REMOTE_ADD(&g->vertex_out_degree_[dst], 1);
    });
    hooks_region_end();

    // Count how many edges will need to be stored on each nodelet
    // This is in preparation for the next step, so we can do one big allocation
    // instead of a bunch of tiny ones.
    LOG("Counting local edges...\n");
    hooks_region_begin("count_local_edges");
    mw_replicated_init(&g->num_local_edges_, 0);
    g->forall_vertices([=, &g](long v) {
        ATOMIC_ADDMS(&g->num_local_edges_, g->vertex_out_degree_[v]);
    });
    hooks_region_end();

    LOG("Allocating edge storage...\n");
    // Run around and compute the largest number of edges on any nodelet
    long max_edges_per_nodelet = 0;
    long check_total_edges = 0;
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        long num_edges_on_nodelet = g->get_nth(nlet).num_local_edges_;
        REMOTE_MAX(&max_edges_per_nodelet, num_edges_on_nodelet);
        REMOTE_ADD(&check_total_edges, num_edges_on_nodelet);
    }
    // Double-check that we haven't lost any edges
    assert(check_total_edges == 2 * g->num_edges_);


    LOG("Will use %li MiB on each nodelet\n", (max_edges_per_nodelet * sizeof(long)) >> 20);

    // Allocate a big stripe, such that there is enough room for the nodelet
    // with the most local edges
    // There will be wasted space on the other nodelets
    long ** edge_storage = (long**)mw_malloc2d(NODELETS(), sizeof(long) * max_edges_per_nodelet);
    assert(edge_storage);
    // Initialize each copy of G.edge_storage to point to the local chunk
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        *(long**)mw_get_nth(&g->edge_storage_, nlet) = edge_storage[nlet];
        *(long**)mw_get_nth(&g->next_edge_storage_, nlet) = edge_storage[nlet];
    }

    // Assign each edge block a position within the big array
    LOG("Carving edge storage...\n");
    hooks_region_begin("carve_edge_storage");
    g->forall_vertices([=, &g](long v) {
        // Empty vertices don't need storage
        if (g->vertex_out_degree_[v] > 0) {
            // Local vertices have one edge block on the local nodelet
            g->vertex_out_neighbors_[v] = grab_edges(
                &g->next_edge_storage_,
                g->vertex_out_degree_[v]
            );
            // HACK Prepare to fill
            g->vertex_out_degree_[v] = 0;
        }
    });
    hooks_region_end();
    // Populate the edge blocks with edges
    // Scan the edge list one more time
    // For each edge, find the right edge block, then
    // atomically increment eb->nedgesblk to find out where it goes
    LOG("Filling edge blocks...\n");
    hooks_region_begin("fill_edge_blocks");
    dist_el.forall_edges([&] (long src, long dst) {
        // Insert both ways for undirected graph
        g->insert_edge(src, dst);
        g->insert_edge(dst, src);
    });
    hooks_region_end();

    // LOG("Checking graph...\n");
    // check_graph();
    // dump_graph();

    LOG("...Done\n");
    return g;
}


void
graph::dump()
{
    for (long src = 0; src < num_vertices_; ++src) {
        if (vertex_out_degree_[src] > 0) {
            LOG("%li ->", src);
            long * edges_begin = vertex_out_neighbors_[src];
            long * edges_end = edges_begin + vertex_out_degree_[src];
            for (long * e = edges_begin; e < edges_end; ++e) { LOG(" %li", *e); }
            LOG("\n");
        }
    }
}

void
graph::print_distribution()
{
    // Compute percentage of edges on each nodelet
    double percent_edges[NODELETS()];
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        long num_local_edges = *(long*)mw_get_nth(&num_local_edges_, nlet);
        percent_edges[nlet] = (double)num_local_edges / (num_edges_ * 2);
    }

    // Compute the max (to scale the y-axis)
    double max_percent = 0;
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        if (percent_edges[nlet] > max_percent) {
            max_percent = percent_edges[nlet];
        }
    }

    // Compute bar heights
    long histogram[NODELETS()];
    const long bar_height = 10;
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        histogram[nlet] = bar_height * (percent_edges[nlet] / max_percent);
    }

    // Draw a histogram representing edge distribution
    printf("Edge distribution per nodelet: \n");
    for (long row = bar_height; row > 0; --row) {
        // Y-axis label
        printf("%5.1f%% ", 100 * max_percent * row / bar_height);
        // Draw bar segments in this row
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            if (histogram[nlet] >= row) {
                printf("█");
            } else {
                printf(" ");
            }
        }
        printf("\n");
    }
    // Bottom two rows: nodelet number, stacked vertically
    printf("       "); // Spacer
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        if (nlet > 10) { printf("%li", nlet / 10); }
        else           { printf(" "); }
    }
    printf("\n");
    printf("       "); // Spacer
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        printf("%li", nlet % 10);
    }
    printf("\n");
    fflush(stdout);
}

