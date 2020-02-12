#include <cilk/cilk.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/intrinsics.h>
#include <emu_cxx_utils/fill.h>

#include "graph.h"
#include "dist_edge_list.h"

using namespace emu;

bool
out_edge_exists(graph& g, long src, long dst)
{
    return g.out_edges_end(src) != std::find_if(
        g.out_edges_begin(src), g.out_edges_end(src), [&](long e) {
            assert(e >= 0);
            assert(e < g.num_vertices());
            return e == dst;
        }
    );
}

// Compare the edge list with the constructed graph
// VERY SLOW, use only for testing
bool
graph::check(dist_edge_list &dist_el) {
    long ok = 1;
    dist_el.forall_edges([&] (long src, long dst) {
        if (!out_edge_exists(*this, src, dst)) {
            LOG("Missing out edge for %li->%li\n", src, dst);
            ok = 0;
        }
        if (!out_edge_exists(*this, dst, src)) {
            LOG("Missing out edge for %li->%li\n", src, dst);
            ok = 0;
        }
    });
    return (bool)ok;
}

std::unique_ptr<emu::repl_shallow<graph>>
graph::from_edge_list(dist_edge_list & dist_el)
{
    LOG("Initializing distributed vertex list...\n");
    auto g = emu::make_repl_shallow<graph>(
        dist_el.num_vertices_, dist_el.num_edges_);
    // Assign vertex ID's as position in the list
    parallel::for_each(fixed,
        g->vertex_id_.begin(), g->vertex_id_.end(),
        [id_begin=g->vertex_id_.begin()](long &id) {
            // Compute index in table from the pointer
            id = &id - id_begin;
        }
    );
    // Init all vertex degrees to zero
    parallel::fill(fixed,
        g->vertex_out_degree_.begin(), g->vertex_out_degree_.end(), 0L);

    // Compute degree of each vertex
    LOG("Computing degree of each vertex...\n");
    hooks_region_begin("calculate_degrees");
    // Initialize the degree of each vertex to zero
    // Scan the edge list and do remote atomic adds into vertex_out_degree
    dist_el.forall_edges([&] (long src, long dst) {
        assert(src >= 0 && src < g->num_vertices());
        assert(dst >= 0 && dst < g->num_vertices());
        emu::remote_add(&g->vertex_out_degree_[src], 1);
        emu::remote_add(&g->vertex_out_degree_[dst], 1);
    });
    hooks_region_end();

    // Count how many edges will need to be stored on each nodelet
    // This is in preparation for the next step, so we can do one big allocation
    // instead of a bunch of tiny ones.
    LOG("Counting local edges...\n");
    hooks_region_begin("count_local_edges");
    g->num_local_edges_ = 0;
    g->for_each_vertex([&](long v) {
        emu::atomic_addms(&g->num_local_edges_, g->vertex_out_degree_[v]);
    });
    hooks_region_end();

    LOG("Allocating edge storage...\n");
    // Run around and compute the largest number of edges on any nodelet
    long max_edges_per_nodelet = emu::repl_reduce(g->num_local_edges_,
        [](long lhs, long rhs) { return std::max(lhs, rhs); });
    // Double-check that we haven't lost any edges
    assert(2 * g->num_edges_ ==
        emu::repl_reduce(g->num_local_edges_, std::plus<>()));

    LOG("Will use %li MiB on each nodelet\n", (max_edges_per_nodelet * sizeof(long)) >> 20);

    // Allocate a big stripe, such that there is enough room for the nodelet
    // with the most local edges
    // There will be wasted space on the other nodelets
    g->edge_storage_ = new emu::repl_array<long>(max_edges_per_nodelet);

    // Initialize each copy of next_edge_storage to point to the local array
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        g->get_nth(nlet).next_edge_storage_ = g->edge_storage_->get_nth(nlet);
    }

    // Assign each edge block a position within the big array
    LOG("Carving edge storage...\n");
    hooks_region_begin("carve_edge_storage");
    g->for_each_vertex([&](long v) {
        // Empty vertices don't need storage
        if (g->vertex_out_degree_[v] > 0) {
            // Local vertices have one edge block on the local nodelet
            g->vertex_out_neighbors_[v] = emu::atomic_addms(
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
        percent_edges[nlet] =
            (double)num_local_edges_.get_nth(nlet) / (num_edges_ * 2);
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
    if (NODELETS() >= 100) {
        // Bottom rows: nodelet number, stacked vertically
        printf("       "); // Spacer
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            if (nlet >= 100) { printf("%li", nlet / 100); }
            else             { printf(" "); }
        }
        printf("\n");
    }
    if (NODELETS() >= 10) {
        printf("       "); // Spacer
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            if (nlet >= 10) { printf("%li", (nlet / 10) % 10); }
            else           { printf(" "); }
        }
        printf("\n");
    }
    printf("       "); // Spacer
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        printf("%li", nlet % 10);
    }
    printf("\n");
    fflush(stdout);
}

