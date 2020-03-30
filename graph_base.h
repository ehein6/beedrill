#pragma once

#include <string>
#include <vector>

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/pointer_manipulation.h>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/find.h>
#include <emu_cxx_utils/fill.h>
#include <emu_cxx_utils/repl_array.h>

#include "common.h"
#include "dist_edge_list.h"
#include "worklist.h"

// Global data structures
template<class Edge>
class graph_base {
protected:
    // Total number of vertices in the graph (max vertex ID + 1)
    emu::repl<long> num_vertices_;
    // Total number of edges in the graph
    emu::repl<long> num_edges_;

    // Distributed vertex array
    // ID of each vertex
    emu::striped_array<long> vertex_id_;
    // number of neighbors for this vertex (on all nodelets)
    emu::striped_array<long> vertex_out_degree_;
    // Pointer to local edge array (light vertices only)
    // OR replicated edge block pointer (heavy vertices only)
    emu::striped_array<Edge *> vertex_out_neighbors_;

    // Pointer to chunk of memory on each nodelet for storing edges
    emu::repl<emu::repl_array<Edge> *> edge_storage_;
    // Total number of edges stored on each nodelet
    emu::repl<long> num_local_edges_;
    // Pointer to un-reserved edge storage in local stripe
    Edge *next_edge_storage_;
public:
    // Constructor
    graph_base(long num_vertices, long num_edges)
        : num_vertices_(num_vertices)
        , num_edges_(num_edges)
        , vertex_id_(num_vertices)
        , vertex_out_degree_(num_vertices)
        , vertex_out_neighbors_(num_vertices)
    {}

    // Shallow copy constructor
    graph_base(const graph_base &other, emu::shallow_copy shallow)
        : num_vertices_(other.num_vertices_)
        , num_edges_(other.num_edges_)
        // Make shallow copies for striped arrays
        , vertex_id_(other.vertex_id_, shallow)
        , vertex_out_degree_(other.vertex_out_degree_, shallow)
        , vertex_out_neighbors_(other.vertex_out_neighbors_, shallow)
    {}

    graph_base(const graph_base &other) = delete;

    ~graph_base()
    {
        // Note: Could avoid new/delete here by using a unique_ptr, but we
        // can't safely replicate those yet.
        delete edge_storage_;
    }

    using edge_type = Edge;
    // TODO: more complex edge list data structures will require something
    // fancier than a pointer
    using edge_iterator = Edge*;
    using const_edge_iterator = const Edge*;

    /**
     * This is NOT a general purpose edge insert function, it relies on assumptions
     * - The edge block for this vertex (local or remote) has enough space for the edge
     * - The out-degree for this vertex is counting up from zero, representing the number of edges stored
     */
    void
    insert_edge(long src, long dst)
    {
        // Get the local edge array
        Edge *edges = vertex_out_neighbors_[src];
        long *num_edges_ptr = &vertex_out_degree_[src];
        // Atomically claim a position in the edge list and insert the edge
        // NOTE: Relies on all edge counters being set to zero in the previous step
        edges[emu::atomic_addms(num_edges_ptr, 1)].dst = dst;
    }

    void
    remove_edge(long src, edge_iterator e)
    {
        // FIXME this is not atomic! Do we need a lock?
        // FIXME need to keep the list sorted?
        // Get a pointer to the last edge in the list
        edge_iterator last_edge = out_edges_end(src) - 1;
        // Swap the edge to be removed with the last edge
        std::swap(*e, *last_edge);
        // Decrement the length of this edge list
        emu::remote_add(&vertex_out_degree_[src], -1);
    }

public:
    // Compare the edge list with the constructed graph
// VERY SLOW, use only for testing
    bool
    check(dist_edge_list &dist_el) {
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

        // Check for duplicates (assumes the edge lists are sorted)
        for_each_vertex(emu::dyn, [&](long v) {
            auto pos = std::adjacent_find(out_edges_begin(v), out_edges_end(v));
            if (pos != out_edges_end(v)) {
                LOG("Edge %li->%li is duplicated\n", v, pos->dst);
                ok = false;
            }
        });

        return (bool)ok;
    }

    void
    dump()
    {
        for (long src = 0; src < num_vertices_; ++src) {
            if (vertex_out_degree_[src] > 0) {
                LOG("%li ->", src);
                auto edges_begin = vertex_out_neighbors_[src];
                auto edges_end = edges_begin + vertex_out_degree_[src];
                for (auto e = edges_begin; e < edges_end; ++e) { LOG(" %li", e->dst); }
                LOG("\n");
            }
        }
    }

    void
    print_distribution()
    {
        // Format for 80-column display width
        // We need one column per nlet/node/chassis, plus 8 for the y-ticks
        const long char_limit = 80 - 8;
        std::string col_label;
        long num_cols, nlets_per_col;
        long num_nlets = NODELETS();
        if (num_nlets == 1) {
            // Everything on one nodelet, not an interesting plot
            return;
        } else if (char_limit > num_nlets) {
            col_label = "nodelet";
            nlets_per_col = 1;
        } else if (char_limit > num_nlets / 8){
            col_label = "node";
            nlets_per_col = 8;
        } else if (char_limit > num_nlets / 64) {
            col_label = "chassis";
            nlets_per_col = 64;
        } else {
            col_label = "rack";
            nlets_per_col = 512;
        }
        num_cols = num_nlets / nlets_per_col;

        // Compute percentage of edges on each nodelet
        std::vector<double> percent_edges(num_cols, 0);
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            long col = nlet / nlets_per_col;
            percent_edges[col] +=
                (double)num_local_edges_.get_nth(nlet) / (num_edges_ * 2);
        }

        // Compute the max (to scale the y-axis)
        double max_percent = 0;
        for (long col = 0; col < num_cols; ++col) {
            if (percent_edges[col] > max_percent) {
                max_percent = percent_edges[col];
            }
        }

        // Compute bar heights
        std::vector<long> histogram(num_cols, 0);
        const long bar_height = 10;
        for (long col = 0; col < num_cols; ++col) {
            histogram[col] = bar_height * (percent_edges[col] / max_percent);
        }

        // Draw a histogram representing edge distribution
        printf("Edge distribution per %s: \n", col_label.c_str());
        for (long row = bar_height; row > 0; --row) {
            // Y-axis label
            printf("%5.1f%% ", 100 * max_percent * row / bar_height);
            // Draw bar segments in this row
            for (long col = 0; col < num_cols; ++col) {
                if (histogram[col] >= row) {
                    printf("█");
                } else {
                    printf(" ");
                }
            }
            printf("\n");
        }
        if (num_cols >= 10) {
            printf("       "); // Spacer
            for (long col = 0; col < num_cols; ++col) {
                if (col >= 10) { printf("%li", (col / 10) % 10); }
                else           { printf(" "); }
            }
            printf("\n");
        }
        printf("       "); // Spacer
        for (long col = 0; col < num_cols; ++col) {
            printf("%li", col % 10);
        }
        printf("\n");
        fflush(stdout);
    }

    long num_vertices() const {
        return num_vertices_;
    }

    long num_edges() const {
        return num_edges_;
    }

    long out_degree(long vertex_id) const {
        return vertex_out_degree_[vertex_id];
    }

    Edge *out_neighbors(long vertex_id) {
        return vertex_out_neighbors_[vertex_id];
    }

    long * vertices_begin() { return vertex_id_.begin(); }
    long * vertices_end() { return vertex_id_.end(); }

    edge_iterator
    out_edges_begin(long src)
    {
        return vertex_out_neighbors_[src];
    }

    edge_iterator
    out_edges_end(long src)
    {
        return out_edges_begin(src) + out_degree(src);
    }

    // Convenience functions for mapping over edges/vertices

    // Map a function to all vertices in parallel, using the specified policy
    template<class Policy, class Function>
    void for_each_vertex(Policy policy, Function worker)
    {
        emu::parallel::for_each(
            policy, vertex_id_.begin(), vertex_id_.end(), worker
        );
    }

    template<class Function>
    void for_each_vertex(Function worker) {
        for_each_vertex(emu::default_policy, worker);
    }

    template<class Policy, class Function>
    void for_each_out_edge(Policy policy, long src, Function worker)
    {
        // Spawn threads over the range according to the specified policy
        emu::parallel::for_each(
            policy, out_edges_begin(src), out_edges_end(src), worker
        );
    }

    template<class Function>
    void for_each_out_edge(long src, Function worker)
    {
        for_each_out_edge(emu::default_policy, src, worker);
    }

    template<class Policy, class Function>
    edge_iterator find_out_edge_if(Policy policy, long src, Function worker)
    {
        return emu::parallel::find_if(
            policy, out_edges_begin(src), out_edges_end(src), worker
        );
    }

    edge_iterator
    find_out_edge(long src, long dst)
    {
        return std::find(out_edges_begin(src), out_edges_end(src), dst);
    }

    template<class Compare>
    void
    sort_edge_lists(Compare comp)
    {
        hooks_region_begin("sort_edge_lists");
        for_each_vertex(emu::dyn, [&](long v){
            std::sort(out_edges_begin(v), out_edges_end(v), comp);
        });
        hooks_region_end();
    }

    bool
    out_edge_exists(long src, long dst)
    {
        return out_edges_end(src) != std::find_if(
            out_edges_begin(src), out_edges_end(src), [&](long e) {
                assert(e >= 0);
                assert(e < num_vertices());
                return e == dst;
            }
        );
    }

    template<class Graph>
    friend std::unique_ptr<emu::repl_shallow<Graph>>
    create_graph_from_edge_list(dist_edge_list & dist_el);
};

template<class Graph>
std::unique_ptr<emu::repl_shallow<Graph>>
create_graph_from_edge_list(dist_edge_list & dist_el)
{
    using namespace emu;
    LOG("Initializing distributed vertex list...\n");
    auto the_graph = emu::make_repl_shallow<Graph>(
        dist_el.num_vertices(), dist_el.num_edges());
    emu::repl_shallow<Graph> *g = &*the_graph;
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
    dist_el.forall_edges([g] (long src, long dst) {
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
    g->for_each_vertex([g](long v) {
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
    g->edge_storage_ = new emu::repl_array<typename Graph::edge_type>(
        max_edges_per_nodelet);

    // Initialize each copy of next_edge_storage to point to the local array
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        g->get_nth(nlet).next_edge_storage_ = g->edge_storage_->get_nth(nlet);
    }

    // Assign each edge block a position within the big array
    LOG("Carving edge storage...\n");
    hooks_region_begin("carve_edge_storage");
    g->for_each_vertex([g](long v) {
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
    dist_el.forall_edges([g] (long src, long dst) {
        // Insert both ways for undirected graph
        g->insert_edge(src, dst);
        g->insert_edge(dst, src);
    });
    hooks_region_end();

    // LOG("Checking graph...\n");
    // check_graph();
    // dump_graph();

    LOG("...Done\n");
    return the_graph;
}
