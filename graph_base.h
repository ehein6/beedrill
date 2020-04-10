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
#include <emu_cxx_utils/reducers.h>

#include "common.h"
#include "dist_edge_list.h"
#include "worklist.h"

template<class Edge>
struct edge_block {
    Edge * edges_;
    long num_edges_;
    Edge * begin() { return edges_; }
    Edge * end() { return edges_ + num_edges_; }
};

template<class T>
class repl_storage {
private:
    // Pointer to un-reserved edge storage in local stripe
    T *next_;
    // Pointer to chunk of memory on each nodelet for storing edge block ptrs
    emu::repl_array<T> storage_;
public:

    // Constructor
    explicit repl_storage(long size)
    : storage_(size)
    , next_(storage_.get_localto(this))
    {}

    // Shallow copy constructor
    repl_storage(const repl_storage& other, emu::shallow_copy shallow)
    : storage_(other.storage_, shallow)
    , next_(storage_.get_localto(this))
    {}

    // Allocate n objects from the storage
    T* allocate(long n) {
        return emu::atomic_addms(&next_, n);
    }
};

// New design: multiple edge blocks per vertex
// Define a edge_grain - min number of edges to put on a single node
// New striped array: edge_blocks
// vertex_edge_blocks


// New repl_array for Edge


// Global data structures
template<class Edge>
class graph_base {
    using EdgeBlock = edge_block<Edge>;
protected:
    // Total number of vertices in the graph (max vertex ID + 1)
    emu::repl<long> num_vertices_;
    // Total number of edges in the graph
    emu::repl<long> num_edges_;

    // Distributed vertex array
    // ID of each vertex
    emu::striped_array<long> vertex_id_;
    // Number of neighbors for this vertex (on all nodelets)
    emu::striped_array<long> vertex_out_degree_;
    // Pointer to first edge block for each vertex
    emu::striped_array<EdgeBlock *> vertex_edge_blocks_;
    // Size of each edge block
    emu::striped_array<long> vertex_num_edge_blocks_;

    // Pointer to chunk of memory on each nodelet for
    // storing edge blocks
    emu::repl<repl_storage<EdgeBlock> *> edge_block_storage_;

    // Pointer to chunk of memory on each nodelet for
    // storing edge blocks
    emu::repl<repl_storage<Edge> *> edge_storage_;

public:
    // Constructor
    graph_base(long num_vertices, long num_edges)
        : num_vertices_(num_vertices), num_edges_(num_edges), vertex_id_(num_vertices),
          vertex_out_degree_(num_vertices), vertex_out_neighbors_(num_vertices) {}

    // Shallow copy constructor
    graph_base(const graph_base &other, emu::shallow_copy shallow)
        : num_vertices_(other.num_vertices_), num_edges_(other.num_edges_)
        // Make shallow copies for striped arrays
        , vertex_id_(other.vertex_id_, shallow), vertex_out_degree_(other.vertex_out_degree_, shallow),
          vertex_out_neighbors_(other.vertex_out_neighbors_, shallow) {}

    graph_base(const graph_base &other) = delete;

    ~graph_base() {
        // Note: Could avoid new/delete here by using a unique_ptr, but we
        // can't safely replicate those yet.
        delete edge_block_storage_;
        delete edge_storage_;
    }

    using edge_type = Edge;
    // TODO: more complex edge list data structures will require something
    // fancier than a pointer
    using edge_iterator = Edge *;
    using const_edge_iterator = const Edge *;

    // Returns which nodelet an edge should be stored on
    EdgeBlock *
    lookup_edge_block(long src, long dst) {
        auto num_ebs = vertex_num_edge_blocks_[src];
        // Fast mod for when rhs is a power of 2
        auto mod = [](unsigned long lhs, unsigned long rhs) {
            return lhs & (rhs - 1);
        };
        // The first edge block is always on the same nodelet as the vertex
        // We use a hash on the low bits of the destination vertex ID
        // to compute an offset away from the "home" nodelet.
        return vertex_edge_blocks_[mod(dst, num_ebs)];
    }

    // Get the "home nodelet" for a vertex
    static long vertex_to_nlet(long v) { return v & (NODELETS() - 1); }

    /**
     * This is NOT a general purpose edge insert function, it relies on assumptions
     * - The edge block for this vertex (local or remote) has enough space for the edge
     * - The out-degree for this vertex is counting up from zero, representing the number of edges stored
     */
    void
    insert_edge(long src, long dst) {
        // Find the edge block where this edge belongs
        auto eb = lookup_edge_block(src, dst);
        // Atomically claim a position in the edge list and insert the edge
        // NOTE: Relies on all edge counters being set to zero in the previous step
        eb->edges_[emu::atomic_addms(&eb->num_edges_, 1)].dst = dst;
    }


public:
    // Compare the edge list with the constructed graph
// VERY SLOW, use only for testing
    bool
    check(dist_edge_list &dist_el) {
        long ok = 1;
        dist_el.forall_edges([&](long src, long dst) {
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

        return (bool) ok;
    }

    void
    dump() {
        for (long src = 0; src < num_vertices_; ++src) {
            if (vertex_out_degree_[src] > 0) {
                LOG("%li ->", src);
                for_each_out_edge(emu::seq, src, [&](long dst) {
                    LOG(" %li", dst);
                });
                LOG("\n");
            }
        }
    }

    void
    print_distribution() {
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
        } else if (char_limit > num_nlets / 8) {
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
                (double) num_local_edges_.get_nth(nlet) / (num_edges_ * 2);
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
                else { printf(" "); }
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

    long *vertices_begin() { return vertex_id_.begin(); }
    long *vertices_end() { return vertex_id_.end(); }

    // Convenience functions for mapping over edges/vertices


    // Map a function to all vertices in parallel, using the specified policy
    template<class Policy, class Function>
    void for_each_edge_block(Policy policy, long src, Function worker) {
        auto eb_begin = vertex_edge_blocks_[src];
        auto eb_end = eb_begin + vertex_num_edge_blocks_[src];

        // Spawn a for_each at each edge block
        // TODO choose policy for this loop
        emu::parallel::for_each(emu::seq, eb_begin, eb_end,
            [policy, worker](EdgeBlock &eb) {
                cilk_migrate_hint(eb.edges);
                cilk_spawn emu::parallel::for_each(policy, eb.begin(), eb.end(), worker
                );
            }
        );
    }
    // Map a function to all vertices in parallel, using the specified policy
    template<class Policy, class Function>
    void for_each_vertex(Policy policy, Function worker) {
        emu::parallel::for_each(
            policy, vertex_id_.begin(), vertex_id_.end(), worker
        );
    }

    template<class Function>
    void for_each_vertex(Function worker) {
        for_each_vertex(emu::default_policy, worker);
    }

    template<class Policy, class Function>
    void for_each_out_edge(Policy policy, long src, Function worker) {
        auto eb_begin = vertex_edge_blocks_[src];
        auto eb_end = eb_begin + vertex_num_edge_blocks_[src];

        // Spawn a for_each at each edge block
        // TODO choose policy for this loop
        emu::parallel::for_each(emu::seq, eb_begin, eb_end,
            [policy, worker](EdgeBlock &eb) {
                cilk_migrate_hint(eb.edges);
                cilk_spawn emu::parallel::for_each(
                    policy, eb.begin(), eb.end(), worker
                );
            }
        );
    }

    template<class Function>
    void for_each_out_edge(long src, Function worker) {
        for_each_out_edge(emu::default_policy, src, worker);
    }

    template<class Policy, class Function>
    edge_iterator find_out_edge_if(Policy policy, long src, Function worker) {
        return emu::parallel::find_if(
            policy, out_edges_begin(src), out_edges_end(src), worker
        );
    }

    edge_iterator
    find_out_edge(long src, long dst) {
        auto eb = lookup_edge_block(src, dst);
        return std::find(eb->begin(), eb->end(), dst);
    }

    template<class Compare>
    void
    sort_edge_lists(Compare comp) {
        hooks_region_begin("sort_edge_lists");
        for_each_vertex(emu::dyn, [&](long v) {
            std::sort(out_edges_begin(v), out_edges_end(v), comp);
        });
        hooks_region_end();
    }

    bool
    out_edge_exists(long src, long dst) {
        return out_edges_end(src) != std::find_if(
            out_edges_begin(src), out_edges_end(src), [&](long e) {
                assert(e >= 0);
                assert(e < num_vertices());
                return e == dst;
            }
        );
    }

    using handle = std::unique_ptr<emu::repl_shallow<graph_base>>;

    static handle
    create_graph_from_edge_list(dist_edge_list &dist_el)
    {
        using namespace emu;
        LOG("Initializing distributed vertex list...\n");
        auto the_graph = emu::make_repl_shallow<graph_base>(
            dist_el.num_vertices_, dist_el.num_edges_);
        emu::repl_shallow<graph_base> *g = &*the_graph;

        // Assign vertex ID's as position in the list
        parallel::for_each(fixed,
            g->vertex_id_.begin(), g->vertex_id_.end(),
            [id_begin = g->vertex_id_.begin()](long &id) {
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
        dist_el.forall_edges([g](long src, long dst) {
            assert(src >= 0 && src < g->num_vertices());
            assert(dst >= 0 && dst < g->num_vertices());
            emu::remote_add(&g->vertex_out_degree_[src], 1);
            emu::remote_add(&g->vertex_out_degree_[dst], 1);
        });
        hooks_region_end();

        // Decide how many edge blocks each vertex needs to have
        // This will affect the graph distribution
        g->for_each_vertex([g](long v) {
            // Minimum number of edges to put in an edge block
            // TODO make this a configuration parameter
            const long edge_block_grain = 1024;
            // Divide and round up to nearest integer
            // FIXME need to round up to nearest power of two
            // FIXME should never have more edge blocks than nodelets
            auto div_round_up = [](long num, long den) {
                return (num + den - 1) / den;
            };
            // Store number of edge blocks for this vertex
            g->vertex_num_edge_blocks_[v] = div_round_up(
                g->out_degree(v), edge_block_grain
            );
        });

        // Count edge blocks on each nodelet
        auto edge_blocks_per_nodelet = emu::make_repl<long>(0);
        g->for_each_vertex([g, ebs_per_nlet=*edge_blocks_per_nodelet](long v) {
            auto num_ebs = g->vertex_edge_blocks_[v];
            auto home_nlet = g->vertex_to_nlet(v);
            // Increment counter on each nodelet where this vertex will have a
            // block
            for (auto i = 0; i < num_ebs; ++i) {
                emu::remote_add(&ebs_per_nlet.get_nth(home_nlet + i), 1);
            }

        });
        // Function object wrapper for max
        auto max2 = [](long lhs, long rhs) { return std::max(lhs, rhs); };
        // Count the total number of edge blocks, and the maximum on a single
        // nodelet
        long max_ebs = emu::repl_reduce(*edge_blocks_per_nodelet, max2);
        long total_ebs = emu::repl_reduce(*edge_blocks_per_nodelet, std::plus<>());
        double ebs_per_vertex = total_ebs / g->num_vertices();
        LOG("Reserving %li MiB for %li edge blocks (avg. %3.2f per vertex)\n",
            (max_ebs * sizeof(EdgeBlock)) >> 20,
            total_ebs,
            ebs_per_vertex);

        // Allocate a chunk of memory for storing edge blocks
        g->edge_block_storage_ = new repl_storage<EdgeBlock>(ebs_per_vertex);

        // Allocate edge blocks for each vertex
        g->for_each_vertex([g](long v) {
            auto num_ebs = g->vertex_num_edge_blocks_[v];
            auto storage = g->edge_block_storage_;
            g->vertex_edge_blocks_[v] = storage->allocate(num_ebs);
            // Initialize each edge block
            for (long i = 0; i < num_ebs; ++i) {
                EdgeBlock& eb = g->vertex_edge_blocks_[v][i];
                eb.num_edges_ = 0;
                eb.edges_ = nullptr;
            }
        });

        // Now that we know where each edge will go, scan the edge list again
        // This time increment the size of each edge block
        dist_el.forall_edges([g](long src, long dst) {
            auto out_eb = g->lookup_edge_block(src, dst);
            emu::remote_add(&out_eb->num_edges_, 1);
            auto in_eb = g->lookup_edge_block(dst, src);
            emu::remote_add(&in_eb->num_edges_, 1);
        });

        // Count how many edges will need to be stored on each nodelet
        // This is in preparation for the next step, so we can do one big allocation
        // instead of a bunch of tiny ones.
        LOG("Counting local edges...\n");
        hooks_region_begin("count_local_edges");
        auto num_local_edges = emu::make_repl<long>(0);
        g->for_each_vertex([g, num_local = *num_local_edges](long v) {
            auto num_ebs = g->vertex_num_edge_blocks_[v];
            auto home_nlet = g->vertex_to_nlet(v);
            for (long i = 0; i < num_ebs; ++i) {
                EdgeBlock &eb = g->vertex_edge_blocks_[v][i];
                emu::remote_add(num_local.get_nth(home_nlet + i), eb.num_edges_);
            }
        });
        hooks_region_end();

        LOG("Allocating edge storage...\n");
        // Run around and compute the largest number of edges on any nodelet
        long max_edges_per_nodelet = emu::repl_reduce(*num_local_edges, max2);
        // Double-check that we haven't lost any edges
        assert(2 * g->num_edges_ ==
               emu::repl_reduce(g->num_local_edges_, std::plus<>()));

        LOG("Will use %li MiB on each nodelet\n", (max_edges_per_nodelet * sizeof(long)) >> 20);

        // Allocate a big stripe, such that there is enough room for the nodelet
        // with the most local edges
        // There will be wasted space on the other nodelets
        g->edge_storage_ = new repl_storage<Edge>(max_edges_per_nodelet);

        // Assign each edge block a position within the big array
        LOG("Carving edge storage...\n");
        hooks_region_begin("carve_edge_storage");
        g->for_each_vertex([g](long v) {
            // TODO allocate edges for each block on the right nodelet
                // Local vertices have one edge block on the local nodelet
                g->vertex_out_neighbors_[v] = g->edge_storage_->allocate()
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
        dist_el.forall_edges([g](long src, long dst) {
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
}; // end class graph_base