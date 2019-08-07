#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/striped_for_each.h>

#include "common.h"
#include "dist_edge_list.h"

// Global data structures
struct graph {
    // Total number of vertices in the graph (max vertex ID + 1)
    emu::repl<long> num_vertices_;
    // Total number of edges in the graph
    emu::repl<long> num_edges_;

    // Distributed vertex array
    // number of neighbors for this vertex (on all nodelets)
    emu::striped_array<long> vertex_out_degree_;

    // Pointer to local edge array (light vertices only)
    // OR replicated edge block pointer (heavy vertices only)
    emu::striped_array<long *> vertex_out_neighbors_;

    // Total number of edges stored on each nodelet
    long num_local_edges_;
    // Pointer to stripe of memory where edges are stored
    long *edge_storage_;
    // Pointer to un-reserved edge storage in local stripe
    long *next_edge_storage_;

    // Constructor
    graph(long num_vertices, long num_edges)
        : num_vertices_(num_vertices), num_edges_(num_edges), vertex_out_degree_(num_vertices),
          vertex_out_neighbors_(num_vertices) {}

    // Shallow copy constructor
    graph(const graph &other, emu::shallow_copy)
        : num_vertices_(other.num_vertices_), num_edges_(other.num_edges_)
        // Make shallow copies for striped arrays
        , vertex_out_degree_(other.vertex_out_degree_, emu::shallow_copy()),
          vertex_out_neighbors_(other.vertex_out_neighbors_, emu::shallow_copy()) {
    }

    graph(const graph &other) = delete;

    /**
     * This is NOT a general purpose edge insert function, it relies on assumptions
     * - The edge block for this vertex (local or remote) has enough space for the edge
     * - The out-degree for this vertex is counting up from zero, representing the number of edges stored
     */
    void
    insert_edge(long src, long dst) {
        // Get the local edge array
        long *edges = vertex_out_neighbors_[src];
        long *num_edges_ptr = &vertex_out_degree_[src];
        // Atomically claim a position in the edge list and insert the edge
        // NOTE: Relies on all edge counters being set to zero in the previous step
        edges[ATOMIC_ADDMS(num_edges_ptr, 1)] = dst;
    }


public:

    static std::unique_ptr<emu::repl_copy<graph>>
    from_edge_list(dist_edge_list &dist_el);

    bool check(dist_edge_list &dist_el);

    void dump();

    void print_distribution();

    long num_vertices() const {
        return num_vertices_;
    }

    long num_edges() const {
        return num_edges_;
    }

    long out_degree(long vertex_id) const {
        return vertex_out_degree_[vertex_id];
    }

    long *out_neighbors(long vertex_id) {
        return vertex_out_neighbors_[vertex_id];
    }

    bool
    out_edge_exists(long src, long dst) {
        // Find the edge block that would contain this neighbor
        long *edges_begin = vertex_out_neighbors_[src];
        long *edges_end = edges_begin + vertex_out_degree_[src];;

        // Search for the neighbor
        // TODO replace with std::find
        for (long *e = edges_begin; e < edges_end; ++e) {
            assert(*e >= 0);
            assert(*e < num_vertices_);
            if (*e == dst) { return true; }
        }

        // Neighbor not found
        return false;
    }

    // Map a function to all vertices in parallel, using the specified policy
    template<class Policy, class Function>
    void forall_vertices(Policy policy, Function worker) {
        emu::parallel::for_each(policy,
            vertex_out_degree_.begin(), vertex_out_degree_.end(),
            [&](long &degree) {
                // Compute index in table from the pointer
                long vertex_id = &degree - vertex_out_degree_.begin();
                worker(vertex_id);
            }
        );
    }

    template<class Function>
    void forall_vertices(Function worker) {
        forall_vertices(emu::execution::default_policy, worker);
    }

    using edge_iterator = long*;

    edge_iterator
    out_edges_begin(long src)
    {
        // Find the edge list for this vertex
        return vertex_out_neighbors_[src];
    }

    edge_iterator
    out_edges_end(long src)
    {
        return out_edges_begin(src) + vertex_out_degree_[src];
    }

    template<class Compare>
    void
    sort_edge_lists(Compare comp)
    {
        hooks_region_begin("sort_edge_lists");
        forall_vertices([&](long v){
            std::sort(out_edges_begin(v), out_edges_end(v), comp);
        });
        hooks_region_end();
    }


    /**
     * Map a function in parallel across all neighbors of vertex @c vertex_id
     *
     * @tparam F Prototype is void (*worker)(long destination_vertex_id, args...)
     * @param src Vertex
     * @param grain Minimum number of neighbors to give to each thread
     * @param worker Worker function, will be called on each neighbor
     */
    template<class Policy, class Function>
    void forall_out_neighbors(Policy policy, long src, Function worker)
    {
        // Spawn threads over the range according to the specified policy
        emu::parallel::for_each(policy, out_edges_begin(src), out_edges_end(src),
            [&](long dst) {
                worker(src, dst);
            }
        );
    }

    template<class Function>
    void forall_out_neighbors(long src, Function worker)
    {
        forall_out_neighbors(emu::execution::default_policy, src, worker);
    }
};