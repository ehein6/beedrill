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
    long * edge_storage_;
    // Pointer to un-reserved edge storage in local stripe
    long * next_edge_storage_;

    // Constructor
    graph(long num_vertices, long num_edges)
    : num_vertices_(num_vertices)
    , num_edges_(num_edges)
    , vertex_out_degree_(num_vertices)
    , vertex_out_neighbors_(num_vertices)
    {}

    // Shallow copy constructor
    graph(const graph& other, emu::shallow_copy)
    : num_vertices_(other.num_vertices_)
    , num_edges_(other.num_edges_)
    // Make shallow copies for striped arrays
    , vertex_out_degree_(other.vertex_out_degree_, emu::shallow_copy())
    , vertex_out_neighbors_(other.vertex_out_neighbors_, emu::shallow_copy())
    {
    }

    graph(const graph& other) = delete;

    /**
     * This is NOT a general purpose edge insert function, it relies on assumptions
     * - The edge block for this vertex (local or remote) has enough space for the edge
     * - The out-degree for this vertex is counting up from zero, representing the number of edges stored
     */
    void
    insert_edge(long src, long dst)
    {
        // Get the local edge array
        long * edges = vertex_out_neighbors_[src];
        long * num_edges_ptr = &vertex_out_degree_[src];
        // Atomically claim a position in the edge list and insert the edge
        // NOTE: Relies on all edge counters being set to zero in the previous step
        edges[ATOMIC_ADDMS(num_edges_ptr, 1)] = dst;
    }


public:

    static std::unique_ptr<emu::repl_copy<graph>>
    from_edge_list(dist_edge_list & dist_el);
    bool check(dist_edge_list& dist_el);
    void dump();
    void print_distribution();

    long num_vertices () const
    {
        return num_vertices_;
    }

    long num_edges () const
    {
        return num_edges_;
    }

    long out_degree(long vertex_id) const
    {
        return vertex_out_degree_[vertex_id];
    }

    long * out_neighbors(long vertex_id) {
        return vertex_out_neighbors_[vertex_id];
    }

    bool
    out_edge_exists(long src, long dst)
    {
        // Find the edge block that would contain this neighbor
        long * edges_begin = vertex_out_neighbors_[src];
        long * edges_end = edges_begin + vertex_out_degree_[src];;

        // Search for the neighbor
        for (long * e = edges_begin; e < edges_end; ++e) {
            assert(*e >= 0);
            assert(*e < num_vertices_);
            if (*e == dst) { return true; }
        }

        // Neighbor not found
        return false;
    }

    // Map a function to all vertices in parallel, using the specified policy
    template<typename P, typename F, typename... Args>
    void forall_vertices(P policy, F worker, Args&&... args)
    {
        emu::parallel::striped_for_each_i(policy,
            vertex_out_degree_.data(), 0L, vertex_out_degree_.size(),
            worker, std::forward<Args>(args)...
        );
    }

    /**
     * Map a function in parallel across all neighbors of vertex @c vertex_id
     *
     * Due to limitations in the Emu compiler, prefer to pass additional arguments
     * in @c args rather than relying on automatic lambda capture.
     * @tparam F Prototype is void (*worker)(long destination_vertex_id, args...)
     * @tparam Args
     * @param src Vertex
     * @param grain Minimum number of neighbors to give to each thread
     * @param worker Worker function, will be called on each neighbor
     * @param args Additional arguments that will be forwarded to the worker function
     */
    template<typename Policy, typename Function, typename... Args>
    void forall_out_neighbors(
        Policy policy,
        long src, Function worker, Args&&... args
    ){
        // Find the edge list for this vertex
        long * edges_begin = vertex_out_neighbors_[src];
        long degree = vertex_out_degree_[src];
        long * edges_end = edges_begin + degree;
        // Spawn threads over the range according to the specified policy
        emu::parallel::for_each(
            policy, edges_begin, edges_end,
            [](long dst, long src, Function worker, Args&&... args) {
                worker(src, dst, std::forward<Args>(args)...);
            }, src, worker, std::forward<Args>(args)...
        );
    }
};
