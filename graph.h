#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/pointer_manipulation.h>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/find.h>
#include <emu_cxx_utils/repl_array.h>

#include "common.h"
#include "dist_edge_list.h"

// Global data structures
struct graph {
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
    emu::striped_array<long *> vertex_out_neighbors_;

    // Pointer to chunk of memory on each nodelet for storing edges
    emu::repl<emu::repl_array<long> *> edge_storage_;
    // Total number of edges stored on each nodelet
    emu::repl<long> num_local_edges_;
    // Pointer to un-reserved edge storage in local stripe
    long *next_edge_storage_;

    // Constructor
    graph(long num_vertices, long num_edges)
    : num_vertices_(num_vertices)
    , num_edges_(num_edges)
    , vertex_id_(num_vertices)
    , vertex_out_degree_(num_vertices)
    , vertex_out_neighbors_(num_vertices)
    {}

    // Shallow copy constructor
    graph(const graph &other, emu::shallow_copy shallow)
    : num_vertices_(other.num_vertices_)
    , num_edges_(other.num_edges_)
    // Make shallow copies for striped arrays
    , vertex_id_(other.vertex_id_, shallow)
    , vertex_out_degree_(other.vertex_out_degree_, shallow)
    , vertex_out_neighbors_(other.vertex_out_neighbors_, shallow)
    {}

    graph(const graph &other) = delete;

    ~graph()
    {
        // Note: Could avoid new/delete here by using a unique_ptr, but we
        // can't safely replicate those yet.
        delete edge_storage_;
    }

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
        edges[emu::atomic_addms(num_edges_ptr, 1)] = dst;
    }


public:

    static std::unique_ptr<emu::repl_shallow<graph>>
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

    long * vertices_begin() { return vertex_id_.begin(); }
    long * vertices_end() { return vertex_id_.end(); }

    // TODO: more complex edge list data structures will require something
    // fancier than a pointer
    using edge_iterator = long*;
    using const_edge_iterator = const long*;

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
    long* find_out_edge_if(Policy policy, long src, Function worker)
    {
        return emu::parallel::find_if(
            policy, out_edges_begin(src), out_edges_end(src), worker
        );
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
};