#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/pointer_manipulation.h>
#include <emu_cxx_utils/for_each.h>

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
        emu::parallel::for_each(policy,
            vertex_out_degree_.begin(), vertex_out_degree_.end(),
            [worker,degree_begin=vertex_out_degree_.begin()](long &degree) {
                // HACK Compute index in table from the pointer
                long vertex_id = &degree - degree_begin;
                worker(vertex_id);
            }
        );
    }

    template<class Function>
    void for_each_vertex(Function worker) {
        for_each_vertex(emu::execution::default_policy, worker);
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
        for_each_out_edge(emu::execution::default_policy, src, worker);
    }

    // Optimized functor for iterating over edge lists
    template<typename Function>
    struct edge_visitor
    {
        Function worker_;
        explicit edge_visitor(Function worker) : worker_(worker) {}

        void
        operator()(edge_iterator begin, edge_iterator end)
        {
            // Visit neighbors one at a time until remainder is evenly divisible by four
            while ((end - begin) % 4 != 0) {
                worker_(*begin++);
            }
            long e1, e2, e3, e4;
            for (long *e = begin; e != end;) {
                // Pick up four edges
                e4 = *e++;
                e3 = *e++;
                e2 = *e++;
                e1 = *e++;
                // Visit each neighbor without returning home
                // Once an edge has been traversed, we can resize to
                // avoid carrying it along with us.
                worker_(e1);
                worker_(e2);
                worker_(e3);
                worker_(e4);
                RESIZE();
            }
        }
    };

    template<class Function>
    void for_each_out_edge_grouped(emu::execution::parallel_policy policy, long src, Function worker)
    {
        auto begin = out_edges_begin(src);
        auto end = out_edges_end(src);
        long grain = policy.grain_;
        // Spawn a thread for each granule
        for (; begin != end;) {
            // Last iteration will be less than a full granule
            auto last = begin + grain; if (last > end) { last = end; }
            cilk_spawn edge_visitor{worker}(begin, last);
            begin = last;
        }
    }

    template<class Function>
    void for_each_out_edge_grouped(emu::execution::sequenced_policy policy, long src, Function worker)
    {
        edge_visitor{worker}(out_edges_begin(src), out_edges_end(src));
    }


    // Optimized functor for searching through edge lists
    template<typename Function>
    struct edge_searcher
    {
        Function worker_;
        explicit edge_searcher(Function worker) : worker_(worker) {}

        bool
        operator()(edge_iterator begin, edge_iterator end)
        {
            // Visit neighbors one at a time until remainder is evenly divisible by four
            while ((end - begin) % 4 != 0) {
                if (worker_(*begin++)) { return true; }
            }
            long e1, e2, e3, e4;
            for (long *e = begin; e != end;) {
                // Pick up four edges
                e4 = *e++;
                e3 = *e++;
                e2 = *e++;
                e1 = *e++;
                // Visit each neighbor without returning home
                // Once an edge has been traversed, we can resize to
                // avoid carrying it along with us.
                if (worker_(e1)) { return true; }
                if (worker_(e2)) { return true; }
                if (worker_(e3)) { return true; }
                if (worker_(e4)) { return true; }
                RESIZE();
            }
            return false;
        }
    };

    template<class Function>
    void find_out_edge_grouped(emu::execution::sequenced_policy policy, long src, Function worker)
    {
        edge_searcher{worker}(out_edges_begin(src), out_edges_end(src));
    }

    template<class Compare>
    void
    sort_edge_lists(Compare comp)
    {
        hooks_region_begin("sort_edge_lists");
        for_each_vertex([&](long v){
            std::sort(out_edges_begin(v), out_edges_end(v), comp);
        });
        hooks_region_end();
    }


};