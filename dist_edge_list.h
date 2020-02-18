#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/for_each.h>
#include "common.h"

// Distributed edge list that the graph will be created from.
// First array stores source vertex ID, second array stores dest vertex ID
struct dist_edge_list
{
    // Largest vertex ID + 1
    emu::repl<long> num_vertices_;
    // Length of both arrays
    emu::repl<long> num_edges_;
    // Striped array of source vertex ID's
    emu::striped_array<long> src_;
    // Striped array of dest vertex ID's
    emu::striped_array<long> dst_;

    // Constructor
    dist_edge_list(long num_vertices, long num_edges)
    : num_vertices_(num_vertices)
    , num_edges_(num_edges)
    , src_(num_edges)
    , dst_(num_edges)
    {}

    // Shallow copy constructor
    dist_edge_list(const dist_edge_list& other, emu::shallow_copy)
    : num_vertices_(other.num_vertices_)
    , num_edges_(other.num_edges_)
    , src_(other.src_, emu::shallow_copy())
    , dst_(other.dst_, emu::shallow_copy())
    {}

    dist_edge_list(const dist_edge_list& other) = delete;

    // Smart pointer to a replicated dist_edge_list
    using handle = std::unique_ptr<emu::repl_shallow<dist_edge_list>>;

    /**
     * Load edge list from matrix market (.mtx) file
     * @param filename
     * @return
     */
    static handle
    load(const char* filename);

    // Creates distributed edge list from file
    // Supports files that are larger than the memory of a single
    // nodelet by doing a buffered load
    static handle
    load_binary(const char* filename);

    /**
     * Load edge list from matrix market (.mtx) file
     * @param filename
     * @return
     */
    static handle
    load_mtx(const char* filename);

    // Creates distributed edge list from file
    // Reads from all nodelets at once
    // Assumes filename is the same on all nodelets
    static handle
    load_distributed(const char* filename);
    // Print the edge list to stdout for debugging
    void dump() const;

    template<class Policy, class Function>
    void forall_edges(Policy policy, Function worker)
    {
        emu::parallel::for_each(policy, src_.begin(), src_.end(),
            [&](long& src) {
                // HACK Compute index in table from the pointer
                long i = &src - src_.begin();
                long dst = dst_[i];
                worker(src, dst);
            }
        );
    }

    template<class Function>
    void forall_edges(Function worker)
    {
        forall_edges(emu::default_policy, worker);
    }
};
