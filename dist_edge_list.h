#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/replicated.h>

// Distributed edge list that the graph will be created from.
// First array stores source vertex ID, second array stores dest vertex ID
struct dist_edge_list
{
    // Largest vertex ID + 1
    emu::repl<long> num_vertices_;
    // Length of both arrays
    emu::repl<long> num_edges_;
    // Striped array of source vertex ID's
    emu::repl_copy<emu::striped_array<long>> src_;
    // Striped array of dest vertex ID's
    emu::repl_copy<emu::striped_array<long>> dst_;

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

    // Creates distributed edge list from file
    static std::unique_ptr<emu::repl_copy<dist_edge_list>>
    load(const char* filename);
    // Creates distributed edge list from file
    // Reads from all nodelets at once
    // Assumes filename is the same on all nodelets
    static std::unique_ptr<emu::repl_copy<dist_edge_list>>
    load_distributed(const char* filename);
    // Print the edge list to stdout for debugging
    void dump() const;

    template<typename F, typename... Args>
    void forall_edges(long grain, F worker, Args&&... args)
    {
        striped_array_apply(src_.data(), src_.size(), grain, worker, std::forward<Args>(args)...);
    }
};
