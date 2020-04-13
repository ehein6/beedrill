#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/for_each.h>
#include "common.h"
#include "edge_list.h"

// Forward reference
namespace emu { class fileset; }

// Distributed edge list that the graph will be created from.
// First array stores source vertex ID, second array stores dest vertex ID
struct dist_edge_list
{
private:
    // Largest vertex ID + 1
    emu::repl<long> num_vertices_;
    // Length of both arrays
    emu::repl<long> num_edges_;
    // Striped array of source vertex ID's
    emu::striped_array<long> src_;
    // Striped array of dest vertex ID's
    emu::striped_array<long> dst_;

public:
    // Default constructor
    dist_edge_list() = default;

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

    long num_vertices() const { return num_vertices_; }
    long num_edges() const { return num_edges_; }

    // Smart pointer to a replicated dist_edge_list
    using handle = std::unique_ptr<emu::repl_shallow<dist_edge_list>>;

    // Load distributed edge list from fileset (one slice per nodelet)
    static handle
    load_distributed(const char* filename);

    // Load distributed edge list from file
    // Supports files that are larger than the memory of a single
    // nodelet by doing a buffered load
    static handle
    load_binary(const char* filename);

    // Print the edge list to stdout for debugging
    void dump() const;

    template<class Policy, class Function>
    void forall_edges(Policy policy, Function worker)
    {
        emu::parallel::for_each(policy, src_.begin(), src_.end(),
            [this, src_begin=src_.begin(), worker](long& src) {
                // HACK Compute index in table from the pointer
                long i = &src - src_begin;
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

    friend void serialize(emu::fileset& f, dist_edge_list& self);
    friend void deserialize(emu::fileset& f, dist_edge_list& self);
};
