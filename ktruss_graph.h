#pragma once
#include "graph_base.h"

struct ktruss_edge_slot
{
    // Destination vertex ID
    long dst;
    // Number of triangles this edge is involved in
    long TC;
    // Max K-truss number for this edge
    long KTE;

    // TODO These fields are not active at the same time, could save a lot of
    // space by putting them all in a union.

    // Most graph algos were written when an edge was just a long int, this
    // operator allows most of that code to work
    operator long() const { return dst; }
};

// This class will be compiled in graph.cc, just declare it here
extern template class graph_base<ktruss_edge_slot>;
/**
 * Graph with edge properties for computing k-truss
 */
class ktruss_graph : public graph_base<ktruss_edge_slot>
{
    // Inherit constructors
    using graph_base::graph_base;

    friend std::unique_ptr<emu::repl_shallow<ktruss_graph>>
    create_graph_from_edge_list(dist_edge_list & dist_el);
};