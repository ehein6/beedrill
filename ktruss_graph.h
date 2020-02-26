#pragma once
#include "graph_base.h"

struct ktruss_edge_slot
{
    // Destination vertex ID
    long dst;
    // Number of triangles this edge is involved in
    long TC;
    union {
        // Number of triangles for which this edge is a "qr edge"
        long qrC;
        // Reference count of all 'p' vertices for closing triangles,
        // for each 'q' vertex
        //     Note: This field is only set on the reverse edges, so it is safe
        //     to put in a union
        long pRefC;
    };
    // Max K-truss number for this edge
    long KTE;

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
public:

    using graph_base::graph_base;

    // Allows us to remove edges from the graph
    void
    set_out_degree(long src, long degree)
    {
        assert(degree <= vertex_out_degree_[src]);
        vertex_out_degree_[src] = degree;
        // TODO how to decrement num_edges?
    }

    friend std::unique_ptr<emu::repl_shallow<ktruss_graph>>
    create_graph_from_edge_list(dist_edge_list & dist_el);
};