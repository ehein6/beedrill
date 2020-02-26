#pragma once
#include "graph_base.h"

struct edge_slot {
    long dst;
    // Most graph algos were written when an edge was just a long int, this
    // operator allows most of that code to work
    operator long() const { return dst; }
};

// This class will be compiled in graph.cc, just declare it here
extern template class graph_base<edge_slot>;
/**
 * Basic graph with no edge properties
 */
class graph : public graph_base<edge_slot>
{
    // Inherit constructors
    using graph_base::graph_base;
};