#pragma once

#include "common.h"
#include "graph.h"

class pagerank {
private:
    emu::repl<graph*> g_;
    // PageRank value for each vertex
    emu::striped_array<double> scores_;
    // Outgoing contribution from each vertex
    emu::striped_array<long> contrib_;
    // Accumulates error from each step
    emu::repl<double> error_;

public:
    explicit pagerank(graph & g);
    pagerank(const pagerank& other, emu::shallow_copy tag);

    void run (int max_iters, double damping, double epsilon);
    void clear();
    bool check();
};
