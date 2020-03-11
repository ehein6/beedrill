#pragma once

#include "common.h"
#include "graph.h"
#include "worklist.h"

class pagerank
{
private:
    emu::repl<graph*> g_;
    // PageRank value for each vertex
    emu::striped_array<double> scores_;
    // Outgoing contribution from each vertex
    emu::striped_array<double> contrib_;
    // Incoming contribution from other vertices
    emu::striped_array<double> incoming_;
    // Accumulates error from each step
    emu::repl<double> error_;
    // Constants related to applying the damping factor
    emu::repl<double> base_score_;
    emu::repl<double> damping_;

    worklist worklist_;
public:
    explicit pagerank(graph & g);
    pagerank(const pagerank& other, emu::shallow_copy tag);
    /**
     * Runs the PageRank algorithm on the graph until convergence or
     * the maximum number of iterations have been reached.
     * @param max_iters Maximum number of iterations to run
     * @param damping Damping parameter during rank update
     * @param epsilon Stop iterating when change in rank is smaller than this
     * @return Number of iterations performed
     */
    int run (int max_iters, double damping, double epsilon);
    void clear();
    bool check(double damping, double epsilon);

    double operator[] (size_t i) const { return scores_[i]; }
};
