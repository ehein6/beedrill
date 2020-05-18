#pragma once

#include <vector>
#include <queue>
#include <emu_cxx_utils/execution_policy.h>
#include "common.h"
#include "graph.h"
#include "sliding_queue.h"
#include "worklist.h"

class hybrid_bfs {
private:
    emu::repl<graph*> g_;
    // For each vertex, parent in the BFS tree.
    emu::striped_array<long> parent_;
    // Temporary copy of parent array
    emu::striped_array<long> new_parent_;
    // Used to store vertices to visit in the next frontier
    sliding_queue queue_;
    // Tracks the sum of the degrees of vertices in the frontier
    // Declared here to avoid re-allocating before each step
    emu::repl<long> scout_count_;
    emu::repl<long> awake_count_;

    worklist<graph::edge_type> worklist_;

    long count_num_traversed_edges();
    void dump_queue_stats();

    long top_down_step_with_remote_writes();
    long top_down_step_with_migrating_threads();
    long bottom_up_step();

public:

    void run_with_remote_writes(long source, long max_level);
    void run_with_migrating_threads(long source, long max_level);
    void run_with_remote_writes_hybrid(long source, long max_level, long alpha, long beta);
    void run_beamer(long source, long max_level, long alpha, long beta);

    explicit hybrid_bfs(graph & g);
    hybrid_bfs(const hybrid_bfs& other, emu::shallow_copy tag);

    void clear();
    bool check(long source);
    void print_tree();

    struct stats {
        // Depth of the BFS tree
        long max_level = 0;
        // Total number of edges touched during the BFS
        long num_edges_traversed = 0;
        // Number of vertices in each level
        std::vector<long> frontier_size;
    };

    stats compute_stats();
};
