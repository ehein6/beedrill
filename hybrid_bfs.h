#pragma once

#include <vector>
#include <queue>
#include "graph.h"
#include "sliding_queue.h"
#include "bitmap.h"
#include "ack_control.h"


class hybrid_bfs {
private:
    emu::repl<graph*> g_;
    // For each vertex, parent in the BFS tree.
    emu::repl_copy<emu::striped_array<long>> parent_;
    // Temporary copy of parent array
    emu::repl_copy<emu::striped_array<long>> new_parent_;
    // Used to store vertices to visit in the next frontier
    emu::repl_copy<sliding_queue> queue_;
    // Bitmap representation of the current frontier
    emu::repl_copy<bitmap> frontier_;
    // Bitmap representation of the next frontier
    emu::repl_copy<bitmap> next_frontier_;
    // Tracks the sum of the degrees of vertices in the frontier
    long scout_count_;

    void queue_to_bitmap();
    void bitmap_to_queue();

    void top_down_step_with_remote_writes();
    void top_down_step_with_migrating_threads();
    long bottom_up_step();

public:

    void run_with_remote_writes(long source);
    void run_with_migrating_threads(long source);
    void run_with_remote_writes_hybrid(long source, long alpha, long beta);
    void run_beamer(long source, long alpha, long beta);

    explicit
    hybrid_bfs(graph & g)
    : g_(&g)
    , parent_(g.num_vertices())
    , new_parent_(g.num_vertices())
    , queue_(g.num_vertices())
    , frontier_(g.num_vertices())
    , next_frontier_(g.num_vertices())
    , scout_count_(0)
    {
        // Force ack controller singleton to initialize itself
        ack_controller::instance();
        clear();
    }

    // Shallow copy constructor
    hybrid_bfs(const hybrid_bfs& other, emu::shallow_copy tag)
    : g_(other.g_)
    , parent_(other.parent_, tag)
    , new_parent_(other.new_parent_, tag)
    , queue_(other.queue_, tag)
    , frontier_(other.frontier_, tag)
    , next_frontier_(other.next_frontier_, tag)
    , scout_count_(other.scout_count_)
    {}




    // FIXME
//    static void
//    scout_count_allreduce()
//    {
//        // Add up all replicated copies
//        long sum = 0;
//        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
//            long local_scout_count = *(long*)mw_get_nth(&HYBRID_BFS.scout_count, nlet);
//            sum += local_scout_count;
//        }
//        // Set all copies to the sum
//        mw_replicated_init(&HYBRID_BFS.scout_count, sum);
//    }
    void clear();
    bool check(long source);
    void print_tree();
    long count_num_traversed_edges();
    void dump_queue_stats();
};



