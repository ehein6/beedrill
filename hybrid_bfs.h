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
    emu::repl<long> scout_count_;

public:

    enum alg {
        REMOTE_WRITES,
        MIGRATING_THREADS,
        REMOTE_WRITES_HYBRID,
        BEAMER_HYBRID,
    };

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
        ack_control_init();
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

    void
    queue_to_bitmap()
    {
        // For each item in the queue, set the corresponding bit in the bitmap
        queue_.forall_items([](long i, bitmap& b) {
            b.set_bit(i);
        }, frontier_);
    }

    void
    bitmap_to_queue()
    {
        // For each vertex, test whether the bit in the bitmap is set
        // If so, add the index of the bit to the queue
        striped_array_apply(parent_.data(), parent_.size(), 64,
            [](long v, bitmap & b, sliding_queue & q) {
                if (b.get_bit(v)) {
                    q.push_back(v);
                }
            }, frontier_, queue_
        );
    }

    void
    clear()
    {
        // Initialize the parent array with the -degree of the vertex
        striped_array_apply(parent_.data(), parent_.size(), 128,
            [](long v, graph & g, hybrid_bfs & bfs) {
                long out_degree = g.out_degree(v);
                bfs.parent_[v] = out_degree != 0 ? -out_degree : -1;
                bfs.new_parent_[v] = -1;
            }, *g_, *this
        );
        // Reset the queue
        queue_.reset_all();
        // Clear out both bitmaps
        frontier_.clear();
        next_frontier_.clear();
    }

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




/**
 * Top-down BFS step ("remote writes" variant)
 * Fire off a remote write for each edge in the frontier
 * This write travels to the home node for the destination vertex,
 * setting the source vertex as its parent.
 * Return the sum of the degrees of the vertices in the new frontier
 *
 * Overview of top_down_step_with_remote_writes()
 *   DISABLE ACKS
 *   spawn mark_queue_neighbors() on each nodelet
 *     spawn mark_queue_neighbors_worker() over a slice of the local queue
 *       IF LIGHT VERTEX
 *       call mark_neighbors_parallel() on a local array of edges
 *         call/spawn mark_neighbors() over a local array of edges
 *       ELSE IF HEAVY VERTEX
 *       spawn mark_neighbors_in_eb() for each remote edge block
 *         call mark_neighbors_parallel() on the local edge block
 *           call/spawn mark_neighbors() over the local edge block
 *   RE-ENABLE ACKS
 *   SYNC
 *   spawn populate_next_frontier() over all vertices
*/

    void
    top_down_step_with_remote_writes()
    {
        ack_control_disable_acks();
        // For each vertex in the queue...
        queue_.forall_items([] (long v, graph& g, long * new_parent) {
            // for each neighbor of that vertex...
            g.forall_out_neighbors(v, 512, [](long src, long dst, long * new_parent) {
                // write the vertex ID to the neighbor's new_parent entry.
                new_parent[dst] = src; // Remote write
            }, new_parent);
        }, *g_, new_parent_.data());
        ack_control_reenable_acks();

        // Add to the queue all vertices that didn't have a parent before
//        scout_count_ = 0;
        g_->forall_vertices(128, [](long v, hybrid_bfs & bfs) {
            if (bfs.parent_[v] < 0 && bfs.new_parent_[v] >= 0) {
                // Update count with degree of new vertex
//                bfs.scout_count_ += -bfs.parent_[v];
                // Set parent
                bfs.parent_[v] = bfs.new_parent_[v];
                // Add to the queue for the next frontier
                bfs.queue_.push_back(v);
            }
        }, *this);
//        scout_count_allreduce();
    }
//
///**
// * Top-down BFS step ("migrating threads" variant)
// * For each edge in the frontier, migrate to the dst vertex's nodelet
// * If the dst vertex doesn't have a parent, set src as parent
// * Then append to local queue for next frontier
// * Return the sum of the degrees of the vertices in the new frontier
// *
// * Overview of top_down_step_with_migrating_threads()
// *   spawn explore_local_frontier() on each nodelet
// *     spawn explore_frontier_spawner() over a slice of the local queue
// *       IF LIGHT VERTEX
// *       call explore_frontier_parallel() on a local array of edges
// *         call/spawn frontier_visitor over a local array of edges
// *       ELSE IF HEAVY VERTEX
// *       spawns explore_frontier_in_eb() for each remote edge block
// *         call explore_frontier_parallel() on the local edge block
// *           call/spawn frontier_visitor over the local edge block
//*/
//
//    static inline void
//    visit(long src, long dst)
//    {
//        // Look up the parent of the vertex we are visiting
//        long * parent = &HYBRID_BFS.parent[dst];
//        long curr_val = *parent;
//        // If we are the first to visit this vertex
//        if (curr_val < 0) {
//            // Set self as parent of this vertex
//            if (ATOMIC_CAS(parent, src, curr_val) == curr_val) {
//                // Add it to the queue
//                sliding_queue_push_back(&HYBRID_BFS.queue, dst);
//                REMOTE_ADD(&HYBRID_BFS.scout_count, -curr_val);
//            }
//        }
//    }
//
//// Using noinline to minimize the size of the migrating context
//    static __attribute__((always_inline)) inline void
//    frontier_visitor(long src, long * edges_begin, long * edges_end)
//    {
//        long e1, e2, e3, e4;
//
//        // Visit neighbors one at a time until remainder is evenly divisible by four
//        while ((edges_end - edges_begin) % 4 != 0) {
//            visit(src, *edges_begin++);
//        }
//
//        for (long * e = edges_begin; e < edges_end;) {
//            // Pick up four edges
//            e4 = *e++;
//            e3 = *e++;
//            e2 = *e++;
//            e1 = *e++;
//            // Visit each neighbor without returning home
//            // Once an edge has been traversed, we can resize to
//            // avoid carrying it along with us.
//            visit(src, e1); RESIZE();
//            visit(src, e2); RESIZE();
//            visit(src, e3); RESIZE();
//            visit(src, e4); RESIZE();
//        }
//    }
//
//    static inline void
//    explore_frontier_parallel(long src, long * edges_begin, long * edges_end)
//    {
//        long degree = edges_end - edges_begin;
//        long grain = 64;
//        if (degree <= grain) {
//            // Low-degree local vertex, handle in this thread
//            // TODO spawn here to separate from parent thread?
//            frontier_visitor(src, edges_begin, edges_end);
//        } else {
//            // High-degree local vertex, spawn local threads
//            for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
//                long * e2 = e1 + grain;
//                if (e2 > edges_end) { e2 = edges_end; }
//                cilk_spawn frontier_visitor(src, e1, e2);
//            }
//        }
//    }
//
//    void
//    explore_frontier_worker(sliding_queue * queue, long * queue_pos)
//    {
//        const long queue_end = queue->end;
//        const long * queue_buffer = queue->buffer;
//        long v = ATOMIC_ADDMS(queue_pos, 1);
//        for (; v < queue_end; v = ATOMIC_ADDMS(queue_pos, 1)) {
//            long src = queue_buffer[v];
//            long * edges_begin = G.vertex_out_neighbors[src].local_edges;
//            long * edges_end = edges_begin + G.vertex_out_degree[src];
//            explore_frontier_parallel(src, edges_begin, edges_end);
//        }
//    }
//
//    void
//    explore_local_frontier(sliding_queue * queue)
//    {
//        // Decide how many workers to create
//        long num_workers = 64;
//        long queue_size = sliding_queue_size(queue);
//        if (queue_size < num_workers) {
//            num_workers = queue_size;
//        }
//        // Spawn workers
//        long queue_pos = queue->start;
//        for (long t = 0; t < num_workers; ++t) {
//            cilk_spawn explore_frontier_worker(queue, &queue_pos);
//        }
//    }
//
//    void
//    top_down_step_with_migrating_threads()
//    {
//        // Spawn a thread on each nodelet to process the local queue
//        // For each neighbor without a parent, add self as parent and append to queue
//        mw_replicated_init(&HYBRID_BFS.scout_count, 0);
//        for (long n = 0; n < NODELETS(); ++n) {
//            sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
//            cilk_spawn_at(local_queue) explore_local_frontier(local_queue);
//        }
//        cilk_sync;
//        scout_count_allreduce();
//    }

    void
    dump_queue_stats()
    {
        printf("Queue contents: ");
        queue_.dump_all();
        printf("\n");
        fflush(stdout);

        printf("Bitmap contents: ");
        frontier_.dump();
        printf("\n");
        fflush(stdout);

        printf("Frontier size per nodelet: ");
        for (long n = 0; n < NODELETS(); ++n) {
            sliding_queue & local_queue = queue_.get_nth(n);
            printf("%li ", local_queue.size());
        }
        printf("\n");
        fflush(stdout);
    }

//
///**
// * Bottom-up BFS step
// * For each vertex that is not yet a part of the BFS tree,
// * check all in-neighbors to see if they are in the current frontier
// * using a replicated bitmap.
// * If a parent is found, put the child in the bitmap for the next frontier
// * Returns the number of vertices that found a parent (size of next frontier)
// *
// * Overview of bottom_up_step()
// *   spawn search_for_parent_worker() over the entire vertex list
// *     IF LIGHT VERTEX
// *     call search_for_parent_parallel() on a local array of edges
// *       call/spawn search_for_parent over a local array of edges
// *     ELSE IF HEAVY VERTEX
// *     spawn search_for_parent_in_remote_ebs()
// *       spawn search_for_parent_in_eb() at each remote edge block
// *         call search_for_parent_parallel() on the local edge block
// *           call/spawn search_for_parent() over the local edge block
// *
//*/
//
//    void
//    search_for_parent(long child, long * edges_begin, long * edges_end, long * awake_count)
//    {
//        // For each vertex connected to me...
//        for (long * e = edges_begin; e < edges_end; ++e) {
//            long parent = *e;
//            // If the vertex is in the frontier...
//            if (frontier_.get_bit(parent)) {
//                // Claim as a parent
//                parent_[child] = parent;
//                // Increment number of vertices woken up on this step
//                REMOTE_ADD(awake_count, 1);
//                // Put myself in the frontier
//                next_frontier_.set_bit(child);
//                // No need to keep looking for a parent
//                break;
//            }
//        }
//    }
//
//    static inline void
//    search_for_parent_parallel(long child, long * edges_begin, long * edges_end, long * awake_count)
//    {
//        long degree = edges_end - edges_begin;
//        long grain = MY_LOCAL_GRAIN_MIN(degree, 128);
//        if (degree <= grain) {
//            // Low-degree local vertex, handle in this thread
//            search_for_parent(child, edges_begin, edges_end, awake_count);
//        } else {
//            // High-degree local vertex, spawn local threads
//            long num_found = 0;
//            for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
//                long * e2 = e1 + grain;
//                if (e2 > edges_end) { e2 = edges_end; }
//                cilk_spawn search_for_parent(child, e1, e2, &num_found);
//            }
//            cilk_sync;
//            // If multiple parents were found, we only increment the counter once
//            if (num_found > 0) {
//                REMOTE_ADD(awake_count, 1);
//            }
//        }
//    }
//
//// Calls search_for_parent_parallel over a remote edge block
//    void
//    search_for_parent_in_eb(long child, edge_block * eb, long * awake_count)
//    {
//        search_for_parent_parallel(child, eb->edges, eb->edges + eb->num_edges, awake_count);
//    }
//
//    void
//    search_for_parent_in_remote_ebs(long v, long * awake_count)
//    {
//        // Heavy vertex, spawn a thread for each remote edge block
//        long num_found = 0;
//        edge_block * eb = G.vertex_out_neighbors[v].repl_edge_block;
//        for (long i = 0; i < NODELETS(); ++i) {
//            edge_block * remote_eb = mw_get_nth(eb, i);
//            cilk_spawn_at(remote_eb) search_for_parent_in_eb(v, remote_eb, &num_found);
//        }
//        cilk_sync;
//        // If multiple parents were found, we only increment the counter once
//        if (num_found > 0) {
//            REMOTE_ADD(awake_count, 1);
//        }
//    }
//
//// Spawns threads to call search_for_parent in parallel for a part of the vertex list
//    void
//    search_for_parent_worker(long * array, long begin, long end, va_list args)
//    {
//        long * awake_count = va_arg(args, long*);
//        // For each vertex in our slice of the queue...
//        long local_awake_count = 0;
//        for (long v = begin; v < end; v += NODELETS()) {
//            if (HYBRID_BFS.parent[v] < 0) {
//                // How big is this vertex?
//                if (is_heavy_out(v)) {
//                    // Heavy vertex, spawn a thread for each remote edge block
//                    cilk_spawn search_for_parent_in_remote_ebs(v, &local_awake_count);
//                } else {
//                    long * edges_begin = G.vertex_out_neighbors[v].local_edges;
//                    long * edges_end = edges_begin + G.vertex_out_degree[v];
//                    search_for_parent_parallel(v, edges_begin, edges_end, &local_awake_count);
//                }
//            }
//        }
//        cilk_sync;
//        // Update global count
//        REMOTE_ADD(awake_count, local_awake_count);
//    }
//
//    static long
//    bottom_up_step()
//    {
//        long awake_count = 0;
//        // TODO we can do the clear in parallel with other stuff
//        next_frontier_.clear();
//        emu_1d_array_apply(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 64),
//            search_for_parent_worker, &awake_count
//        );
//        return awake_count;
//    }
//
///**
// * Run BFS using Scott Beamer's direction-optimizing algorithm
// *  1. Do top-down steps with migrating threads until condition is met
// *  2. Do bottom-up steps until condition is met
// *  3. Do top-down steps with migrating threads until done
// */
//    void
//    hybrid_bfs_run_beamer (long source, long alpha, long beta)
//    {
//        assert(source < g_->num_vertices());
//
//        // Start with the source vertex in the first frontier, at level 0, and mark it as visited
//        queue_.push_back(source);
//        queue_.slide_all_windows();
//        parent_[source] = source;
//
//        long edges_to_check = g_->num_edges() * 2;
//        scout_count_ = g_->out_degree(source));
//
//        // While there are vertices in the queue...
//        while (!queue_.all_empty()) {
//            if (scout_count_ > edges_to_check / alpha) {
//                long awake_count, old_awake_count;
//                // Convert sliding queue to bitmap
//                // hooks_region_begin("queue_to_bitmap");
//                queue_to_bitmap();
//                // hooks_region_end();
//                awake_count = queue_.combined_size();
//                queue_.slide_all_windows();
//                // Do bottom-up steps for a while
//                do {
//                    old_awake_count = awake_count;
//                    // hooks_set_attr_i64("awake_count", awake_count);
//                    // hooks_region_begin("bottom_up_step");
//                    awake_count = bottom_up_step();
//                    std::swap(frontier_, next_frontier_);
//                    // hooks_region_end();
//                } while (awake_count >= old_awake_count ||
//                         (awake_count > g_->num_vertices() / beta));
//                // Convert back to a queue
//                // hooks_region_begin("bitmap_to_queue");
//                bitmap_to_queue();
//                queue_.slide_all_windows();
//                // hooks_region_end();
//                scout_count_ = 1;
//            } else {
//                edges_to_check -= scout_count_;
//                // Do a top-down step
//                // hooks_region_begin("top_down_step");
//                top_down_step_with_migrating_threads();
//                // Slide all queues to explore the next frontier
//                queue_.slide_all_windows();
//                // hooks_region_end();
//            }
//            // dump_queue_stats();
//        }
//    }
//
//    /**
//     * Run BFS using top-down steps with migrating threads
//     */
//    void
//    hybrid_bfs_run_with_migrating_threads(long source)
//    {
//        assert(source < G.num_vertices);
//
//        // Start with the source vertex in the first frontier, at level 0, and mark it as visited
//        queue_.get_nth(0).push_back(source);
//        queue_.slide_all_windows();
//        parent_[source] = source;
//
//        // While there are vertices in the queue...
//        while (!queue_.all_empty()) {
//            // Explore the frontier
//            top_down_step_with_migrating_threads();
//            // Slide all queues to explore the next frontier
//            queue_.slide_all_windows();
//        }
//    }
//
    /**
     * Run BFS using top-down steps with remote writes
     */
    void
    run_with_remote_writes(long source)
    {
        assert(source < g_->num_vertices());

        // Start with the source vertex in the first frontier, at level 0, and mark it as visited
        queue_.get_nth(0).push_back(source);
        queue_.slide_all_windows();
        parent_[source] = source;

        // While there are vertices in the queue...
        while (!queue_.all_empty()) {
            // Explore the frontier
            top_down_step_with_remote_writes();
            // Slide all queues to explore the next frontier
            queue_.slide_all_windows();
        }
    }

///**
// * Run BFS using a hybrid algorithm
// *  1. Do top-down steps with migrating threads until condition is met
// *  2. Do top-down steps with remote writes until condition is met
// *  3. Do top-down steps with migrating threads until done
// */
//    void
//    hybrid_bfs_run_with_remote_writes_hybrid(long source, long alpha, long beta)
//    {
//        assert(source < g_->num_vertices());
//
//        // Start with the source vertex in the first frontier, at level 0, and mark it as visited
//        queue_.get_nth(0).push_back(source);
//        queue_.slide_all_windows();
//        parent_[source] = source;
//
//        long edges_to_check = g_->num_edges() * 2;
//        scout_count_ = g_->out_degree(source);
//
//        // While there are vertices in the queue...
//        while (!queue_.all_empty()) {
//
//            if (scout_count_ > edges_to_check / alpha) {
//                long awake_count, old_awake_count;
//                awake_count = queue_.combined_size();
//                // Do remote-write steps for a while
//                do {
//                    old_awake_count = awake_count;
//                    top_down_step_with_remote_writes();
//                    queue_.slide_all_windows();
//                    awake_count = queue_.combined_size();
//                } while (awake_count >= old_awake_count ||
//                         (awake_count > g_->num_vertices() / beta));
//                scout_count_ = 1;
//            } else {
//                edges_to_check -= scout_count_;
//                top_down_step_with_migrating_threads();
//                // Slide all queues to explore the next frontier
//                queue_.slide_all_windows();
//            }
//        }
//    }

/**
 * Run breadth-first search on the graph
 * @param alg Which BFS implementation to use
 * @param source Source vertex that forms the root of the BFS tree
 * @param alpha Hybrid BFS parameter, adjusts when to switch from top-down to bottom-up (default 15)
 * @param beta Hybrid BFS parameter, adjusts when to switch from bottom-up to top down (default 18)
 */
    void
    run(alg alg, long source, long alpha, long beta)
    {
        if (alg == alg::REMOTE_WRITES) {
            run_with_remote_writes(source);
//        } else if (alg == alg::MIGRATING_THREADS) {
//            hybrid_bfs_run_with_migrating_threads(source);
//        } else if (alg == alg::REMOTE_WRITES_HYBRID) {
//            hybrid_bfs_run_with_remote_writes_hybrid(source, alpha, beta);
//        } else if (alg == alg::BEAMER_HYBRID) {
//            hybrid_bfs_run_beamer(source, alpha, beta);
        } else {
            assert(0);
        }
    }

    bool
    check(long source)
    {
        // Local array to store the depth of each vertex in the tree
        std::vector<long> depth(g_->num_vertices(), -1);

        // Do a serial BFS
        std::queue<long> q;

        // Push source into the queue
        q.push(source);
        depth[source] = 0;
        // For each vertex in the queue...
        while (!q.empty()) {
            long u = q.front(); q.pop();
            // For each out-neighbor of this vertex...
            long * edges_begin = g_->out_neighbors(u);
            long * edges_end = edges_begin + g_->out_degree(u);
            for (long * e = edges_begin; e < edges_end; ++e) {
                long v = *e;
                // Add unexplored neighbors to the queue
                if (depth[v] == -1) {
                    depth[v] = depth[u] + 1;
                    q.push(v);
                }
            }
        }

//         // Dump the tree to stdout
//         // For each level in the tree...
//         bool is_level_empty = false;
//         for (long current_depth = 0; !is_level_empty; ++current_depth) {
//             is_level_empty = true;
//             printf("Level %li:", current_depth);
//             for (long v = 0; v < g_->num_vertices(); ++v) {
//                 // Print every vertex at this depth in the tree
//                 if (depth[v] == current_depth) {
//                     printf(" %li", v);
//                     // Keep going as long as we find a vertex at each level
//                     is_level_empty = false;
//                 }
//             }
//             printf("\n");
//         }
//         fflush(stdout);
//
//         print_tree();

        // Check each vertex
        // We are comparing the parent array produced by the parallel BFS
        // with the depth array produced by the serial BFS
        bool correct = true;
        for (long u = 0; u < g_->num_vertices(); ++u) {
            // Is the vertex a part of both BFS trees?
            if (depth[u] >= 0 && parent_[u] >= 0) {

                // Special case for source vertex
                if (u == source) {
                    if (!((parent_[u] == u) && (depth[u] == 0))) {
                        LOG("Source wrong\n");
                        correct = false;
                        break;
                    }
                    continue;
                }

                // Verify that this vertex is connected to its parent
                bool parent_found = false;
                // For all in-edges...
                long * edges_begin = g_->out_neighbors(u);
                long * edges_end = edges_begin + g_->out_degree(u);
                for (long * iter = edges_begin; iter < edges_end; ++iter) {
                    long v = *iter;
                    // If v is the parent of u, their depths should differ by 1
                    if (v == parent_[u]) {
                        if (depth[v] != depth[u] - 1) {
                            LOG("Wrong depths for %li and %li\n", u, v);
                            break;
                        }
                        parent_found = true;
                        break;
                    }
                }
                if (!parent_found) {
                    LOG("Couldn't find edge from %li to %li\n", parent_[u], u);
                    correct = false;
                    break;
                }
                // Do both trees agree about whether this vertex is in the tree?
            } else if ((depth[u] < 0) != (parent_[u] < 0)) {
                LOG("Reachability mismatch: depth[%li] = %li, parent[%li] = %li\n",
                    u, depth[u], u, parent_[u]);
                correct = false;
                break;
            }
        }

        return correct;
    }

    void
    print_tree()
    {
        for (long v = 0; v < g_->num_vertices(); ++v) {
            long parent = parent_[v];
            if (parent < 0) { continue; }

            printf("%4li", v);
            // Climb the tree back to the root
            while(true) {
                LOG(" <- %4li", parent);
                if (parent == -1) { break; }
                if (parent == parent_[parent]) { break; }
                parent = parent_[parent];
            }
            printf("\n");
        }
        fflush(stdout);
    }

    long
    count_num_traversed_edges()
    {
        auto sum = emu::make_repl<long>(0);
        g_->forall_vertices(256, [] (long v, graph& g, hybrid_bfs& bfs, long& local_sum) {
            if (bfs.parent_[v] >= 0) {
                local_sum += g.out_degree(v);
            }
        }, *g_, *this, *sum);
        // FIXME reduce sum;
        // Divide by two, since each undirected edge is counted twice
        return *sum / 2;
    }
};



