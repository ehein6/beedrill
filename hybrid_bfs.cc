#include "hybrid_bfs.h"


void
hybrid_bfs::queue_to_bitmap()
{
    // For each item in the queue, set the corresponding bit in the bitmap
    queue_.forall_items([](long i, bitmap& b) {
        b.set_bit(i);
    }, frontier_);
}

void
hybrid_bfs::bitmap_to_queue()
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


/**
 * Top-down BFS step ("migrating threads" variant)
 * For each edge in the frontier, migrate to the dst vertex's nodelet
 * If the dst vertex doesn't have a parent, set src as parent
 * Then append to local queue for next frontier
 * Return the sum of the degrees of the vertices in the new frontier
 */
void
hybrid_bfs::top_down_step_with_remote_writes()
{
    ack_controller::instance().disable_acks();
    // For each vertex in the queue...
    queue_.forall_items([] (long v, graph& g, long * new_parent) {
        // for each neighbor of that vertex...
        g.forall_out_neighbors(v, 512, [](long src, long dst, long * new_parent) {
            // write the vertex ID to the neighbor's new_parent entry.
            new_parent[dst] = src; // Remote write
        }, new_parent);
    }, *g_, new_parent_.data());
    ack_controller::instance().reenable_acks();

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






// Using noinline to minimize the size of the migrating context
//static __attribute__((always_inline)) inline void
//frontier_visitor(long src, long * edges_begin, long * edges_end)
//{
//    long e1, e2, e3, e4;
//
//    // Visit neighbors one at a time until remainder is evenly divisible by four
//    while ((edges_end - edges_begin) % 4 != 0) {
//        visit(src, *edges_begin++);
//    }
//
//    for (long * e = edges_begin; e < edges_end;) {
//        // Pick up four edges
//        e4 = *e++;
//        e3 = *e++;
//        e2 = *e++;
//        e1 = *e++;
//        // Visit each neighbor without returning home
//        // Once an edge has been traversed, we can resize to
//        // avoid carrying it along with us.
//        visit(src, e1); RESIZE();
//        visit(src, e2); RESIZE();
//        visit(src, e3); RESIZE();
//        visit(src, e4); RESIZE();
//    }
//}


void
hybrid_bfs::top_down_step_with_migrating_threads()
{
    // Spawn a thread on each nodelet to process the local queue
    // For each neighbor without a parent, add self as parent and append to queue
    mw_replicated_init(&scout_count_, 0);

    queue_.forall_items(
        [] (long v, hybrid_bfs& bfs) {
            // for each neighbor of that vertex...
            bfs.g_->forall_out_neighbors(v, 512,
                [](long src, long dst, hybrid_bfs& bfs) {
                    // Look up the parent of the vertex we are visiting
                    long * parent = &bfs.parent_[dst];
                    long curr_val = *parent;
                    // If we are the first to visit this vertex
                    if (curr_val < 0) {
                        // Set self as parent of this vertex
                        if (ATOMIC_CAS(parent, src, curr_val) == curr_val) {
                            // Add it to the queue
                            bfs.queue_.push_back(dst);
                            REMOTE_ADD(&bfs.scout_count_, -curr_val);
                        }
                    }
                }, bfs
            );
        }, *this
    );
    // FIXME Combine per-nodelet values of scout_count
    //scout_count_allreduce();
}


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