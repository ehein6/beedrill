#include "hybrid_bfs.h"
#include "ack_control.h"

#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/striped_for_each.h>

hybrid_bfs::hybrid_bfs(graph & g)
: g_(&g)
, parent_(g.num_vertices())
, new_parent_(g.num_vertices())
, queue_(g.num_vertices())
, frontier_(g.num_vertices())
, next_frontier_(g.num_vertices())
, scout_count_(0)
{
    // Force ack controller singleton to initialize itself
    ack_control_init();
}

// Shallow copy constructor
hybrid_bfs::hybrid_bfs(const hybrid_bfs& other, emu::shallow_copy tag)
: g_(other.g_)
, parent_(other.parent_, tag)
, new_parent_(other.new_parent_, tag)
, queue_(other.queue_, tag)
, frontier_(other.frontier_, tag)
, next_frontier_(other.next_frontier_, tag)
, scout_count_(other.scout_count_)
{}

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
    emu::parallel::striped_for_each_i(
        emu::execution::parallel_limited_policy(64),
        parent_.data(), 0L, parent_.size(),
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
long
hybrid_bfs::top_down_step_with_remote_writes()
{
    ack_control_disable_acks();
    // For each vertex in the queue...
    queue_.forall_items([] (long v, graph& g, long * new_parent) {
        // for each neighbor of that vertex...
        g.forall_out_neighbors(min_grain(512), v, [](long src, long dst, long * new_parent) {
            // write the vertex ID to the neighbor's new_parent entry.
            new_parent[dst] = src; // Remote write
        }, new_parent);
    }, *g_, new_parent_.data());
    ack_control_reenable_acks();

    // Add to the queue all vertices that didn't have a parent before
    scout_count_ = 0;
    g_->forall_vertices(min_grain(128), [](long v, hybrid_bfs & bfs) {
        if (bfs.parent_[v] < 0 && bfs.new_parent_[v] >= 0) {
            // Update count with degree of new vertex
            REMOTE_ADD(&static_cast<long&>(bfs.scout_count_), -bfs.parent_[v]);
            // Set parent
            bfs.parent_[v] = bfs.new_parent_[v];
            // Add to the queue for the next frontier
            bfs.queue_.push_back(v);
        }
    }, *this);
    // Combine per-nodelet values of scout_count
    return emu::repl_reduce(scout_count_, std::plus<>());
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


long
hybrid_bfs::top_down_step_with_migrating_threads()
{
    // Spawn a thread on each nodelet to process the local queue
    // For each neighbor without a parent, add self as parent and append to queue
    scout_count_ = 0;
    queue_.forall_items( [] (long v, hybrid_bfs& bfs) {
        // for each neighbor of that vertex...
        bfs.g_->forall_out_neighbors(min_grain(512), v, [](long src, long dst, hybrid_bfs& bfs) {
            // Look up the parent of the vertex we are visiting
            long * parent = &bfs.parent_[dst];
            long curr_val = *parent;
            // If we are the first to visit this vertex
            if (curr_val < 0) {
                // Set self as parent of this vertex
                if (ATOMIC_CAS(parent, src, curr_val) == curr_val) {
                    // Add it to the queue
                    bfs.queue_.push_back(dst);
                    REMOTE_ADD(&static_cast<long&>(bfs.scout_count_), -curr_val);
                }
            }
        }, bfs);
    }, *this);
    // Combine per-nodelet values of scout_count
    return emu::repl_reduce(scout_count_, std::plus<>());
}


/**
 * Bottom-up BFS step
 * For each vertex that is not yet a part of the BFS tree,
 * check all in-neighbors to see if they are in the current frontier
 * using a replicated bitmap.
 * If a parent is found, put the child in the bitmap for the next frontier
 * Returns the number of vertices that found a parent (size of next frontier)
*/
long
hybrid_bfs::bottom_up_step()
{
    // TODO we can do the clear in parallel with other stuff
    next_frontier_.clear();
    awake_count_ = 0;

    // For all vertices without a parent...
    g_->forall_vertices(min_grain(512), [] (long v, hybrid_bfs& bfs, long& awake_count) {
        if (bfs.parent_[v] >= 0) { return; }
        // Look for neighbors who are in the frontier
        long num_parents = 0;
        bfs.g_->forall_out_neighbors(min_grain(512), v, [](long child, long parent, hybrid_bfs &bfs, long& num_parents) {
            // If the neighbor is in the frontier...
            if (bfs.frontier_.get_bit(parent)) {
                // Claim as a parent
                bfs.parent_[child] = parent;
                // Increment number of parents found
                REMOTE_ADD(&num_parents, 1);
                // Put myself in the frontier
                bfs.next_frontier_.set_bit(child);
                // TODO No need to keep looking for a parent, quit here
            }
        }, bfs, num_parents);
        // Track number of vertices woken up in this step
        // If we run in parallel, we may find several parents, but
        // we still only increment the counter once per vertex added.
        if (num_parents > 0) { REMOTE_ADD(&awake_count, 1); }
    }, *this, awake_count_);

    return repl_reduce(awake_count_, std::plus<>());
}


/**
 * Run BFS using Scott Beamer's direction-optimizing algorithm
 *  1. Do top-down steps with migrating threads until condition is met
 *  2. Do bottom-up steps until condition is met
 *  3. Do top-down steps with migrating threads until done
 */
void
hybrid_bfs::run_beamer (long source, long alpha, long beta)
{
    assert(source < g_->num_vertices());

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    queue_.push_back(source);
    queue_.slide_all_windows();
    parent_[source] = source;

    long edges_to_check = g_->num_edges() * 2;
    long scout_count = g_->out_degree(source);

    // While there are vertices in the queue...
    while (!queue_.all_empty()) {
        if (scout_count > edges_to_check / alpha) {
            long awake_count, old_awake_count;
            // Convert sliding queue to bitmap
            // hooks_region_begin("queue_to_bitmap");
            queue_to_bitmap();
            // hooks_region_end();
            awake_count = queue_.combined_size();
            queue_.slide_all_windows();
            // Do bottom-up steps for a while
            do {
                old_awake_count = awake_count;
                // hooks_set_attr_i64("awake_count", awake_count);
                // hooks_region_begin("bottom_up_step");
                awake_count = bottom_up_step();
                emu::repl_swap(frontier_, next_frontier_);
                // hooks_region_end();
            } while (awake_count >= old_awake_count ||
                     (awake_count > g_->num_vertices() / beta));
            // Convert back to a queue
            // hooks_region_begin("bitmap_to_queue");
            bitmap_to_queue();
            queue_.slide_all_windows();
            // hooks_region_end();
            scout_count = 1;
        } else {
            edges_to_check -= scout_count;
            // Do a top-down step
            // hooks_region_begin("top_down_step");
            scout_count = top_down_step_with_migrating_threads();
            // Slide all queues to explore the next frontier
            queue_.slide_all_windows();
            // hooks_region_end();
        }
    }
}

/**
 * Run BFS using top-down steps with migrating threads
 */
void
hybrid_bfs::run_with_migrating_threads(long source)
{
    assert(source < g_->num_vertices());

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    queue_.get_nth(0).push_back(source);
    queue_.slide_all_windows();
    parent_[source] = source;

    // While there are vertices in the queue...
    while (!queue_.all_empty()) {
        // Explore the frontier
        top_down_step_with_migrating_threads();
        // Slide all queues to explore the next frontier
        queue_.slide_all_windows();
    }
}

/**
 * Run BFS using top-down steps with remote writes
 */
void
hybrid_bfs::run_with_remote_writes(long source)
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

/**
 * Run BFS using a hybrid algorithm
 *  1. Do top-down steps with migrating threads until condition is met
 *  2. Do top-down steps with remote writes until condition is met
 *  3. Do top-down steps with migrating threads until done
 */
void
hybrid_bfs::run_with_remote_writes_hybrid(long source, long alpha, long beta)
{
    assert(source < g_->num_vertices());

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    queue_.get_nth(0).push_back(source);
    queue_.slide_all_windows();
    parent_[source] = source;

    long edges_to_check = g_->num_edges() * 2;
    long scout_count = g_->out_degree(source);

    // While there are vertices in the queue...
    while (!queue_.all_empty()) {
        if (scout_count > edges_to_check / alpha) {
            long awake_count, old_awake_count;
            awake_count = queue_.combined_size();
            // Do remote-write steps for a while
            do {
                old_awake_count = awake_count;
                top_down_step_with_remote_writes();
                queue_.slide_all_windows();
                awake_count = queue_.combined_size();
            } while (awake_count >= old_awake_count ||
                     (awake_count > g_->num_vertices() / beta));
            scout_count = 1;
        } else {
            edges_to_check -= scout_count;
            scout_count = top_down_step_with_migrating_threads();
            // Slide all queues to explore the next frontier
            queue_.slide_all_windows();
        }
    }
}



bool
hybrid_bfs::check(long source)
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
hybrid_bfs::print_tree()
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
hybrid_bfs::count_num_traversed_edges()
{
    auto sum = emu::make_repl<long>(0);
    g_->forall_vertices(min_grain(256), [] (long v, graph& g, hybrid_bfs& bfs, long& local_sum) {
        if (bfs.parent_[v] >= 0) {
            local_sum += g.out_degree(v);
        }
    }, *g_, *this, *sum);

    // FIXME reduce sum;
    // Divide by two, since each undirected edge is counted twice
    return emu::repl_reduce(*sum, std::plus<long>()) / 2;
}

void
hybrid_bfs::dump_queue_stats()
{
    printf("Queue contents: ");
    queue_.dump_all();
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

void
hybrid_bfs::dump_bitmap_stats()
{
    printf("Frontier contents: ");
    frontier_.dump();
    printf("Next frontier contents: ");
    next_frontier_.dump();
}

void
hybrid_bfs::clear()
{
    // Initialize the parent array with the -degree of the vertex
    emu::parallel::striped_for_each_i(
        emu::execution::parallel_limited_policy(128),
        parent_.data(), 0L, parent_.size(),
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
