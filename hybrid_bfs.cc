#include "hybrid_bfs.h"
#include "ack_control.h"

#include <emu_cxx_utils/execution_policy.h>
#include <emu_cxx_utils/for_each.h>

using namespace emu;
using namespace emu::execution;

hybrid_bfs::hybrid_bfs(graph & g)
: g_(&g)
, parent_(g.num_vertices())
, new_parent_(g.num_vertices())
, queue_(g.num_vertices())
, scout_count_(0L)
, awake_count_(0L)
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
, scout_count_(other.scout_count_)
, awake_count_(other.awake_count_)
{}

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
    queue_.forall_items([&](long src) {
        // for each neighbor of that vertex...
        g_->for_each_out_edge(src, [&](long dst) {
            // write the vertex ID to the neighbor's new_parent entry.
            new_parent_[dst] = src; // Remote write
        });
    });
    ack_control_reenable_acks();

    // Add to the queue all vertices that didn't have a parent before
    scout_count_ = 0;
    g_->for_each_vertex([&](long v) {
        if (parent_[v] < 0 && new_parent_[v] >= 0) {
            // Update count with degree of new vertex
            REMOTE_ADD(&scout_count_, -parent_[v]);
            // Set parent
            parent_[v] = new_parent_[v];
            // Add to the queue for the next frontier
            queue_.push_back(v);
        }
    });
    // Combine per-nodelet values of scout_count
    return emu::repl_reduce(scout_count_, std::plus<>());
}

long
hybrid_bfs::top_down_step_with_migrating_threads()
{
    // Spawn a thread on each nodelet to process the local queue
    // For each neighbor without a parent, add self as parent and append to queue
    scout_count_ = 0;
    queue_.forall_items([this](long src) {
        // for each neighbor of that vertex...
        g_->for_each_out_edge_grouped(
            parallel_policy(64), src, [this, src](long dst) {
                // Look up the parent of the vertex we are visiting
                long * parent = &parent_[dst];
                long curr_val = *parent;
                // If we are the first to visit this vertex
                if (curr_val < 0) {
                    // Set self as parent of this vertex
                    if (ATOMIC_CAS(parent, src, curr_val) == curr_val) {
                        // Add it to the queue
                        queue_.push_back(dst);
                        REMOTE_ADD(&scout_count_, -curr_val);
                    }
                }
            }
        );
    });
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
    awake_count_ = 0;

    // For all vertices without a parent...
    g_->for_each_vertex([this](long child) {
        if (parent_[child] >= 0) { return; }
        // Look for neighbors who are in the frontier
        g_->find_out_edge_grouped(seq, child, [this, child](long parent) {
            // If the neighbor is in the frontier...
            if (parent_[parent] >= 0) {
                // Claim as a parent
                new_parent_[child] = parent;
                // No need to keep searching
                return true;
            } else return false;
        });
    });

    // Add to the queue all vertices that didn't have a parent before
    g_->for_each_vertex([this](long v) {
        if (parent_[v] < 0 && new_parent_[v] >= 0) {
            // Set parent
            parent_[v] = new_parent_[v];
            // Add to the queue for the next frontier
            queue_.push_back(v);
            // Track number of vertices woken up in this step
            REMOTE_ADD(&awake_count_, 1);
        }
    });
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
            awake_count = queue_.combined_size();
            // Do bottom-up steps for a while
            do {
                old_awake_count = awake_count;
                // hooks_set_attr_i64("awake_count", awake_count);
                // hooks_region_begin("bottom_up_step");
                awake_count = bottom_up_step();
                queue_.slide_all_windows();
                // hooks_region_end();
            } while (awake_count >= old_awake_count ||
                    (awake_count > g_->num_vertices() / beta));
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
    g_->for_each_vertex([&] (long v) {
        if (parent_[v] >= 0) {
            REMOTE_ADD(&sum->get(), g_->out_degree(v));
        }
    });

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
hybrid_bfs::clear()
{
    g_->for_each_vertex([&](long v) {
        long out_degree = g_->out_degree(v);
        parent_[v] = out_degree != 0 ? -out_degree : -1;
        new_parent_[v] = -1;
    });
    // Reset the queue
    queue_.reset_all();
}
