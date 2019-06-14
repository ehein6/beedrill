#include "hybrid_bfs.h"
#include <stdlib.h>
#include <assert.h>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <stdio.h>
#include "ack_control.h"
#include "cursor.h"

// Global replicated struct with BFS data pointers
replicated hybrid_bfs_data HYBRID_BFS;

// HACK define inlinable versions of grain size functions
// Will need to manually fix these for 3GC build
static inline long
MY_LOCAL_GRAIN(long n)
{
    long local_num_threads = 64 * 1;
    return n > local_num_threads ? (n/local_num_threads) : 1;
}
static inline long
MY_LOCAL_GRAIN_MIN(long n, long min_grain)
{
    long grain = MY_LOCAL_GRAIN(n);
    return grain > min_grain ? grain : min_grain;
}

void
queue_to_bitmap(sliding_queue * q, bitmap * b)
{
    // TODO parallelize with emu_local_for
    for (long i = q->start; i < q->end; ++i) {
        bitmap_set_bit(b, q->buffer[i]);
    }
}

void
queue_to_replicated_bitmap(sliding_queue * q, bitmap * b)
{
    // Transfer entries from the queue to the bitmap on each nodelet
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        sliding_queue * remote_q = mw_get_nth(q, nlet);
        bitmap * remote_b = mw_get_nth(b, nlet);
        cilk_spawn_at(remote_q) queue_to_bitmap(remote_q, remote_b);
    }
    // Syncronize all bitmaps
    cilk_sync;
    bitmap_replicated_sync(b);
}

void
bitmap_to_queue_worker(long * array, long begin, long end, va_list args)
{
    bitmap * b = va_arg(args, bitmap*);
    sliding_queue * q = va_arg(args, sliding_queue *);
    for (long v = begin; v < end; v += NODELETS()){
        if (bitmap_get_bit(b, v)) {
            sliding_queue_push_back(q, v);
        }
    }
}

void
replicated_bitmap_to_queue(bitmap * b, sliding_queue * q)
{
    // TODO parallelize with emu_local_for
    emu_1d_array_apply(G.vertex_out_degree, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 64),
        bitmap_to_queue_worker, b, q
    );
}

static void
init_parent_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    for (long v = begin; v < end; v += NODELETS()) {
        long out_degree = G.vertex_out_degree[v];
        HYBRID_BFS.parent[v] = out_degree != 0 ? -out_degree : -1;
        HYBRID_BFS.new_parent[v] = -1;
    }
}

void
hybrid_bfs_data_clear()
{
    emu_1d_array_apply(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 128),
        init_parent_worker
    );
    sliding_queue_replicated_reset(&HYBRID_BFS.queue);
    bitmap_replicated_clear(&HYBRID_BFS.frontier);
    bitmap_replicated_clear(&HYBRID_BFS.next_frontier);
}

static void
scout_count_allreduce()
{
    // Add up all replicated copies
    long sum = 0;
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        long local_scout_count = *(long*)mw_get_nth(&HYBRID_BFS.scout_count, nlet);
        sum += local_scout_count;
    }
    // Set all copies to the sum
    mw_replicated_init(&HYBRID_BFS.scout_count, sum);
}

void
hybrid_bfs_init()
{
    init_striped_array(&HYBRID_BFS.parent, G.num_vertices);
    init_striped_array(&HYBRID_BFS.new_parent, G.num_vertices);
    sliding_queue_replicated_init(&HYBRID_BFS.queue, G.num_vertices);
    bitmap_replicated_init(&HYBRID_BFS.frontier, G.num_vertices);
    bitmap_replicated_init(&HYBRID_BFS.next_frontier, G.num_vertices);

    hybrid_bfs_data_clear();
    ack_control_init();
}

void
hybrid_bfs_deinit()
{
    mw_free(HYBRID_BFS.parent);
    mw_free(HYBRID_BFS.new_parent);
    sliding_queue_replicated_deinit(&HYBRID_BFS.queue);
    bitmap_replicated_deinit(&HYBRID_BFS.frontier);
    bitmap_replicated_deinit(&HYBRID_BFS.next_frontier);
}


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

static inline void
mark_neighbors(long src, long * edges_begin, long * edges_end)
{
    for (long * e = edges_begin; e < edges_end; ++e) {
        long dst = *e;
        HYBRID_BFS.new_parent[dst] = src; // Remote write
    }
}

static inline void
mark_neighbors_parallel(long src, long * edges_begin, long * edges_end)
{
    long degree = edges_end - edges_begin;
    long grain = 512;
    if (degree <= grain) {
        // Low-degree local vertex, handle in this thread
        mark_neighbors(src, edges_begin, edges_end);
    } else {
        // High-degree local vertex, spawn local threads
        for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
            long * e2 = e1 + grain;
            if (e2 > edges_end) { e2 = edges_end; }
            cilk_spawn mark_neighbors(src, e1, e2);
        }
    }
}

void
mark_neighbors_in_eb(long src, edge_block * eb)
{
    mark_neighbors_parallel(src, eb->edges, eb->edges + eb->num_edges);
}

void
mark_queue_neighbors_worker(sliding_queue * queue, long * queue_pos)
{
    // Keep grabbing vertices off the local queue
    const long queue_end = queue->end;
    const long * queue_buffer = queue->buffer;
    long v = ATOMIC_ADDMS(queue_pos, 1);
    for (; v < queue_end; v = ATOMIC_ADDMS(queue_pos, 1)) {
        long src = queue_buffer[v];
        // How big is this vertex?
        if (is_heavy_out(src)) {
            // Heavy vertex, spawn a thread for each remote edge block
            edge_block * eb = G.vertex_out_neighbors[src].repl_edge_block;
            for (long i = 0; i < NODELETS(); ++i) {
                edge_block * remote_eb = mw_get_nth(eb, i);
                cilk_spawn_at(remote_eb) mark_neighbors_in_eb(src, remote_eb);
            }
        } else {
            long * edges_begin = G.vertex_out_neighbors[src].local_edges;
            long * edges_end = edges_begin + G.vertex_out_degree[src];
            mark_neighbors_parallel(src, edges_begin, edges_end);
        }
    }
}

void
mark_queue_neighbors_spawner(sliding_queue * queue)
{
    ack_control_disable_acks();
    // Decide how many workers to create
    long num_workers = 64;
    long queue_size = sliding_queue_size(queue);
    if (queue_size < num_workers) {
        num_workers = queue_size;
    }
    // Spawn workers
    long queue_pos = queue->start;
    for (long t = 0; t < num_workers; ++t) {
        cilk_spawn mark_queue_neighbors_worker(queue, &queue_pos);
    }
    cilk_sync;
    ack_control_reenable_acks();
}

    // For each vertex in the graph, detect if it was assigned a parent in this iteration
    static void
    populate_next_frontier(long * array, long begin, long end, va_list args)
    {
        long local_scout_count = 0;
        for (long i = begin; i < end; i += NODELETS()) {
            if (HYBRID_BFS.parent[i] < 0 && HYBRID_BFS.new_parent[i] >= 0) {
                // Update count with degree of new vertex
                local_scout_count += -HYBRID_BFS.parent[i];
                // Set parent
                HYBRID_BFS.parent[i] = HYBRID_BFS.new_parent[i];
                // Add to the queue for the next frontier
                sliding_queue_push_back(&HYBRID_BFS.queue, i);
            }
        }
        // Update global count
        REMOTE_ADD(&HYBRID_BFS.scout_count, local_scout_count);
    }

void
top_down_step_with_remote_writes()
{
    // Spawn a thread on each nodelet to process the local queue
    // For each neighbor, write your vertex ID to new_parent
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        cilk_spawn_at(local_queue) mark_queue_neighbors_spawner(local_queue);
    }
    cilk_sync;
    // Add to the queue all vertices that didn't have a parent before
    mw_replicated_init(&HYBRID_BFS.scout_count, 0);
    emu_1d_array_apply(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 128),
        populate_next_frontier
    );
    scout_count_allreduce();
}

/**
 * Top-down BFS step ("migrating threads" variant)
 * For each edge in the frontier, migrate to the dst vertex's nodelet
 * If the dst vertex doesn't have a parent, set src as parent
 * Then append to local queue for next frontier
 * Return the sum of the degrees of the vertices in the new frontier
 *
 * Overview of top_down_step_with_migrating_threads()
 *   spawn explore_local_frontier() on each nodelet
 *     spawn explore_frontier_spawner() over a slice of the local queue
 *       IF LIGHT VERTEX
 *       call explore_frontier_parallel() on a local array of edges
 *         call/spawn frontier_visitor over a local array of edges
 *       ELSE IF HEAVY VERTEX
 *       spawns explore_frontier_in_eb() for each remote edge block
 *         call explore_frontier_parallel() on the local edge block
 *           call/spawn frontier_visitor over the local edge block
*/

static inline void
visit(long src, long dst)
{
    // Look up the parent of the vertex we are visiting
    long * parent = &HYBRID_BFS.parent[dst];
    long curr_val = *parent;
    // If we are the first to visit this vertex
    if (curr_val < 0) {
        // Set self as parent of this vertex
        if (ATOMIC_CAS(parent, src, curr_val) == curr_val) {
            // Add it to the queue
            sliding_queue_push_back(&HYBRID_BFS.queue, dst);
            REMOTE_ADD(&HYBRID_BFS.scout_count, -curr_val);
        }
    }
}

// Using noinline to minimize the size of the migrating context
static __attribute__((always_inline)) inline void
frontier_visitor(long src, long * edges_begin, long * edges_end)
{
    long e1, e2, e3, e4;

    // Visit neighbors one at a time until remainder is evenly divisible by four
    while ((edges_end - edges_begin) % 4 != 0) {
        visit(src, *edges_begin++);
    }

    for (long * e = edges_begin; e < edges_end;) {
        // Pick up four edges
        e4 = *e++;
        e3 = *e++;
        e2 = *e++;
        e1 = *e++;
        // Visit each neighbor without returning home
        // Once an edge has been traversed, we can resize to
        // avoid carrying it along with us.
        visit(src, e1); RESIZE();
        visit(src, e2); RESIZE();
        visit(src, e3); RESIZE();
        visit(src, e4); RESIZE();
    }
}

static inline void
explore_frontier_parallel(long src, long * edges_begin, long * edges_end)
{
    long degree = edges_end - edges_begin;
    long grain = 64;
    if (degree <= grain) {
        // Low-degree local vertex, handle in this thread
        // TODO spawn here to separate from parent thread?
        frontier_visitor(src, edges_begin, edges_end);
    } else {
        // High-degree local vertex, spawn local threads
        for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
            long * e2 = e1 + grain;
            if (e2 > edges_end) { e2 = edges_end; }
            cilk_spawn frontier_visitor(src, e1, e2);
        }
    }
}

void
explore_frontier_worker(sliding_queue * queue, long * queue_pos)
{
    const long queue_end = queue->end;
    const long * queue_buffer = queue->buffer;
    long v = ATOMIC_ADDMS(queue_pos, 1);
    for (; v < queue_end; v = ATOMIC_ADDMS(queue_pos, 1)) {
        long src = queue_buffer[v];
        long * edges_begin = G.vertex_out_neighbors[src].local_edges;
        long * edges_end = edges_begin + G.vertex_out_degree[src];
        explore_frontier_parallel(src, edges_begin, edges_end);
    }
}

void
explore_local_frontier(sliding_queue * queue)
{
    // Decide how many workers to create
    long num_workers = 64;
    long queue_size = sliding_queue_size(queue);
    if (queue_size < num_workers) {
        num_workers = queue_size;
    }
    // Spawn workers
    long queue_pos = queue->start;
    for (long t = 0; t < num_workers; ++t) {
        cilk_spawn explore_frontier_worker(queue, &queue_pos);
    }
}

void
top_down_step_with_migrating_threads()
{
    // Spawn a thread on each nodelet to process the local queue
    // For each neighbor without a parent, add self as parent and append to queue
    mw_replicated_init(&HYBRID_BFS.scout_count, 0);
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        cilk_spawn_at(local_queue) explore_local_frontier(local_queue);
    }
    cilk_sync;
    scout_count_allreduce();
}

void
dump_queue_stats()
{
    printf("Queue contents: ");
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        for (long i = local_queue->start; i < local_queue->end; ++i) {
            printf("%li ", local_queue->buffer[i]);
        }
    }
    printf("\n");
    fflush(stdout);

    printf("Bitmap contents: ");
    for (long n = 0; n < NODELETS(); ++n) {
        bitmap * local_bitmap = mw_get_nth(&HYBRID_BFS.frontier, n);
        bitmap_dump(local_bitmap);
    }
    printf("\n");
    fflush(stdout);

    printf("Frontier size per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        printf("%li ", local_queue->end - local_queue->start);
    }
    printf("\n");
    fflush(stdout);

    printf("Total out-degree per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        long degree_sum = 0;
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        for (long i = local_queue->start; i < local_queue->end; ++i) {
            long v = local_queue->buffer[i];
            degree_sum += G.vertex_out_degree[v];
        }
        printf("%li ", degree_sum);
    }

    printf("\n");
    fflush(stdout);
}


/**
 * Bottom-up BFS step
 * For each vertex that is not yet a part of the BFS tree,
 * check all in-neighbors to see if they are in the current frontier
 * using a replicated bitmap.
 * If a parent is found, put the child in the bitmap for the next frontier
 * Returns the number of vertices that found a parent (size of next frontier)
 *
 * Overview of bottom_up_step()
 *   spawn search_for_parent_worker() over the entire vertex list
 *     IF LIGHT VERTEX
 *     call search_for_parent_parallel() on a local array of edges
 *       call/spawn search_for_parent over a local array of edges
 *     ELSE IF HEAVY VERTEX
 *     spawn search_for_parent_in_remote_ebs()
 *       spawn search_for_parent_in_eb() at each remote edge block
 *         call search_for_parent_parallel() on the local edge block
 *           call/spawn search_for_parent() over the local edge block
 *
*/

static void
search_for_parent(long child, long * edges_begin, long * edges_end, long * awake_count)
{
    // For each vertex connected to me...
    for (long * e = edges_begin; e < edges_end; ++e) {
        long parent = *e;
        // If the vertex is in the frontier...
        if (bitmap_get_bit(&HYBRID_BFS.frontier, parent)) {
            // Claim as a parent
            HYBRID_BFS.parent[child] = parent;
            // Increment number of vertices woken up on this step
            REMOTE_ADD(awake_count, 1);
            // Put myself in the frontier
            bitmap_set_bit(&HYBRID_BFS.next_frontier, child);
            // No need to keep looking for a parent
            break;
        }
    }
}

static inline void
search_for_parent_parallel(long child, long * edges_begin, long * edges_end, long * awake_count)
{
    long degree = edges_end - edges_begin;
    long grain = MY_LOCAL_GRAIN_MIN(degree, 128);
    if (degree <= grain) {
        // Low-degree local vertex, handle in this thread
        search_for_parent(child, edges_begin, edges_end, awake_count);
    } else {
        // High-degree local vertex, spawn local threads
        long num_found = 0;
        for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
            long * e2 = e1 + grain;
            if (e2 > edges_end) { e2 = edges_end; }
            cilk_spawn search_for_parent(child, e1, e2, &num_found);
        }
        cilk_sync;
        // If multiple parents were found, we only increment the counter once
        if (num_found > 0) {
            REMOTE_ADD(awake_count, 1);
        }
    }
}

// Calls search_for_parent_parallel over a remote edge block
void
search_for_parent_in_eb(long child, edge_block * eb, long * awake_count)
{
    search_for_parent_parallel(child, eb->edges, eb->edges + eb->num_edges, awake_count);
}

void
search_for_parent_in_remote_ebs(long v, long * awake_count)
{
    // Heavy vertex, spawn a thread for each remote edge block
    long num_found = 0;
    edge_block * eb = G.vertex_out_neighbors[v].repl_edge_block;
    for (long i = 0; i < NODELETS(); ++i) {
        edge_block * remote_eb = mw_get_nth(eb, i);
        cilk_spawn_at(remote_eb) search_for_parent_in_eb(v, remote_eb, &num_found);
    }
    cilk_sync;
    // If multiple parents were found, we only increment the counter once
    if (num_found > 0) {
        REMOTE_ADD(awake_count, 1);
    }
}

// Spawns threads to call search_for_parent in parallel for a part of the vertex list
void
search_for_parent_worker(long * array, long begin, long end, va_list args)
{
    long * awake_count = va_arg(args, long*);
    // For each vertex in our slice of the queue...
    long local_awake_count = 0;
    for (long v = begin; v < end; v += NODELETS()) {
        if (HYBRID_BFS.parent[v] < 0) {
            // How big is this vertex?
            if (is_heavy_out(v)) {
                // Heavy vertex, spawn a thread for each remote edge block
                cilk_spawn search_for_parent_in_remote_ebs(v, &local_awake_count);
            } else {
                long * edges_begin = G.vertex_out_neighbors[v].local_edges;
                long * edges_end = edges_begin + G.vertex_out_degree[v];
                search_for_parent_parallel(v, edges_begin, edges_end, &local_awake_count);
            }
        }
    }
    cilk_sync;
    // Update global count
    REMOTE_ADD(awake_count, local_awake_count);
}

static long
bottom_up_step()
{
    long awake_count = 0;
    // TODO we can do the clear in parallel with other stuff
    bitmap_clear(&HYBRID_BFS.next_frontier);
    emu_1d_array_apply(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 64),
        search_for_parent_worker, &awake_count
    );
    return awake_count;
}

/**
 * Run BFS using Scott Beamer's direction-optimizing algorithm
 *  1. Do top-down steps with migrating threads until condition is met
 *  2. Do bottom-up steps until condition is met
 *  3. Do top-down steps with migrating threads until done
 */
void
hybrid_bfs_run_beamer (long source, long alpha, long beta)
{
    assert(source < G.num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&HYBRID_BFS.queue, 0), source);
    sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    HYBRID_BFS.parent[source] = source;

    long edges_to_check = G.num_edges * 2;
    mw_replicated_init(&HYBRID_BFS.scout_count, G.vertex_out_degree[source]);

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&HYBRID_BFS.queue)) {
        if (HYBRID_BFS.scout_count > edges_to_check / alpha) {
            long awake_count, old_awake_count;
            // Convert sliding queue to bitmap
            // hooks_region_begin("queue_to_bitmap");
            queue_to_replicated_bitmap(&HYBRID_BFS.queue, &HYBRID_BFS.frontier);
            // hooks_region_end();
            awake_count = sliding_queue_combined_size(&HYBRID_BFS.queue);
            sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
            // Do bottom-up steps for a while
            do {
                old_awake_count = awake_count;
                // hooks_set_attr_i64("awake_count", awake_count);
                // hooks_region_begin("bottom_up_step");
                awake_count = bottom_up_step();
                bitmap_replicated_swap(&HYBRID_BFS.frontier, &HYBRID_BFS.next_frontier);
                // hooks_region_end();
            } while (awake_count >= old_awake_count ||
                    (awake_count > G.num_vertices / beta));
            // Convert back to a queue
            // hooks_region_begin("bitmap_to_queue");
            replicated_bitmap_to_queue(&HYBRID_BFS.frontier, &HYBRID_BFS.queue);
            sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
            // hooks_region_end();
            mw_replicated_init(&HYBRID_BFS.scout_count, 1);
        } else {
            edges_to_check -= HYBRID_BFS.scout_count;
            // Do a top-down step
            // hooks_region_begin("top_down_step");
            top_down_step_with_migrating_threads();
            // Slide all queues to explore the next frontier
            sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
            // hooks_region_end();
        }
        // dump_queue_stats();
    }
}

/**
 * Run BFS using top-down steps with migrating threads
 */
void
hybrid_bfs_run_with_migrating_threads(long source)
{
    assert(source < G.num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&HYBRID_BFS.queue, 0), source);
    sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    HYBRID_BFS.parent[source] = source;

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&HYBRID_BFS.queue)) {
        // Explore the frontier
        top_down_step_with_migrating_threads();
        // Slide all queues to explore the next frontier
        sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    }
}

/**
 * Run BFS using top-down steps with remote writes
 */
void
hybrid_bfs_run_with_remote_writes(long source)
{
    assert(source < G.num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&HYBRID_BFS.queue, 0), source);
    sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    HYBRID_BFS.parent[source] = source;

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&HYBRID_BFS.queue)) {
        // Explore the frontier
        top_down_step_with_remote_writes();
        // Slide all queues to explore the next frontier
        sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    }
}

/**
 * Run BFS using a hybrid algorithm
 *  1. Do top-down steps with migrating threads until condition is met
 *  2. Do top-down steps with remote writes until condition is met
 *  3. Do top-down steps with migrating threads until done
 */
void
hybrid_bfs_run_with_remote_writes_hybrid(long source, long alpha, long beta)
{
    assert(source < G.num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&HYBRID_BFS.queue, 0), source);
    sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    HYBRID_BFS.parent[source] = source;

    long edges_to_check = G.num_edges * 2;
    mw_replicated_init(&HYBRID_BFS.scout_count, G.vertex_out_degree[source]);

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&HYBRID_BFS.queue)) {

        if (HYBRID_BFS.scout_count > edges_to_check / alpha) {
            long awake_count, old_awake_count;
            awake_count = sliding_queue_combined_size(&HYBRID_BFS.queue);
            // Do remote-write steps for a while
            do {
                old_awake_count = awake_count;
                top_down_step_with_remote_writes();
                sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
                awake_count = sliding_queue_combined_size(&HYBRID_BFS.queue);
            } while (awake_count >= old_awake_count ||
                     (awake_count > G.num_vertices / beta));
            mw_replicated_init(&HYBRID_BFS.scout_count, 1);
        } else {
            edges_to_check -= HYBRID_BFS.scout_count;
            top_down_step_with_migrating_threads();
            // Slide all queues to explore the next frontier
            sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
        }
    }
}

/**
 * Run breadth-first search on the graph
 * @param alg Which BFS implementation to use
 * @param source Source vertex that forms the root of the BFS tree
 * @param alpha Hybrid BFS parameter, adjusts when to switch from top-down to bottom-up (default 15)
 * @param beta Hybrid BFS parameter, adjusts when to switch from bottom-up to top down (default 18)
 */
void
hybrid_bfs_run(hybrid_bfs_alg alg, long source, long alpha, long beta)
{
    if (alg == REMOTE_WRITES) {
        hybrid_bfs_run_with_remote_writes(source);
    } else if (alg == MIGRATING_THREADS) {
        hybrid_bfs_run_with_migrating_threads(source);
    } else if (alg == REMOTE_WRITES_HYBRID) {
        hybrid_bfs_run_with_remote_writes_hybrid(source, alpha, beta);
    } else if (alg == BEAMER_HYBRID) {
        hybrid_bfs_run_beamer(source, alpha, beta);
    } else {
        assert(0);
    }
}

bool
hybrid_bfs_check(long source)
{
    // Local array to store the depth of each vertex in the tree
    long * depth = mw_localmalloc(G.num_vertices * sizeof(long), &depth);
    assert(depth);
    for (long i = 0; i < G.num_vertices; ++i) { depth[i] = -1; }

    // Do a serial BFS
    cursor c;
    sliding_queue q;
    sliding_queue_init(&q, G.num_vertices);
    // Push source into the queue
    sliding_queue_push_back(&q, source);
    sliding_queue_slide_window(&q);
    depth[source] = 0;
    // For each vertex in the queue...
    for (long i = q.start; i < q.end; ++i) {
        long u = q.buffer[i];
        // For each out-neighbor of this vertex...
        for (cursor_init_out(&c, u); cursor_valid(&c); cursor_next(&c)) {
            long v = *c.e;
            // Add unexplored neighbors to the queue
            if (depth[v] == -1) {
                depth[v] = depth[u] + 1;
                sliding_queue_push_back(&q, v);
            }
        }
        sliding_queue_slide_window(&q);
    }

    // // Dump the tree to stdout
    // // For each level in the tree...
    // bool is_level_empty = false;
    // for (long current_depth = 0; !is_level_empty; ++current_depth) {
    //     is_level_empty = true;
    //     printf("Level %li:", current_depth);
    //     for (long v = 0; v < G.num_vertices; ++v) {
    //         // Print every vertex at this depth in the tree
    //         if (depth[v] == current_depth) {
    //             printf(" %li", v);
    //             // Keep going as long as we find a vertex at each level
    //             is_level_empty = false;
    //         }
    //     }
    //     printf("\n");
    // }
    // fflush(stdout);

    // Check each vertex
    // We are comparing the parent array produced by the parallel BFS
    // with the depth array produced by the serial BFS
    bool correct = true;
    long * parent = HYBRID_BFS.parent;
    for (long u = 0; u < G.num_vertices; ++u) {
        // Is the vertex a part of both BFS trees?
        if (depth[u] >= 0 && parent[u] >= 0) {

            // Special case for source vertex
            if (u == source) {
                if (!((parent[u] == u) && (depth[u] == 0))) {
                    LOG("Source wrong\n");
                    correct = false;
                    break;
                }
                continue;
            }

            // Verify that this vertex is connected to its parent
            bool parent_found = false;
            // For all in-edges...
            for (cursor_init_out(&c, u); cursor_valid(&c); cursor_next(&c)) {
                long v = *c.e;
                // If v is the parent of u, their depths should differ by 1
                if (v == parent[u]) {
                    if (depth[v] != depth[u] - 1) {
                        LOG("Wrong depths for %li and %li\n", u, v);
                        break;
                    }
                    parent_found = true;
                    break;
                }
            }
            if (!parent_found) {
                LOG("Couldn't find edge from %li to %li\n", parent[u], u);
                correct = false;
                break;
            }
        // Do both trees agree about whether this vertex is in the tree?
        } else if ((depth[u] < 0) != (parent[u] < 0)) {
            LOG("Reachability mismatch: depth[%li] = %li, parent[%li] = %li\n",
                u, depth[u], u, parent[u]);
            correct = false;
            break;
        }
    }

    sliding_queue_deinit(&q);
    mw_free(depth);
    return correct;
}

void
hybrid_bfs_print_tree()
{
    for (long v = 0; v < G.num_vertices; ++v) {
        long parent = HYBRID_BFS.parent[v];
        if (parent < 0) { continue; }

        printf("%4li", v);
        // Climb the tree back to the root
        while(true) {
            LOG(" <- %4li", parent);
            if (parent == -1) { break; }
            if (parent == HYBRID_BFS.parent[parent]) { break; }
            parent = HYBRID_BFS.parent[parent];
        }
        printf("\n");
    }
    fflush(stdout);
}

static void
compute_num_traversed_edges_worker(long * array, long begin, long end, long * partial_sum, va_list args)
{
    long local_sum = 0;
    const long nodelets = NODELETS();
    for (long v = begin; v < end; v += nodelets) {
        if (HYBRID_BFS.parent[v] >= 0) {
            local_sum += G.vertex_out_degree[v];
        }
    }
    REMOTE_ADD(partial_sum, local_sum);
}

long
hybrid_bfs_count_num_traversed_edges()
{
    return emu_1d_array_reduce_sum(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
        compute_num_traversed_edges_worker
    ) / 2;
    // Divide by two, since each undirected edge is counted twice
}
