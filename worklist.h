#pragma once
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/intrinsics.h>
#include <emu_cxx_utils/execution_policy.h>
#include <cilk/cilk.h>

class worklist {
private:
    // The first vertex in the work list
    // If this is a replicated instance, there will be a different head on
    // each nodelet
    volatile long head_;

    // The following arrays have one element per vertex. Together they function
    // as a linked-list node using struct-of-arrays.

    // Next vertex ID to process (next pointer in linked list)
    emu::striped_array<long> next_vertex_;
    // Pointer to start of edge list to process
    emu::striped_array<long*> edges_begin_;
    // Pointer past the end of edge list to process
    emu::striped_array<long*> edges_end_;

public:

    explicit worklist(long num_vertices)
    : head_(-1)
    , next_vertex_(num_vertices)
    , edges_begin_(num_vertices)
    , edges_end_(num_vertices)
    {}

    worklist(const worklist& other, emu::shallow_copy shallow)
    : next_vertex_(other.next_vertex_, shallow)
    , edges_begin_(other.edges_begin_, shallow)
    , edges_end_(other.edges_end_, shallow)
    {}

    /**
     * Reset all replicated copies of the work queue.
     * Only valid to call on replicated instance
     */
    void clear_all ()
    {
        assert(emu::pmanip::is_repl(this));
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            get_nth(nlet).clear();
        }
    }

    /**
     * Reset the work queue so that new edges can be added
     */
    void clear()
    {
        head_ = -1;
    }

    /**
     * Returns the nth replicated copy of the work list
     * Only valid to call on a replicated instance
     * @param n nodelet ID
     * @return the nth replicated copy of the work list
     */
    worklist&
    get_nth(long n)
    {
        return *emu::pmanip::get_nth(this, n);
    }

    /**
     * Atomically append edges to the work queue.
     *
     * @param src source vertex for all edges
     * @param edges_begin Pointer to start of edge list to append
     * @param edges_end Pointer past the end of the edge list to append
     */
    void append(long src, long * edges_begin, long * edges_end)
    {
        assert(emu::pmanip::is_repl(this));
        // Get the pointer from the nodelet where src vertex lives
        volatile long * head_ptr = &get_nth(src & (NODELETS()-1)).head_;
        // Append to head of worklist
        edges_begin_[src] = edges_begin;
        edges_end_[src] = edges_end;
        long prev_head;
        do {
            prev_head = *head_ptr;
            next_vertex_[src] = prev_head;
        } while (prev_head != emu::atomic_cas(head_ptr, prev_head, src));
    }

private:
    // Worker function spawned in dynamic process_all
    template<class Visitor, long Grain>
    void worker(Visitor visitor)
    {
        long grain = Grain;
        // Walk through the worklist
        for (long src = head_; src >= 0; src = next_vertex_[src]) {
            // Get end pointer for the vertex list of src
            long * edges_end = edges_end_[src];
            // Try to atomically grab some edges to process for this vertex
            long *e1, *e2;
            for (e1 = emu::atomic_addms(&edges_begin_[src], grain);
                 e1 <= edges_end;
                 e1 = emu::atomic_addms(&edges_begin_[src], grain))
            {
                e2 = e1 + grain; if (e2 > edges_end) { e2 = edges_end; }
                // Visit each edge
                for (long * e = e1; e < e2; ++e) {
                    visitor(src, *e);
                }
            }
            // This vertex is done, move to the next one
        }
    }

public:
    /**
     * Process the edges in the worklist in parallel
     * Spawns local worker threads to pull items off of the work list.
     * @param policy Dynamic policy: grain size indicates how many items each
     * thread will pull off the worklist at a time.
     * @param visitor Lambda function to call on each edge, with signature:
     *  @c void (long src, long dst)
     */
    template<class Visitor, long Grain>
    void process(emu::dynamic_policy<Grain> policy, Visitor visitor)
    {
        for (long t = 0; t < emu::threads_per_nodelet; ++t) {
            cilk_spawn worker<Visitor, Grain>(visitor);
        }
    }

    template<class Visitor, long Grain>
    void process(emu::parallel_policy<Grain> policy, Visitor visitor)
    {
        long grain = Grain;
        // Walk through the worklist
        for (long src = head_; src >= 0; src = next_vertex_[src]) {
            long * end = edges_end_[src];
            // Spawn a thread for each granule
            for (long * e = edges_begin_[src]; e < end; e += grain) {
                auto last = e + grain <= end ? e + grain : end;
                cilk_spawn std::for_each(e, last, [src, visitor](long dst){
                    visitor(src, dst);
                });
            }
            // This vertex is done, move to the next one
        }
    }

    /**
     * Process the edges in the worklist
     * @param visitor Lambda function to call on each edge, with signature:
     *  @c void (long src, long dst)
     */
    template<class Visitor>
    void process(emu::sequenced_policy, Visitor visitor)
    {
        // Walk through the worklist
        for (long src = head_; src >= 0; src = next_vertex_[src]) {
            // Visit each edge for this vertex
            for (long * e = edges_begin_[src]; e != edges_end_[src]; ++e) {
                visitor(src, *e);
            }
            // This vertex is done, move to the next one
        }
    }
    /**
     * Process the edges in all replicated copies of the worklist
     * Spawns local worker threads on each nodelet to pull items off of
     * the work list.
     * Only valid to call on a replicated instance
     * @param policy Execution policy to use at each nodelet
     * @param visitor Lambda function to call on each edge, with signature:
     *  @c void (long src, long dst)
     */
    template<class Policy, class Visitor>
    void process_all(Policy policy, Visitor visitor)
    {
        assert(emu::pmanip::is_repl(this));
        emu::repl_for_each(emu::parallel_policy<1>(), *this,
            [policy, visitor](worklist & w) {
                w.process(policy, visitor);
            }
        );
    }
};
