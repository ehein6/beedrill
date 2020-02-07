#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/execution_policy.h>

class worklist {
private:
    // One per nodelet
    volatile long head_;

    // One per vertex - struct-of-arrays
    emu::striped_array<long*> edges_begin_;
    emu::striped_array<long*> edges_end_;
    emu::striped_array<long> next_vertex_;
public:

    worklist(long num_vertices)
    : head_(-1)
    , edges_begin_(num_vertices)
    , edges_end_(num_vertices)
    , next_vertex_(num_vertices)
    {}

    worklist(const worklist& other, emu::shallow_copy shallow)
    : edges_begin_(other.edges_begin_, shallow)
    , edges_end_(other.edges_end_, shallow)
    , next_vertex_(other.next_vertex_, shallow)
    {}

    void clear_all ()
    {
        assert(emu::pmanip::is_repl(this));
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            get_nth(nlet).clear();
        }
    }

    void clear()
    {
        head_ = -1;
    }

    worklist&
    get_nth(long n)
    {
        return *emu::pmanip::get_nth(this, n);
    }

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

    template<class Visitor>
    void process(emu::parallel_dynamic_policy, Visitor visitor)
    {
        long grain = 64; // TODO get from dynamic policy
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

    template<class Policy, class Visitor>
    void process_all(Policy policy, Visitor visitor)
    {
        assert(emu::pmanip::is_repl(this));
        emu::repl_for_each(emu::par, *this, [policy, visitor](worklist & w) {
            w.process(policy, visitor);
        });
    }

};
