#pragma once

#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/for_each.h>

class sliding_queue
{
private:
    // Next available slot in the queue
    long next_;
    // Start and end of the current window
    long start_;
    long end_;
    // Index of the current window
    long window_;
    // Storage for items in the queue
    emu::repl<long *> buffer_;
    // Starting positions of each window
    emu::repl<long *> heads_;

public:

    long * begin() { return &buffer_[start_]; }
    long * end() { return &buffer_[end_]; }

    explicit sliding_queue(long size)
    {
        // TODO write class for repl array
        buffer_ = static_cast<long*>(mw_mallocrepl(size * sizeof(long)));
        heads_ = static_cast<long*>(mw_mallocrepl(size * sizeof(long)));
        assert(buffer_ && heads_);
        reset_all();
    }

    // Shallow copy constructor
    sliding_queue(const sliding_queue& other, emu::shallow_copy)
    : next_(other.next_)
    , start_(other.start_)
    , end_(other.end_)
    , window_(other.window_)
    , buffer_(other.buffer_)
    , heads_(other.heads_)
    {}

    ~sliding_queue()
    {
        mw_free(buffer_);
        mw_free(heads_);
    }

    void
    reset()
    {
        next_ = 0;
        start_ = 0;
        end_ = 0;
        window_ = 0;
    }

    void
    reset_all()
    {
        for (long n = 0; n < NODELETS(); ++n) {
            get_nth(n).reset();
        }
    }

    // Returns a reference to the copy of T on the Nth nodelet
    sliding_queue&
    get_nth(long n)
    {
        assert(n < NODELETS());
        return *static_cast<sliding_queue*>(mw_get_nth(this, n));
    }


    void
    slide_window()
    {
        start_ = window_ == 0 ? 0 : heads_[window_ - 1];
        end_ = next_;
        heads_[window_] = end_;
        window_ += 1;
    }

    void
    slide_all_windows()
    {
        for (long n = 0; n < NODELETS(); ++n) {
            get_nth(n).slide_window();
        }
    }

    void
    push_back(long v)
    {
        long pos = ATOMIC_ADDMS(&next_, 1);
        buffer_[pos] = v;
    }

    bool
    is_empty()
    {
        return start_ == end_;
    }

    long
    size()
    {
        return end_ - start_;
    }

    bool
    all_empty()
    {
        for (long n = 0; n < NODELETS(); ++n) {
            if (!get_nth(n).is_empty()) {
                return false;
            }
        }
        return true;
    }

    long
    combined_size()
    {
        long size = 0;
        for (long n = 0; n < NODELETS(); ++n) {
            REMOTE_ADD(&size, get_nth(n).size());
        }
        return size;
    }

    void
    dump_all()
    {
        for (long n = 0; n < NODELETS(); ++n) {
            sliding_queue & local_queue = get_nth(n);
            for (long i = local_queue.start_; i < local_queue.end_; ++i) {
                printf("%li ", local_queue.buffer_[i]);
            }
        }
    }

    template<class Function>
    void forall_items(Function worker)
    {
        // First, spawn a thread on each nodelet to handle the local queue
        emu::repl_for_each(emu::execution::parallel_policy(1), *this,
            [&](sliding_queue& queue){
                // Spawn threads to dynamically pull items off of this queue
                emu::parallel::for_each(
                    emu::execution::dyn,
                    queue.begin(), queue.end(), worker
                );
            }
        );
    }
};