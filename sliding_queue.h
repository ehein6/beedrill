#pragma once

#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/repl_array.h>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/pointer_manipulation.h>

class sliding_queue
{
private:
    // Index of next available slot in the queue
    long next_;
    // Start and end of the current window
    long start_;
    long end_;
    // Index of the current window
    long window_;
    // Storage for items in the queue
    emu::repl_array<long> buffers_;
    // Storage for starting positions of each window
    emu::repl_array<long> heads_;

    long * head_ptr_;
    long * buffer_ptr_;

public:

    long * begin() { return &buffer_ptr_[start_]; }
    long * end() { return &buffer_ptr_[end_]; }

    explicit sliding_queue(long size)
    : buffers_(size)
    , heads_(size)
    , head_ptr_(heads_.get_localto(this))
    , buffer_ptr_(buffers_.get_localto(this))
    {
        reset();
    }

    // Shallow copy constructor
    sliding_queue(const sliding_queue& other, emu::shallow_copy)
    : next_(other.next_)
    , start_(other.start_)
    , end_(other.end_)
    , window_(other.window_)
    , buffers_(other.buffers_)
    , heads_(other.heads_)
    , head_ptr_(heads_.get_localto(this))
    , buffer_ptr_(buffers_.get_localto(this))
    {}

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
        // Call reset on each copy of the queue
        emu::repl_for_each(*this, std::mem_fn(&sliding_queue::reset));
    }

    // Returns a reference to the copy of T on the Nth nodelet
    sliding_queue&
    get_nth(long n)
    {
        return *emu::pmanip::get_nth(this, n);
    }

    void
    slide_window()
    {
        start_ = window_ == 0 ? 0 : head_ptr_[window_ - 1];
        end_ = next_;
        head_ptr_[window_] = end_;
        window_ += 1;
    }

    void
    slide_all_windows()
    {
        // Call slide_window on each replicated copy
        emu::repl_for_each(*this, std::mem_fn(&sliding_queue::slide_window));
    }

    void
    push_back(long v)
    {
        long pos = emu::atomic_addms(&next_, 1);
        buffer_ptr_[pos] = v;
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
            size += get_nth(n).size();
        }
        return size;
    }

    void
    dump()
    {
        for (long i = start_; i < end_; ++i) {
            printf("%li ", buffer_ptr_[i]);
        }
    }

    void
    dump_all()
    {
        // Call dump() on each copy
        emu::repl_for_each(*this, std::mem_fn(&sliding_queue::dump));
    }

    template<class Function>
    void forall_items(Function worker)
    {
        // First, spawn a thread on each nodelet to handle the local queue
        emu::repl_for_each(emu::parallel_policy(1), *this,
            [&](sliding_queue& queue){
                // Spawn threads to dynamically pull items off of this queue
                emu::parallel::for_each(
                    emu::dyn,
                    queue.begin(), queue.end(), worker
                );
            }
        );
    }
};