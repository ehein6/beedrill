#pragma once

// #include "striped_for_each.h"

#include <algorithm>
#include <cilk/cilk.h>
#include "execution_policy.h"
#include "nlet_stride_iterator.h"
#include "intrinsics.h"

namespace emu::parallel {
namespace detail {

// Serial version
template<class Iterator, class UnaryFunction>
void
for_each(
    sequenced_policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Forward to standard library implementation
    std::for_each(begin, end, worker);
}

// Unrolled version
template<typename Iterator, typename Function>
class unroller
{
private:
    Function worker_;
public:
    explicit unroller(Function worker) : worker_(worker) {}
    void
    operator()(Iterator begin, Iterator end)
    {
        // Visit elements one at a time until remainder is evenly divisible by four
        while (std::distance(begin, end) % 4 != 0) {
            worker_(*begin++);
        }
        // Four items, which are meant to be held in registers
        typename std::iterator_traits<Iterator>::value_type e1, e2, e3, e4;
        for (; begin != end;) {
            // Pick up four items
            e4 = *begin++;
            e3 = *begin++;
            e2 = *begin++;
            e1 = *begin++;
            // HACK - prevent forward propagation in Emu compiler from
            // reordering these instructions
            (void)NODE_ID();
            // Visit each element without returning home
            worker_(e1);
            worker_(e2);
            worker_(e3);
            worker_(e4);
            // Once an element has been processed, we can resize to
            // avoid carrying it along with us.
            RESIZE();
        }
    }
};

template<class Iterator, class UnaryFunction>
void
for_each(
    unroll_policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    unroller<Iterator, UnaryFunction>{worker}(begin, end);
}

// Parallel version
template<class Policy, class Iterator, class UnaryFunction,
    std::enable_if_t<is_parallel_policy_v<Policy>, int> = 0>
void
for_each(
    Policy policy,
    Iterator begin, Iterator end,
    UnaryFunction worker
) {
    // Serial spawn over each granule
    auto grain = Policy::grain;
    for (; begin < end; begin += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto last = begin + grain <= end ? begin + grain : end;
        cilk_spawn_at(ptr_from_iter(begin)) detail::for_each(
            remove_parallel_t<Policy>(),
            begin, last, worker
        );
    }
}

// Static version
template<class Policy, class Iterator, class UnaryFunction,
    std::enable_if_t<is_static_policy_v<Policy>, int> = 0>
void
for_each(
   Policy policy,
   Iterator begin, Iterator end, UnaryFunction worker
) {
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    long grain = compute_fixed_grain(policy, begin, end);
    for (; begin < end; begin += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto last = begin + grain <= end ? begin + grain : end;
        cilk_spawn_at(ptr_from_iter(begin)) detail::for_each(
            remove_parallel_t<Policy>(),
            begin, last, worker
        );
    }
}

// Dynamic version
template<class Policy, class T, class UnaryOp, bool nlet_stride>
class dyn_worker
{
private:
    // Execution policy of my parent thread
    Policy policy_;
    // Pointer to the begin iterator, which can be atomically advanced
    T** next_ptr_;
    // End of the range
    T* end_;
    // Worker function to call on each item
    UnaryOp unary_op_;

    void worker_thread()
    {
        long grain = Policy::grain;
        if (nlet_stride) { grain *= NODELETS(); }
        // Atomically grab items off the list
        for (T* next = atomic_addms(next_ptr_, grain);
             next < end_;
             next = atomic_addms(next_ptr_, grain))
        {
            // Process each element
            T* last = next + grain; if (last > end_) { last = end_; }
            // May need to convert back to nlet_stride iterator
            using iter = std::conditional_t<nlet_stride,
                nlet_stride_iterator<T*>, T*>;
            detail::for_each(
                remove_parallel_t<Policy>(), iter(next), iter(last), unary_op_);
        }
    }

    /**
     * Optimized implementation for when grain size is 1
     * Compiler can't seem to be able to deduce that the for_each above
     * will only execute once in this case.
     */
    void worker_thread_1()
    {
        long increment = nlet_stride ? NODELETS() : 1;
        // Atomically grab items off the list
        for (T* next = atomic_addms(next_ptr_, increment);
             next < end_;
             next = atomic_addms(next_ptr_, increment))
        {
            // Process each element
            unary_op_(*next);
        }
    }

public:
    explicit dyn_worker(Policy policy, T** next_ptr, T* end, UnaryOp unary_op)
    : policy_(policy)
    , next_ptr_(next_ptr)
    , end_(end)
    , unary_op_(unary_op)
    {}

    void operator()()
    {
        if constexpr (Policy::grain == 1L) {
            worker_thread_1();
        } else {
            worker_thread();
        }
    }
};

// Assumes that the iterators are raw pointers that can be atomically advanced
template<class Policy, class T, class UnaryFunction,
    std::enable_if_t<is_dynamic_policy_v<Policy>, int> = 0>
void
for_each(
   Policy policy,
   T* begin, T* end,
   UnaryFunction worker)
{
    // Shared pointer to the next item to process
    T* next = begin;
    // Set up the worker functor
    dyn_worker<Policy, T, UnaryFunction, /*nlet_stride*/ false> worker_thread(
        policy, &next, end, worker);
    // Create a worker thread for each execution slot
    for (long t = 0; t < threads_per_nodelet; ++t) {
        // Create and spawn the dyn_worker functor, which captures a reference
        // to the next pointer, the end pointer, and the grain size.
        cilk_spawn worker_thread();
    }
}

// Special overload for nlet_stride_iterator, pulls the raw pointer out
// of the iterator and mulitplies stride by NODELETS() before handing off
// to the functor
template<class Policy, class T, class UnaryFunction,
    std::enable_if_t<is_dynamic_policy_v<Policy>, int> = 0>
void
for_each(
    Policy policy,
    nlet_stride_iterator<T*> s_begin, nlet_stride_iterator<T*> s_end,
    UnaryFunction worker)
{
    // Shared pointer to the next item to process
    T* next = &*s_begin;
    T* end = &*s_end;
    // Set up the worker functor
    dyn_worker<Policy, T, UnaryFunction, /*nlet_stride*/ true> worker_thread(
        policy, &next, end, worker);
    // Create a worker thread for each execution slot
    for (long t = 0; t < threads_per_nodelet; ++t) {
        // Create and spawn the dyn_worker functor, which captures a reference
        // to the next pointer, the end pointer, and the grain size.
        cilk_spawn worker_thread();
    }
}

// Serial version for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
    sequenced_policy,
    long nlet_begin, long nlet_end,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Forward to standard library implementation
    // TODO process one stripe at a time to minimize migrations
    std::for_each(begin, end, worker);
}

// Serial version for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
    unroll_policy,
    long nlet_begin, long nlet_end,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // TODO process one stripe at a time to minimize migrations
    unroller<Iterator, UnaryFunction>{worker}(begin, end);
}

// Entry point for all parallel policies with striped layouts
template<class Policy, class Iterator, class UnaryFunction>
void
striped_for_each(
   Policy policy,
   long nlet_begin, long nlet_end,
   Iterator begin, Iterator end,
   UnaryFunction worker
) {
    // Recursive spawn
    for(;;) {
        // How many nodelets do we need to spawn on?
        auto nlet_count = nlet_end - nlet_begin;
        const long nlet_radix = 8;
        if (nlet_count <= nlet_radix) { break; }
        // Divide the nodelets in half
        long nlet_mid = nlet_begin + nlet_count / 2;
        // Spawn a thread to handle the upper half
        cilk_migrate_hint(ptr_from_iter(begin + nlet_mid));
        cilk_spawn striped_for_each(
            policy, nlet_mid, nlet_end, begin, end, worker);
        // Recurse over the lower half
        nlet_end = nlet_mid;
    }

    // Serial spawn
    // Total number of elements
    auto size = end - begin;
    // Number of elements in each range
    auto stripe_size = size / NODELETS();
    // How many nodelets have an extra element?
    auto stripe_remainder = size % NODELETS();
    // For each nodelet in my subrange...
    for (long nlet = nlet_begin; nlet < nlet_end; ++nlet) {
        // Advance to the first element on the nth nodelet and convert
        // to striped iterator
        auto stripe_begin = nlet_stride_iterator<Iterator>(begin + nlet);
        // Now that the pointer has stride NODELETS(), we are addressing
        // only the elements on the nth nodelet
        auto stripe_end = stripe_begin + stripe_size;
        // Distribute remainder among first few nodelets
        if (nlet < stripe_remainder) { stripe_end += 1; }
        // Spawn a thread to handle each stripe
        cilk_migrate_hint(ptr_from_iter(stripe_begin));
        cilk_spawn detail::for_each(
            policy, stripe_begin, stripe_end, worker);
    }
}

} // end namespace detail


template<class ExecutionPolicy, class Iterator, class UnaryFunction,
   // Disable if first argument is not an execution policy
   std::enable_if_t<is_execution_policy_v<ExecutionPolicy>, int> = 0
>
void
for_each(
   ExecutionPolicy policy,
   Iterator begin, Iterator end, UnaryFunction worker
){
   if (end-begin == 0) {
       return;
   } else if (is_striped(begin)) {
       detail::striped_for_each(policy, 0, NODELETS(), begin, end, worker);
   } else {
       detail::for_each(policy, begin, end, worker);
   }
}

template<class Iterator, class UnaryFunction>
void
for_each(Iterator begin, Iterator end, UnaryFunction worker
){
   for_each(emu::default_policy, begin, end, worker);
}

} // end namespace emu::parallel
