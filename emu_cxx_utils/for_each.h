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
            // Visit each element without returning home
            // Once an element has been processed, we can resize to
            // avoid carrying it along with us.
            worker_(e1);
            worker_(e2);
            worker_(e3);
            worker_(e4);
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
    auto grain = policy.grain_;
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
   detail::for_each(
       compute_fixed_grain(policy, begin, end),
       begin, end, worker
   );
}

// Dynamic version
template<class Policy, class T, class Function>
class dyn_worker
{
private:
    // Pointer to the begin iterator, which can be atomically advanced
    T** next_ptr_;
    // End of the range
    T* end_;
    // Number of elements to claim at a time
    long grain_;
    // Worker function to call on each item
    Function worker_;
public:
    explicit dyn_worker(T** next_ptr, T* end, long grain, Function worker)
    : next_ptr_(next_ptr)
    , end_(end)
    , grain_(grain)
    , worker_(worker)
    {}

    void operator()()
    {
        // Atomically grab items off the list
        for (T* item = atomic_addms(next_ptr_, grain_);
             item < end_;
             item = atomic_addms(next_ptr_, grain_))
        {
            auto end = item + grain_ > end_ ? end_ : item + grain_;
            // Call the worker function on the items
            detail::for_each(Policy(), item, end, worker_);
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
    // Pick the right execution policy for each worker to use
    using serial = remove_parallel_t<Policy>;
    dyn_worker<serial, T, UnaryFunction> worker_thread(
        &next, end, policy.grain_, worker);
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
    // Pick the right execution policy for each worker to use
    using serial = remove_parallel_t<Policy>;
    dyn_worker<serial, T, UnaryFunction> worker_thread(
        &next, end, policy.grain_ * NODELETS(), worker);
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
   Iterator begin, Iterator end, UnaryFunction worker
) {
    // Forward to standard library implementation
    // TODO process one stripe at a time to minimize migrations
    std::for_each(begin, end, worker);
}

// Entry point for all parallel policies with striped layouts
template<class Policy, class Iterator, class UnaryFunction>
void
striped_for_each(
   Policy policy,
   Iterator begin, Iterator end,
   UnaryFunction worker
) {
   // Total number of elements
   auto size = end - begin;
   // Number of elements in each range
   auto stripe_size = size / NODELETS();
   // How many nodelets have an extra element?
   auto stripe_remainder = size % NODELETS();
   // Spawn a thread on each nodelet:
   for (long nlet = 0; nlet < NODELETS(); ++nlet) {
       // 1. Convert from iterator to raw pointer
       // 2. Advance to the first element on the nth nodelet
       // 3. Convert to striped iterator
       auto stripe_begin = nlet_stride_iterator<Iterator>(begin + nlet);
       // Now that the pointer has stride NODELETS(), we are addressing only
       // the elements on the nth nodelet
       auto stripe_end = stripe_begin + stripe_size;
       // Distribute remainder among first few nodelets
       if (nlet < stripe_remainder) { stripe_end += 1; }
       // Spawn a thread to handle each stripe
       cilk_spawn_at(ptr_from_iter(stripe_begin)) detail::for_each(
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
       detail::striped_for_each(policy, begin, end, worker);
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
