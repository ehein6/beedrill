#pragma once

#include <algorithm>
#include <cilk/cilk.h>

#include "execution_policy.h"
#include "stride_iterator.h"
#include "intrinsics.h"

namespace emu::parallel {
namespace detail {

// Serial version
template<class Iterator, class UnaryFunction>
void
for_each(
    execution::sequenced_policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Forward to standard library implementation
    std::for_each(begin, end, worker);
}

// Parallel version
template<class Iterator, class UnaryFunction>
void
for_each(
    execution::parallel_policy policy,
    stride_iterator<Iterator> begin, stride_iterator<Iterator> end,
    UnaryFunction worker
) {
    auto grain = policy.grain_;
    auto radix = execution::spawn_radix;
    typename std::iterator_traits<Iterator>::difference_type size;

    // Recursive spawn
    for (;;) {
        // Keep looping until there are few enough iterations to spawn serially
        size = end - begin;
        if (size / grain <= radix) break;
        // Spawn a thread to deal with the odd elements
        auto begin_odds = begin, end_odds = end;
        split(begin, end, begin_odds, end_odds);
        cilk_migrate_hint(ptr_from_iter(begin_odds));
        cilk_spawn for_each(
            policy, begin_odds, end_odds, worker
        );
        // Recurse over the even elements
    }
    // Serial spawn
    for (; begin < end; begin += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto last = begin + grain <= end ? begin + grain : end;
        cilk_migrate_hint(ptr_from_iter(begin));
        cilk_spawn for_each(
            execution::seq,
            begin, last, worker
        );
    }
}

// Parallel dynamic version
// Special case for when our iterator type is a stride iterator
// wrapping a raw pointer
template<class T, class UnaryFunction>
void
for_each(
    execution::parallel_dynamic_policy,
    stride_iterator<T*> s_begin, stride_iterator<T*> s_end,
    UnaryFunction worker
) {
    // Shared pointer to the next item to process
    T * next = static_cast<T*>(s_begin);
    T * end = static_cast<T*>(s_end);
    // Create a worker thread for each execution slot
    for (long t = 0; t < execution::threads_per_nodelet; ++t) {
        cilk_spawn [&next, end, worker](){
            // Atomically grab the next item off the list
            for (T * item = atomic_addms(&next, 1);
                 item < end;
                 item = atomic_addms(&next, 1))
            {
                // Call the worker function on the item
                worker(*item);
            }
        }();
    }
}

template<class Iterator, class UnaryFunction>
void
for_each(
    execution::parallel_fixed_policy policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    for_each(
        execution::compute_fixed_grain(policy, begin, end),
        begin, end, worker
    );
}

} // end namespace detail

template<class ExecutionPolicy, class Iterator, class UnaryFunction,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
void
for_each(
    ExecutionPolicy policy,
    Iterator begin, Iterator end, UnaryFunction worker
){
    // Convert to stride iterators and forward
    detail::for_each(policy,
        stride_iterator<Iterator>(begin),
        stride_iterator<Iterator>(end),
        worker
    );
}

template<class Iterator, class UnaryFunction>
void
for_each(Iterator begin, Iterator end, UnaryFunction worker
){
    for_each(emu::execution::default_policy, begin, end, worker);
}


} // end namespace emu::parallel