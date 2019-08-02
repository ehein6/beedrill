#pragma once

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
        auto begin_odds = (begin + 1).stretch();
        auto end_odds = (end + 1).stretch();
        cilk_spawn_at(&*(begin_odds)) for_each(
            policy, begin_odds, end_odds, worker
        );
        // "Stretch" the iterator, so it only covers the even elements
        begin = begin.stretch();
        end = end.stretch();
    }
    // Serial spawn
    for (; begin < end; begin += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto last = begin + grain <= end ? begin + grain : end;
        cilk_spawn_at(&*begin) for_each(
            execution::seq,
            begin, last, worker
        );
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

template<class ExecutionPolicy, class Iterator, class UnaryFunction>
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

} // end namespace emu::parallel