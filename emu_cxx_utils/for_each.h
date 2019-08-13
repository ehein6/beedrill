#pragma once

#include <algorithm>
#include <cilk/cilk.h>
#include "execution_policy.h"
#include "pointer_manipulation.h"

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
    Iterator begin, Iterator end,
    UnaryFunction worker
) {
    // Serial spawn over each granule
    auto grain = policy.grain_;
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

// Parallel fixed version
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

// Serial version for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
    execution::sequenced_policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Forward to standard library implementation
    // TODO we could save migrations by doing it one stripe at a time
    std::for_each(begin, end, worker);
}

// Parallel version for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
    execution::parallel_policy policy,
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
        // 3. Convert from view-2 to view-1
        // 4. Construct Iterator from raw pointer
        auto stripe_begin = Iterator(pmanip::view2to1(&*(begin) + nlet));
        // Now that the pointer is view-1, we are addressing only the
        // elements on the nth nodelet
        auto stripe_end = stripe_begin + stripe_size;
        // Distribute remainder among first few nodelets
        if (nlet < stripe_remainder) { stripe_end += 1; }
        // Spawn a thread to handle each stripe
        cilk_spawn_at(&*stripe_begin) for_each(
            policy, stripe_begin, stripe_end, worker
        );
    }
}

// Parallel fixed for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
    execution::parallel_fixed_policy policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    striped_for_each(
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
    if (pmanip::is_striped(&*begin)) {
        detail::striped_for_each(policy, begin, end, worker);
    } else {
        detail::for_each(policy, begin, end, worker);
    }
}

} // end namespace emu::parallel