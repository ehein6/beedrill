#pragma once

#include <algorithm>
#include <numeric>
#include <cilk/cilk.h>
#include "execution_policy.h"
#include "nlet_stride_iterator.h"
#include "replicated.h"
#include "intrinsics.h"

#include <cilk/cilk.h>
extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}


namespace emu::parallel {
namespace detail {

template<class ForwardIt, class T, class BinaryOp>
T
reduce(sequenced_policy policy, ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    return std::accumulate(first, last, init, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(parallel_policy policy,
       ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    // Compute number of spawns that will occur based on grain size
    auto grain = policy.grain_;
    long num_spawns = (std::distance(first, last) + grain - 1) / grain;
    // Allocate a private T for each thread that will be spawned
    std::vector<T> partial_sums(num_spawns);
    long tid = 0;
    // Serial spawn over each granule
    for (;first < last; first += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto begin = first;
        auto end = begin + grain <= last ? begin + grain : last;
        // Spawned thread will copy result to i'th partial sum
        // NOTE: this line causes an internal compiler error on GCC 7
        cilk_migrate_hint(ptr_from_iter(begin));
        partial_sums[tid] = cilk_spawn reduce(
            seq, begin, end, init, binary_op);
        // Moving the increment out of the spawn expression to avoid possible race
        // This shouldn't be necessary, but the compiler gets this wrong
        tid += 1;
    }
    // Wait for all partial sums to be valid
    cilk_sync;
    // Reduce partial sums in this thread
    return reduce(seq, partial_sums.begin(), partial_sums.end(),
        init, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
striped_reduce(parallel_policy policy,
               ForwardIt first, ForwardIt last,
               T init, BinaryOp binary_op)
{
    // Allocate a partial sum on each nodelet
    // Using replicated storage, but we'll convert to absolute ptr later
    auto partials = emu::make_repl_ctor<T>(init);
    // Total number of elements
    auto size = std::distance(first, last);
    // Number of elements in each range
    auto stripe_size = size / NODELETS();
    // How many nodelets have an extra element?
    auto stripe_remainder = size % NODELETS();
    // Spawn a thread on each nodelet:
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        // 1. Convert from iterator to raw pointer
        // 2. Advance to the first element on the nth nodelet
        // 3. Convert to striped iterator
        auto stripe_begin = nlet_stride_iterator<ForwardIt>(first + nlet);
        // Now that the pointer has stride NODELETS(), we are addressing only
        // the elements on the nth nodelet
        auto stripe_end = stripe_begin + stripe_size;
        // Distribute remainder among first few nodelets
        if (nlet < stripe_remainder) { stripe_end += 1; }
        // Spawn a thread to handle each stripe
        cilk_migrate_hint(ptr_from_iter(stripe_begin));
        partials->get_nth(nlet) = cilk_spawn reduce(
            policy, stripe_begin, stripe_end, init, binary_op);
    }
    // Wait for all partial sums to be computed
    cilk_sync;
    // Reduce across the partial sums
    return repl_reduce(partials, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(parallel_fixed_policy policy, ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    return reduce(compute_fixed_grain(policy, first, last),
        first, last, init, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
striped_reduce(parallel_fixed_policy policy, ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    return reduce(compute_fixed_grain(policy, first, last),
        first, last, init, binary_op);
}

} // end namespace detail

// Top-level dispatch functions

template<class ExecutionPolicy, class ForwardIt, class T, class BinaryOp,
    // Disable if first argument is not an execution policy
    std::enable_if_t<is_execution_policy_v<ExecutionPolicy>, int> = 0
>
T
reduce(ExecutionPolicy policy, ForwardIt first, ForwardIt last,
    T init = typename std::iterator_traits<ForwardIt>::value_type{},
    BinaryOp binary_op = std::plus<>())
{
    if (std::distance(first, last) == 0) {
        return init;
    } else if (is_striped(first)){
        return detail::striped_reduce(policy, first, last, init, binary_op);
    } else {
        return detail::reduce(policy, first, last, init, binary_op);
    }
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(ForwardIt first, ForwardIt last,
    T init = typename std::iterator_traits<ForwardIt>::value_type{},
    BinaryOp binary_op = std::plus<>())
{
    return reduce(default_policy, first, last, init, binary_op);
}

} // end namespace emu::parallel
