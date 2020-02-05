#pragma once

#include <algorithm>
#include <numeric>
#include <iterator>

#include "execution_policy.h"
#include "intrinsics.h"

#include <cilk/cilk.h>
extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}

/**
 * Dispatch strategy:
 *
 * 1. Use public overloads to supply default arguments
 * 2. Will temp objects live on the stack, or in registers?
 * 3. Do we have an atomic reduce operation?
 *
 */


namespace emu::parallel {
namespace detail {


//// Specialized for summing up longs
//template<class ForwardIt, class BinaryOp>
//void
//reduce_with_remotes(parallel_policy policy,
//    ForwardIt first, ForwardIt last, long &parent_sum, BinaryOp binary_op)
//{
//
//    for_each(first, last, [])
//
//    // This thread is responsible for the sub-range (first, last)
//    long my_sum = std::accumulate(first, last, 0, binary_op);
//    // Wait for children to finish
//    cilk_sync;
//    // Add to parent's sum
//    remote_op<BinaryOp>()(parent_sum, binary_op(children_sum, my_sum));
//}
//
//// Specialization for long type
//template<class ForwardIt, class T, class BinaryOp>
//typename std::enable_if_t<
//    std::is_same_v<long, typename std::iterator_traits<ForwardIt>::value_type>, T>
//reduce(ForwardIt first, ForwardIt last, T init, BinaryOp binary_op) {
//    long sum = init;
//    reduce_with_remotes(first, last, sum, binary_op);
//    return sum;
//}
//
template<class ForwardIt, class T, class BinaryOp>
T
reduce(sequenced_policy policy, ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    return std::accumulate(first, last, init, binary_op);
}

long num_spawns(long size, long grain)
{
    long num_spawns = 0;
    // Convert from number of elements to number of chunks
    long num_chunks = (size + grain - 1) / grain;
    const long radix = emu::spawn_radix;
    // Recursive spawn: 1 per time we divide the range in two
    while (num_chunks / 2 > radix) {
        num_chunks /= 2;
        num_spawns += 1;
    }
    // Serial spawn: 1 per chunk
    num_spawns += num_chunks;

    return num_spawns;
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(parallel_policy policy,
    stride_iterator<ForwardIt> first, stride_iterator<ForwardIt> last,
    T init, BinaryOp binary_op)
{
    auto grain = policy.grain_;
    auto radix = emu::spawn_radix;
    typename std::iterator_traits<ForwardIt>::difference_type size;

    // Allocate a private T for each thread that will be spawned
    std::vector<T> partial_sums(
        num_spawns(std::distance(first, last), grain));
    long tid = 0;

    // Recursive spawn
    for (;;) {
        // Keep looping until there are few enough iterations to spawn serially
        size = last - first;
        if (size / grain <= radix) break;
        // Spawn a thread to deal with the odd elements
        auto first_odds = first, last_odds = last;
        split(first, last, first_odds, last_odds);
        cilk_migrate_hint(ptr_from_iter(first_odds));
        partial_sums[tid] = cilk_spawn reduce(
            policy, first_odds, last_odds, init, binary_op
        );
        // Move to next partial sum
        tid += 1;
    }
    // Serial spawn over each granule
    for (;first < last; first += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto begin = first;
        auto end = begin + grain <= last ? begin + grain : last;
        cilk_migrate_hint(ptr_from_iter(begin));
        // Spawned thread will copy result to i'th partial sum
        // NOTE: this line causes an internal compiler error on GCC 7
        partial_sums[tid] = cilk_spawn reduce(
            seq, begin, end, init, binary_op
        );
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
reduce(parallel_fixed_policy policy, ForwardIt first, ForwardIt last,
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
    return detail::reduce(policy,
        stride_iterator<ForwardIt>(first),
        stride_iterator<ForwardIt>(last),
        init, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(ForwardIt first, ForwardIt last,
    T init = typename std::iterator_traits<ForwardIt>::value_type{},
    BinaryOp binary_op = std::plus<>())
{
    return detail::reduce(default_policy,
        stride_iterator<ForwardIt>(first),
        stride_iterator<ForwardIt>(last),
        init, binary_op);
}

} // end namespace emu::parallel
