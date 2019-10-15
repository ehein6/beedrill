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
//reduce_with_remotes(execution::parallel_policy policy,
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
reduce(execution::sequenced_policy policy, ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    return std::accumulate(first, last, init, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(execution::parallel_policy policy, ForwardIt first, ForwardIt last,
    T init, BinaryOp binary_op)
{
    auto grain = policy.grain_;
    // How many threads will we spawn?
    auto num_spawns = (last - first + grain - 1) / grain;
    // Allocate a private T for each one
    // PROBLEM: where do we put them?
    std::vector<T> partial_sums(num_spawns);

    // Serial spawn over each granule
    for (long i = 0; first < last; first += grain) {
        // Spawn a thread to handle each granule
        // Last iteration may be smaller if things don't divide evenly
        auto begin = first;
        auto end = begin + grain <= last ? begin + grain : last;
        cilk_migrate_hint(ptr_from_iter(begin));
        // Spawned thread will copy result to i'th partial sum
        // NOTE: this line causes an internal compiler error on GCC 7
        partial_sums[i] = cilk_spawn reduce(
            execution::seq,
            begin, end, init, binary_op
        );
        // Moving the increment out of the spawn expression to avoid possible race
        // This shouldn't be necessary, but the compiler gets this wrong
        i += 1;
    }
    // Wait for all partial sums to be valid
    cilk_sync;
    // Reduce partial sums in this thread
    return reduce(execution::seq, partial_sums.begin(), partial_sums.end(),
        init, binary_op);
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(execution::parallel_fixed_policy policy, ForwardIt first, ForwardIt last,
       T init, BinaryOp binary_op)
{
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    return reduce(execution::compute_fixed_grain(policy, first, last),
        first, last, init, binary_op);
}

} // end namespace detail

// Top-level dispatch functions

template<class ExecutionPolicy, class ForwardIt, class T, class BinaryOp,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
T
reduce(ExecutionPolicy policy, ForwardIt first, ForwardIt last, T init, BinaryOp binary_op)
{
    return detail::reduce(policy, first, last, init, binary_op);
}

template<class ExecutionPolicy, class ForwardIt, class T,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
T
reduce(ExecutionPolicy policy, ForwardIt first, ForwardIt last, T init)
{
    return reduce(policy, first, last, init, std::plus<>());
}

template<class ExecutionPolicy, class ForwardIt,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
typename std::iterator_traits<ForwardIt>::value_type
reduce(ExecutionPolicy policy, ForwardIt first, ForwardIt last)
{
    typename std::iterator_traits<ForwardIt>::value_type init{};
    return reduce(policy, first, last, init);
}

template<class ForwardIt, class T, class BinaryOp>
T
reduce(ForwardIt first, ForwardIt last, T init, BinaryOp binary_op)
{
    return detail::reduce(execution::default_policy,
        first, last, init, binary_op);
}

template<class ForwardIt, class T>
T
reduce(ForwardIt first, ForwardIt last, T init)
{
    return reduce(first, last, init, std::plus<>());
}

template<class ForwardIt>
typename std::iterator_traits<ForwardIt>::value_type
reduce(ForwardIt first, ForwardIt last)
{
    typename std::iterator_traits<ForwardIt>::value_type init{};
    return reduce(first, last, init);
}

} // end namespace emu::parallel
