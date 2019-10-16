#pragma once

#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>

#include "transform.h"
#include "reduce.h"

namespace emu::parallel {
namespace detail {

template<class ExecutionPolicy, class ForwardIt, class T, class BinaryOp, class UnaryOp>
T transform_reduce(
    ExecutionPolicy &&policy,
    ForwardIt first, ForwardIt last,
    T init, BinaryOp binary_op, UnaryOp unary_op) {
    // Create transform iterator with unary_op
    auto first_t = boost::make_transform_iterator(first, unary_op);
    auto last_t = boost::make_transform_iterator(last, unary_op);

    // Reduce using binary_op
    return reduce(policy, first_t, last_t, init, binary_op);
}

template<class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T, class BinaryOp1, class BinaryOp2>
T transform_reduce(
    ExecutionPolicy &&policy,
    ForwardIt1 first1, ForwardIt1 last1, ForwardIt2 first2,
    T init, BinaryOp1 binary_op1, BinaryOp2 binary_op2)
{
    // Compute end of second range
    auto last2 = std::next(first2, std::distance(first1, last1));
    // Zip ranges together
    auto first = boost::make_zip_iterator(
        std::make_tuple(first1, first2));
    auto last = boost::make_zip_iterator(
        std::make_tuple(last1, last2));
    // Helper function to map the binary op onto a tuple
    auto worker = [binary_op2](auto tuple) {
        return binary_op2(std::get<0>(tuple), std::get<1>(tuple));
    };
    // Convert to transform iterator
    auto first_t = boost::make_transform_iterator(first, worker);
    auto last_t = boost::make_transform_iterator(last, worker);

    // Reduce using binary_op1
    return reduce(policy, first_t, last_t, init, binary_op1);
}

} // end namespace detail

// Wrappers to provide default arguments

template<class ExecutionPolicy,
    class ForwardIt1, class ForwardIt2, class T,
    class BinaryOp1, class BinaryOp2,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
T transform_reduce(ExecutionPolicy &&policy,
                   ForwardIt1 first1, ForwardIt1 last1, ForwardIt2 first2,
                   T init, BinaryOp1 binary_op1, BinaryOp2 binary_op2) {
    return detail::transform_reduce(
        policy, first1, last1, first2, init, binary_op1, binary_op2
    );
}

template<class ExecutionPolicy,
    class ForwardIt, class T, class BinaryOp, class UnaryOp,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
T transform_reduce(ExecutionPolicy &&policy,
                   ForwardIt first, ForwardIt last,
                   T init, BinaryOp binary_op, UnaryOp unary_op) {
    return detail::transform_reduce(
        policy, first, last, init, binary_op, unary_op
    );
}

template<class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
T transform_reduce(ExecutionPolicy &&policy,
                   ForwardIt1 first1, ForwardIt1 last1, ForwardIt2 first2, T init) {
    return detail::transform_reduce(
        policy, first1, last1, init, std::plus<>(), std::multiplies<>());
}

template<class InputIt1, class InputIt2, class T>
T transform_reduce(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init) {
    return detail::transform_reduce(
        execution::default_policy, first1, last1, init,
        std::plus<>(), std::multiplies<>()
    );
}

template<class InputIt1, class InputIt2, class T, class BinaryOp1, class BinaryOp2>
T transform_reduce(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                   T init, BinaryOp1 binary_op1, BinaryOp2 binary_op2) {
    return detail::transform_reduce(execution::default_policy, first1, last1,
        first2, init, binary_op1, binary_op2);
}

template<class InputIt, class T, class BinaryOp, class UnaryOp>
T transform_reduce(InputIt first, InputIt last,
                   T init, BinaryOp binary_op, UnaryOp unary_op)
{
    return detail::transform_reduce(execution::default_policy, first, last,
        init, binary_op, unary_op);
}



} // end namespace emu::parallel