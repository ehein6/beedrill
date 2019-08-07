#pragma once

namespace emu::parallel {

namespace detail {

template< class ForwardIt1, class ForwardIt2, class ForwardIt3,
    class BinaryOperation>
ForwardIt3
transform(
    execution::sequenced_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op)
{
    // Forward to standard library implementation
    return std::transform(first1, last1, first2, first3, binary_op);
}

// Parallel version
template< class ForwardIt1, class ForwardIt2, class ForwardIt3,
    class BinaryOperation>
void
transform(
    execution::parallel_policy policy,
    stride_iterator<ForwardIt1> first1, stride_iterator<ForwardIt1> last1,
    stride_iterator<ForwardIt2> first2,
    stride_iterator<ForwardIt3> first3,
    BinaryOperation binary_op)
{
    auto grain = policy.grain_;
    auto radix = emu::execution::spawn_radix;
    typename std::iterator_traits<ForwardIt1>::difference_type size;

    // Recursive spawn
    for (;;) {
        // Keep looping until there are few enough iterations to spawn serially
        size = last1 - first1;
        if (size / grain <= radix) break;
        // Spawn a thread to deal with the odd elements
        auto first1_odds = first1 + 1;
        auto last1_odds = last1;
        auto first2_odds = first2 + 1;
        auto first3_odds = first3 + 1;
        stretch(first1_odds, last1_odds);
        stretch(first2_odds);
        stretch(first3_odds);
        cilk_spawn_at(&*(first1_odds)) transform( policy,
            first1_odds, last1_odds, first2_odds, first3_odds, binary_op
        );
        // "Stretch" the iterators, so they only cover the even elements
        stretch(first1, last1);
        stretch(first2);
        stretch(first3);
    }

    // Serial spawn
    for (; first1 < last1; first1 += grain, first2 += grain, first3 += grain) {
        auto last = first1 + grain <= last1 ? first1 + grain : last1;
        cilk_spawn_at(&*first1) transform(
            execution::seq,
            first1, last, first2, first3, binary_op
        );
    }
}

template< class ForwardIt1, class ForwardIt2, class ForwardIt3,
    class BinaryOperation>
void transform(
    execution::parallel_fixed_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op)
{
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    transform(
        execution::compute_fixed_grain(policy, first1, last1),
        first1, last1, first2, first3, binary_op
    );
}

} // end namespace detail

// Convert to stride iterators and forward
template<class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
    class ForwardIt3, class BinaryOperation>
void
transform(
    ExecutionPolicy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op)
{
    // Convert to stride iterators and forward
    detail::transform(
        policy,
        stride_iterator<ForwardIt1>(first1),
        stride_iterator<ForwardIt1>(last1),
        stride_iterator<ForwardIt2>(first2),
        stride_iterator<ForwardIt3>(first3),
        binary_op
    );
}

} // end namespace emu::parallel
