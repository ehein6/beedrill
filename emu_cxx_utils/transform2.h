#pragma once

namespace emu::parallel {

namespace detail {

template<class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2
transform(
    execution::sequenced_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    UnaryOperation unary_op)
{
    // Forward to standard library implementation
    return std::transform(first1, last1, first2, unary_op);
}

// Parallel version
template<class ForwardIt1, class ForwardIt2, class UnaryOperation>
void
transform(
    execution::parallel_policy policy,
    stride_iterator<ForwardIt1> first1, stride_iterator<ForwardIt1> last1,
    stride_iterator<ForwardIt2> first2,
    UnaryOperation unary_op)
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
        stretch(first1_odds, last1_odds);
        stretch(first2);
        cilk_spawn_at(&*(first1_odds)) transform( policy,
            first1_odds, last1_odds, first2_odds, unary_op
        );
        // "Stretch" the iterators, so they only cover the even elements
        stretch(first1, last1);
        stretch(first2);
    }

    // Serial spawn
    for (; first1 < last1; first1 += grain, first2 += grain) {
        auto last = first1 + grain <= last1 ? first1 + grain : last1;
        cilk_spawn_at(&*first1) transform(
            execution::seq,
            first1, last, first2, unary_op
        );
    }
}

template<class ForwardIt1, class ForwardIt2, class UnaryOperation>
void transform(
    execution::parallel_fixed_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    UnaryOperation unary_op)
{
    // Recalculate grain size to limit thread count
    // and forward to unlimited parallel version
    transform(
        execution::compute_fixed_grain(policy, first1, last1),
        first1, last1, first2, unary_op
    );
}

} // end namespace detail

// Convert to stride iterators and forward
template<class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
    class UnaryOperation>
void
transform(
    ExecutionPolicy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    UnaryOperation unary_op)
{
    // Convert to stride iterators and forward
    detail::transform(
        policy,
        stride_iterator<ForwardIt1>(first1),
        stride_iterator<ForwardIt1>(last1),
        stride_iterator<ForwardIt2>(first2),
        unary_op
    );
}

} // end namespace emu::parallel
