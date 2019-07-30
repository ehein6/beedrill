#pragma once
#include "common.h"
#include <algorithm>
#include <iterator>
#include "execution_policy.h"

namespace emu::parallel {

template< class ForwardIt1, class ForwardIt2, class ForwardIt3, class BinaryOperation >
ForwardIt3
transform(
    emu::execution::sequenced_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op)
{
    return std::transform(first1, last1, first2, first3, binary_op);
}

//
//// Parallel std::transform (unary op version) for sequential layouts
//template< class ForwardIt1, class ForwardIt2, class UnaryOperation >
//void transform_dispatch(
//    ForwardIt1 first1, ForwardIt1 last1,
//    ForwardIt2 first2,
//    UnaryOperation unary_op,)
//{
//    // TODO choose grain size
//    const long grain = 4;
//    for (;;) {
//        /* How many elements in my range? */
//        long count = std::distance(first1, last1);
//
//        /* Break out when my range is smaller than the grain size */
//        if (count <= grain) break;
//
//        /* Divide the range in half */
//        /* Invariant: count >= 2 */
//        auto mid1 = std::next(first1, count/2);
//        auto mid2 = std::next(first2, count/2);
//
////        printf("T%li: Spawning for range [%li, %li]\n", THREAD_ID(), *first1, *(mid1-1));
//        /* Spawn a thread to deal with the lower half */
//        // Removing this spawn fixes it as well...
//        cilk_migrate_hint(&*first1);
//        cilk_spawn transform_dispatch(
//            first1, mid1,
//            first2,
//            unary_op,
//            sequential_layout_tag()
//        );
//
//        /* Shrink range to upper half and repeat */
//        first1 = mid1;
//        first2 = mid2;
//        // This print statement forces it to work
////        printf("T%li: Recursing for range [%li, %li]\n", THREAD_ID(), *first1, *(last1-1));
//    }
////    printf("T%li: Transforming range [%li, %li]\n", THREAD_ID(), *first1, *(last1-1));
//    std::transform(first1, last1, first2, unary_op);
//}

// Parallel std::transform (binary op version) for sequential layouts
template< class ForwardIt1, class ForwardIt2, class ForwardIt3, class BinaryOperation >
void transform(
    emu::execution::parallel_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op)
{
    auto grain = policy.grain_;
    auto radix = emu::execution::spawn_radix;
    typename std::iterator_traits<ForwardIt1>::difference_type size;

    // Recursive spawn
    for (;;) {
        // Keep looping until there are few enough iterations to spawn serially
        size = std::distance(first1, last1);
        if (size/grain <= radix) break;
        // Cut the range in half
        /* Divide the range in half */
        /* Invariant: count >= 2 */
        auto mid1 = std::next(first1, size/2);
        auto mid2 = std::next(first2, size/2);
        auto mid3 = std::next(first3, size/2);
        // Spawn a thread to deal with the upper half
        cilk_migrate_hint(&*first1);
        cilk_spawn transform(
            policy,
            first1, mid1,
            first2,
            first3,
            binary_op
        );
        // Shrink range to lower half and repeat
        /* Shrink range to upper half and repeat */
        first1 = mid1;
        first2 = mid2;
        first3 = mid3;
    }
    if (size > grain) {
        // Serial spawn
        for (; first1 < last1; first1 += grain, first2 += grain, first3 += grain) {
            ForwardIt1 last = first1 + grain <= last1 ? first1 + grain : last1;
            cilk_spawn transform(
                execution::seq,
                first1, last, first2, first3, binary_op
            );
        }
    } else {
        // Serial execution
        transform(execution::seq, first1, last1, first2, first3, binary_op);
    }
}



// Top-level dispatch functions

//// Unary op version
//template< class ForwardIt1, class ForwardIt2, class UnaryOperation >
//ForwardIt2 transform( ForwardIt1 first1, ForwardIt1 last1,
//                      ForwardIt2 d_first, UnaryOperation unary_op )
//{
//    typename iterator_layout<ForwardIt1>::value layout;
//    transform_dispatch(first1, last1, d_first, unary_op, layout);
//    return d_first + std::distance(first1, last1);
//}

// Limited parallel version
template< class ForwardIt1, class ForwardIt2, class ForwardIt3, class BinaryOperation >
void transform(
    emu::execution::parallel_limited_policy policy,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op)
{
    // Recalculate grain size to limit thread count
    auto grain = emu::execution::limit_grain(
        policy.grain_,
        last1-first1,
        emu::execution::threads_per_nodelet
    );
    // Forward to unlimited parallel version
    emu::parallel::transform(
        emu::execution::parallel_policy(grain),
        first1, last1, first2, first3, binary_op
    );
}

} // end namespace emu::parallel
