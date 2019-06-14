#pragma once

#include <algorithm>
#include <iterator>
#include <cilk/cilk.h>
extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}
#include "iterator_layout.h"

namespace emu {
namespace parallel {

// Parallel std::transform (unary op version) for sequential layouts
template< class ForwardIt1, class ForwardIt2, class UnaryOperation >
void transform_dispatch( void * hint,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    UnaryOperation unary_op)
{
    (void)hint;
    // TODO choose grain size
    const long grain = 4;
    for (;;) {
        /* How many elements in my range? */
        long count = std::distance(first1, last1);

        /* Break out when my range is smaller than the grain size */
        if (count <= grain) break;

        /* Divide the range in half */
        /* Invariant: count >= 2 */
        auto mid1 = std::next(first1, count/2);
        auto mid2 = std::next(first2, count/2);

        /* Spawn a thread to deal with the lower half */
        cilk_spawn transform_dispatch(&*first1,
            first1, mid1,
            first2,
            unary_op,
            sequential_layout_tag()
        );

        /* Shrink range to upper half and repeat */
        first1 = mid1;
        first2 = mid2;
    }
    std::transform(first1, last1, first2, unary_op);
}

// Parallel std::transform (unary op version) for striped layouts
template< class ForwardIt1, class ForwardIt2, class UnaryOperation >
void transform_dispatch( void * hint,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    UnaryOperation unary_op,
    striped_layout_tag)
{
    (void)hint;
    long size = std::distance(first1, last1);
    // Spawn a thread on each nodelet
    for (long i = 0; i < NODELETS() && i < size; ++i) {
        // Convert to striped iterators and forward to sequential version
        typename ForwardIt1::as_striped_iterator first1_s = std::next(first1, i);
        typename ForwardIt1::as_striped_iterator last1_s = std::next(last1, i);
        typename ForwardIt2::as_striped_iterator first2_s = std::next(first2, i);
        cilk_spawn transform_dispatch(&*first1_s,
            first1_s, last1_s,
            first2_s,
            unary_op,
            sequential_layout_tag()
        );
    }
}

// Parallel std::transform (binary op version) for sequential layouts
template< class ForwardIt1, class ForwardIt2, class ForwardIt3, class BinaryOperation >
void transform_dispatch( void * hint,
    ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2,
    ForwardIt3 first3,
    BinaryOperation binary_op,
    sequential_layout_tag)
{
    (void)hint;
    // TODO choose grain size
    const long grain = 4;
    for (;;) {
        /* How many elements in my range? */
        long count = std::distance(first1, last1);

        /* Break out when my range is smaller than the grain size */
        if (count <= grain) break;

        /* Divide the range in half */
        /* Invariant: count >= 2 */
        auto mid1 = std::next(first1, count/2);
        auto mid2 = std::next(first2, count/2);
        auto mid3 = std::next(first3, count/2);

        /* Spawn a thread to deal with the lower half */
        cilk_spawn transform_dispatch(&*first1,
            first1, mid1,
            first2,
            first3,
            binary_op,
            sequential_layout_tag()
        );

        /* Shrink range to upper half and repeat */
        first1 = mid1;
        first2 = mid2;
        first3 = mid3;
    }
    std::transform(first1, last1, first2, first3, binary_op);
}

// Parallel std::transform (binary op version) for striped layouts
template< class ForwardIt1, class ForwardIt2, class ForwardIt3, class BinaryOperation >
void transform_dispatch( void * hint,
                         ForwardIt1 first1, ForwardIt1 last1,
                         ForwardIt2 first2,
                         ForwardIt3 first3,
                         BinaryOperation binary_op,
                         striped_layout_tag)
{
    (void)hint;
    long size = std::distance(first1, last1);
    // Spawn a thread on each nodelet
    for (long i = 0; i < NODELETS() && i < size; ++i) {
        // Convert to striped iterators and forward to sequential version
        typename ForwardIt1::as_striped_iterator first1_s = std::next(first1, i);
        typename ForwardIt1::as_striped_iterator last1_s = std::next(last1, i);
        typename ForwardIt2::as_striped_iterator first2_s = std::next(first2, i);
        typename ForwardIt3::as_striped_iterator first3_s = std::next(first3, i);
        cilk_spawn transform_dispatch(&*first1_s,
            first1_s, last1_s,
            first2_s,
            first3_s,
            binary_op,
            sequential_layout_tag()
        );
    }
}

// Top-level dispatch functions

// Unary op version
template< class ForwardIt1, class ForwardIt2, class UnaryOperation >
ForwardIt2 transform( ForwardIt1 first1, ForwardIt1 last1,
                      ForwardIt2 d_first, UnaryOperation unary_op )
{
    mw_view(first1)
    transform_dispatch(&*first1, first1, last1, d_first, unary_op, layout);
    return d_first + std::distance(first1, last1);
}

// Binary op version
template< class ForwardIt1, class ForwardIt2, class ForwardIt3, class BinaryOperation >
ForwardIt3 transform( ForwardIt1 first1, ForwardIt1 last1,
                      ForwardIt2 first2,
                      ForwardIt3 d_first, BinaryOperation binary_op)
{
    // TODO what if the layouts don't match?
    typename iterator_layout<ForwardIt1>::value layout1;
    typename iterator_layout<ForwardIt2>::value layout2;
//    static_assert(std::is_same<layout1, layout2>::value, "First two sequences must have the same layout");

    transform_dispatch(&*first1, first1, last1, first2, d_first, binary_op);
    return d_first + std::distance(first1, last1);
}


} // end namespace parallel
} // end namespace emu
