#pragma once

#include <algorithm>
#include <numeric>
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

//
//// Generic std::reduce
//template<class InputIt, class T, class BinaryOp, class Layout>
//T
//reduce_dispatch(void * hint, InputIt first, InputIt last, T init, BinaryOp binary_op, Layout)
//{
//    (void)hint;
//    // TODO choose grain size
//    const long grain = 4;
//    for (;;) {
//        /* How many elements in my range? */
//        long count = std::distance(first, last);
//
//        /* Break out when my range is smaller than the grain size */
//        if (count <= grain) break;
//
//        /* Divide the range in half */
//        /* Invariant: count >= 2 */
//        auto mid = std::next(first, count/2);
//
//        /* Spawn a thread to deal with the lower half */
//        init = binary_op(
//            init,
//            reduce_dispatch(&*first, first, mid, init, binary_op, sequential_layout_tag())
//        );
//
//        /* Shrink range to upper half and repeat */
//        first = mid;
//    }
//    return std::accumulate(first, last, init, binary_op);
//}


struct remote_add { void operator() (long& lhs, const long& rhs) { REMOTE_ADD(&lhs, rhs); }};
struct remote_and { void operator() (long& lhs, const long& rhs) { REMOTE_AND(&lhs, rhs); }};
struct remote_or  { void operator() (long& lhs, const long& rhs) { REMOTE_OR (&lhs, rhs); }};
struct remote_xor { void operator() (long& lhs, const long& rhs) { REMOTE_XOR(&lhs, rhs); }};
struct remote_min { void operator() (long& lhs, const long& rhs) { REMOTE_MIN(&lhs, rhs); }};
struct remote_max { void operator() (long& lhs, const long& rhs) { REMOTE_MAX(&lhs, rhs); }};

template<typename F>
struct remote_op {};

template<> struct remote_op<std::plus<long>> : remote_add {};
template<> struct remote_op<std::bit_and<long>> : remote_and {};
template<> struct remote_op<std::bit_or<long>> : remote_or {};
template<> struct remote_op<std::bit_xor<long>> : remote_xor {};

// Problem: there are no std functors for min and max. They might even be macros.
// What will binary_op be if the programmer wants a max/min reduction?
// Probably they should use std::max_element or std::min_element instead


// Specialized for summing up longs
template<class InputIt, class BinaryOp>
void
reduce_with_remotes(void * hint, InputIt first, InputIt last, long& parent_sum, BinaryOp binary_op, sequential_layout_tag)
{
    (void)hint;
    // TODO choose grain size
    const long grain = 4;
    long children_sum = 0;
    for (;;) {
        /* How many elements in my range? */
        long count = std::distance(first, last);

        /* Break out when my range is smaller than the grain size */
        if (count <= grain) break;

        /* Divide the range in half */
        /* Invariant: count >= 2 */
        auto mid = std::next(first, count/2);

        /* Spawn a thread to deal with the lower half */
        cilk_spawn reduce_with_remotes(&*first,
            first, mid,
            children_sum, binary_op,
            sequential_layout_tag()
        );

        /* Shrink range to upper half and repeat */
        first = mid;
    }
    // This thread is responsible for the sub-range (first, last)
    long my_sum = std::accumulate(first, last, 0, binary_op);
    // Wait for children to finish
    cilk_sync;
    // Add to parent's sum
    remote_op<BinaryOp>()(parent_sum, binary_op(children_sum, my_sum));
}

// Specialization for long type
template<class InputIt, class T, class BinaryOp, class Layout>
typename std::enable_if<
    std::is_same<long, typename std::iterator_traits<InputIt>::value_type>::value, T
>::type
reduce_dispatch(void * hint, InputIt first, InputIt last, T init, BinaryOp binary_op, Layout layout)
{
    long sum = init;
    reduce_with_remotes(&*first, first, last, sum, binary_op, layout);
    return sum;
}


// Top-level dispatch functions
template<class InputIt, class T, class BinaryOp>
T
reduce(InputIt first, InputIt last, T init, BinaryOp binary_op)
{
    typename iterator_layout<InputIt>::value layout;
    return reduce_dispatch(&*first, first, last, init, binary_op, layout);
}

template<class InputIt, class T>
T
reduce(InputIt first, InputIt last, T init)
{
    return reduce(first, last, init, std::plus<typename std::iterator_traits<InputIt>::value_type>());
}

template<class InputIt>
typename std::iterator_traits<InputIt>::value_type
reduce(InputIt first, InputIt last)
{
    return reduce(first, last, typename std::iterator_traits<InputIt>::value_type{});
}


} // end namespace parallel
} // end namespace emu
