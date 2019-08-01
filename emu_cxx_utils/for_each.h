//#pragma once
//
//#include "execution_policy.h"
//
//namespace emu::parallel {
//
//// Serial version
//template<typename Iterator, typename UnaryFunction>
//void
//for_each(
//    emu::execution::sequenced_policy,
//    Iterator begin, Iterator end, UnaryFunction worker
//){
//    for (Iterator i = begin; i != end; ++i) {
//        worker(*i);
//    }
//}
//
//// Parallel version
//template<typename Iterator, typename UnaryFunction>
//void
//for_each(
//    emu::execution::parallel_policy policy,
//    Iterator begin, Iterator end, UnaryFunction worker
//){
//    auto grain = policy.grain_;
//    auto radix = emu::execution::spawn_radix;
//    typename std::iterator_traits<Iterator>::difference_type size;
//
//    // Recursive spawn
//    for (;;) {
//        // Keep looping until there are few enough iterations to spawn serially
//        size = end - begin;
//        if (size/grain <= radix) break;
//        // Cut the range in half
//        Iterator mid = begin + size/2;
//        // Spawn a thread to deal with the upper half
//        cilk_spawn for_each(
//            policy, mid, end, worker
//        );
//        // Shrink range to lower half and repeat
//        end = mid;
//    }
//    if (size > grain) {
//        // Serial spawn
//        for (Iterator first = begin; first < end; first += grain) {
//            Iterator last = first + grain <= end ? first + grain : end;
//            cilk_spawn for_each(
//                execution::seq,
//                first, last, worker
//            );
//        }
//    } else {
//        // Serial execution
//        for_each(
//            execution::seq,
//            begin, end, worker
//        );
//    }
//}
//
//
//// Limited parallel version
//template<typename Iterator, typename UnaryFunction>
//void
//for_each(
//    emu::execution::parallel_limited_policy policy,
//    Iterator begin, Iterator end, UnaryFunction worker
//){
//    // Recalculate grain size to limit thread count
//    auto grain = emu::execution::limit_grain(
//        policy.grain_,
//        end-begin,
//        emu::execution::threads_per_nodelet
//    );
//    // Forward to unlimited parallel version
//    for_each(
//        emu::execution::parallel_policy(grain),
//        begin, end, worker
//    );
//}
//
//// Default, forwards to parallel limited impl
//template<typename Iterator, typename UnaryFunction>
//void
//for_each(
//    Iterator begin, Iterator end, UnaryFunction worker
//){
//    emu::parallel::for_each(emu::execution::par_limit, begin, end, worker);
//}
//
//
//} // end namespace emu::parallel