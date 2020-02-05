#pragma once

#include "execution_policy.h"

namespace emu {
namespace parallel {

template<typename Index, typename Function, typename... Args>
void
for_each_i(
    emu::sequenced_policy,
    Index begin, Index end, Function worker, Args&&... args
){
    for (Index i = begin; i != end; ++i) {
        worker(i, std::forward<Args>(args)...);
    }
}

// Parallel version
template<typename Index, typename Function, typename... Args>
void
for_each_i(
    emu::parallel_policy policy,
    Index begin, Index end, Function worker, Args&&... args
){
    auto grain = policy.grain_;
    auto radix = emu::spawn_radix;
    Index size;

    // Recursive spawn
    for (;;) {
        // Keep looping until there are few enough iterations to spawn serially
        size = end - begin;
        if (size/grain <= radix) break;
        // Cut the range in half
        Index mid = size/2;
        // Spawn a thread to deal with the upper half
        cilk_spawn for_each_i(
            policy, mid, end, worker, std::forward<Args>(args)...
        );
        // Shrink range to lower half and repeat
        end = mid;
    }
    if (size > grain) {
        // Serial spawn
        for (Index first = begin; first < end; first += grain) {
            long last = first + grain <= end ? first + grain : end;
            cilk_spawn for_each_i(
                sequenced_policy(),
                first, last, worker, std::forward<Args>(args)...
            );
        }
    } else {
        // Serial execution
        for_each_i(
            sequenced_policy(),
            begin, end, worker, std::forward<Args>(args)...
        );
    }
}

// Limited parallel version
template<typename Index, typename Function, typename... Args>
void
for_each_i(
    emu::parallel_limited_policy policy,
    Index begin, Index end, Function worker, Args&&... args
){
    // Recalculate grain size to limit thread count
    auto grain = emu::limit_grain(
        policy.grain_,
        end-begin,
        emu::threads_per_nodelet
    );
    // Forward to unlimited parallel version
    for_each_i(
        emu::parallel_policy(grain),
        begin, end, worker, std::forward<Args>(args)...
    );
}

// Dynamic parallel version
template<typename Index, typename Function, typename... Args>
void
for_each_i(
    emu::parallel_dynamic_policy policy,
    Index begin, Index end, Function worker, Args&&... args
){
    long next_i = begin;
    // Spawn worker threads
    // TODO do recursive spawn according to radix parameter
    for (long t = 0; t < threads_per_nodelet; ++t) {
        cilk_spawn dynamic_worker(
            next_i, end, worker, std::forward<Args>(args)...
        );
    }
}

template<typename UnaryFunction>
static void
dynamic_worker(long & next_i, long end, UnaryFunction worker)
{
    // TODO support grain size > 1
    for (long i = ATOMIC_ADDMS(&next_i, 1); i < end; i = ATOMIC_ADDMS(&next_i, 1)) {
        worker(i);
    }
}


} // end namespace emu::parallel
} // end namespace emu