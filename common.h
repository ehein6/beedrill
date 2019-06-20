#pragma once

// Helper functions

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <emu_c_utils/emu_c_utils.h>
#include <cilk/cilk.h>

// HACK so we can compile with old toolchain
#ifndef cilk_spawn_at
#define cilk_spawn_at(X) cilk_spawn
#endif

// Logging macro. Flush right away since Emu hardware usually doesn't
#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout);

namespace emu {
namespace execution {

struct policy_base {};

// Execute loop iterations one at a time, in a single thread
struct sequenced_policy : public policy_base {};

// Base class for policies that take a grain size
struct grain_policy : public policy_base
{
    long grain_;
    explicit grain_policy(long grain) : grain_(grain) {}
};

// Spawn a thread for each grain-sized chunk
struct parallel_policy : public grain_policy { using grain_policy::grain_policy; };
// Create a thread for each execution slot, dividing iterations evenly
// May create fewer threads depending on grain size
struct parallel_limited_policy : public grain_policy { using grain_policy::grain_policy; };
// Create N worker threads, which dynamically pull loop iterations off a work queue
struct parallel_dynamic_policy : public grain_policy { using grain_policy::grain_policy; };

// Max number of threads to spawn within a single thread
const long spawn_radix = 8;
// Target number of threads per nodelet
const long threads_per_nodelet = 64;

}; // end namespace execution
}; // end namespace emu

// Adjust grain size so we don't spawn too many threads
inline long limit_grain(long grain, long n, long max_threads)
{
    long n_threads = n / grain;
    if (n_threads > max_threads) {
        grain = n / max_threads;
    }
    return grain;
}

// Sequential version
template<typename F, typename... Args>
void
local_apply(
    emu::execution::sequenced_policy,
    long size, F worker, Args&&... args
){
    for (long i = 0; i < size; ++i) {
        worker(i, std::forward<Args>(args)...);
    }
}

// Limited parallel version
template<typename F, typename... Args>
void
local_apply(
    emu::execution::parallel_limited_policy policy,
    long size, F worker, Args&&... args
){
    long grain = limit_grain(policy.grain_, size, emu::execution::threads_per_nodelet);
    // Forward to unlimited parallel version
    local_apply(
        emu::execution::parallel_policy(grain),
        size, worker, std::forward<Args>(args)...
    );
}

// Parallel version
template<typename F, typename... Args>
void
local_apply(
    emu::execution::parallel_policy policy,
    long size, F worker, Args&&... args
){
    long grain = policy.grain_;
    auto apply_worker = [](long begin, long end, F worker, Args&&... args) {
        for (long i = begin; i < end; ++i) {
            worker(i, std::forward<Args>(args)...);
        }
    };
    if (size < grain) {
        apply_worker(0, size, worker, std::forward<Args>(args)...);
    } else {
        for (long i = 0; i < size; i += grain) {
            long begin = i;
            long end = begin + grain <= size ? begin + grain : size;
            cilk_spawn apply_worker(begin, end, worker, std::forward<Args>(args)...);
        }
    }
}

// TODO: Parallel with dynamic work queue

//template<typename T, typename F, typename... Args>
//void
//striped_array_apply(execution_policy& policy, T * array, long size, F worker, Args... args)
//{
//    static_assert(sizeof(T) == 8, "Striped array only handles 8-byte types!");
//
//    for (;;) {
//        // Keep looping until there are few enough iterations to spawn serially
//        if (size/policy.grain <= policy.radix) break;
//
//        // Cut the range in half
//        long lower_size = size/2;
//        long upper_size = size - lower_size;
//
//        /* Spawn a thread to deal with the upper half */
//        cilk_spawn_at(array[lower_size]) striped_array_apply(
//            policy, array + lower_size, upper_size, worker, args...
//        );
//
//        /* Shrink range to lower half and repeat */
//        size = lower_size;
//    }
//
//    // Spawn a thread on each nodelet
//    for (long i = 0; i < NODELETS() && i < size; ++i) {
//        cilk_spawn_at(&array[i]) [](T * array, long size, long grain, F worker, Args... args) {
//            const long nodelet = NODE_ID();
//            const long stride = grain*NODELETS();
//            // Spawn threads to handle all the elements on this nodelet
//            // Start with an offset of NODE_ID() and a stride of NODELETS()
//            for (long i = nodelet; i < size; i += stride) {
//                long first = i;
//                long last = first + stride; if (last > size) { last = size; }
//                cilk_spawn [](long begin, long end, F worker, Args... args) {
//                    for (long i = begin; i < end; ++i) {
//                        worker(i, args...);
//                    }
//                }(first, last, worker, args...);
//            }
//        }(array, size, grain, worker, args...);
//    }
//}

// Sequential implementation
template<typename T, typename F, typename... Args>
void
striped_array_apply(
    emu::execution::sequenced_policy,
    T * array, long size, F worker, Args&&... args
){
    for (long i = 0; i < size; ++i) {
        worker(i, std::forward<Args>(args)...);
    }
}

// Limited parallel version
template<typename T, typename F, typename... Args>
void
striped_array_apply(
    emu::execution::parallel_limited_policy policy,
    T * array, long size, F worker, Args&&... args
){
    long grain = limit_grain(policy.grain_, size, NODELETS() * emu::execution::threads_per_nodelet);
    striped_array_apply(
        emu::execution::parallel_policy(grain),
        array, size, worker, std::forward<Args>(args)...
    );
}

// Parallel implementation
template<typename T, typename F, typename... Args>
void
striped_array_apply(
    emu::execution::parallel_policy policy,
    T * array, long size, F worker, Args&&... args
){
    static_assert(sizeof(T) == 8, "Striped array only handles 8-byte types!");
    long grain = policy.grain_;
    // Spawn a thread on each nodelet
    for (long i = 0; i < NODELETS() && i < size; ++i) {
        cilk_spawn_at(&array[i]) [](T * array, long size, long grain, F worker, Args&&... args) {
            const long nodelet = NODE_ID();
            const long stride = grain*NODELETS();
            // Spawn threads to handle all the elements on this nodelet
            // Start with an offset of NODE_ID() and a stride of NODELETS()
            for (long i = nodelet; i < size; i += stride) {
                long first = i;
                long last = first + stride; if (last > size) { last = size; }
                cilk_spawn [](long begin, long end, F worker, Args&&... args) {
                    for (long i = begin; i < end; i += NODELETS()) {
                        worker(i, std::forward<Args>(args)...);
                    }
                }(first, last, worker, std::forward<Args>(args)...);
            }
        }(array, size, grain, worker, std::forward<Args>(args)...);
    }
}

