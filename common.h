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

struct execution_policy {
    long radix;
    long grain;
    enum schedule_type {
        STATIC,
        DYNAMIC
    };
    schedule_type schedule;
};


template<typename F, typename... Args>
void
local_apply(long size, long grain, F worker, Args&&... args)
{
    // TODO Adjust grain to limit thread spawn

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

//template<typename T, typename F, typename... Args>
//void
//striped_array_apply(T * array, long size, long grain, F worker, Args&&... args)
//{
//    for (long i = 0; i < size; ++i) {
//        worker(i, std::forward<Args>(args)...);
//    }
//}


template<typename T, typename F, typename... Args>
void
striped_array_apply(T * array, long size, long grain, F worker, Args&&... args)
{
    static_assert(sizeof(T) == 8, "Striped array only handles 8-byte types!");

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
                    for (long i = begin; i < end; ++i) {
                        worker(i, std::forward<Args>(args)...);
                    }
                }(first, last, worker, std::forward<Args>(args)...);
            }
        }(array, size, grain, worker, std::forward<Args>(args)...);
    }
}

