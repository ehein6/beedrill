#pragma once

#include "execution_policy.h"

namespace emu {
namespace parallel {

template<typename Index, typename Function, typename... Args>
void
striped_for_each_i(
    emu::execution::sequenced_policy,
    Index begin, Index end, Function worker, Args&&... args
){
    for (Index i = begin; i != end; ++i) {
        worker(i, std::forward<Args>(args)...);
    }
}

template<typename Iterator, typename Function, typename... Args>
void
striped_for_each(
    emu::execution::sequenced_policy,
    Iterator begin, Iterator end, Function worker, Args&&... args
){
    for (Iterator i = begin; i != end; ++i) {
        worker(*i, std::forward<Args>(args)...);
    }
}

// Parallel version
template<typename T, typename Index, typename Function, typename... Args>
void
striped_for_each_i(
    emu::execution::parallel_policy policy,
    T * array, Index begin, Index end, Function worker, Args&&... args
){
}

template<typename T, typename Index, typename Function, typename... Args>
void
striped_for_each_i(
    emu::execution::parallel_limited_policy policy,
    T * array, Index begin, Index end, Function worker, Args&&... args
){
}


// Parallel version
template<typename T, typename Function, typename... Args>
void
striped_for_each(
    emu::execution::parallel_policy policy,
    T * begin, T * end, Function worker, Args&&... args
){
}

// Parallel version
template<typename T, typename Function, typename... Args>
void
striped_for_each(
    emu::execution::parallel_limited_policy policy,
    T * begin, T * end, Function worker, Args&&... args
){
}




} // end namespace emu::parallel
} // end namespace emu