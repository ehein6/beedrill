#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include "pointer_manipulation.h"
#include "stride_iterator.h"

#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/fusion/adapted/std_tuple.hpp>

#ifndef EMU_CXX_SPAWN_RADIX
#define EMU_CXX_SPAWN_RADIX 16
#endif

// TODO move this to a new header
namespace emu {

template<class Iterator>
void *
ptr_from_iter(Iterator iter);

// Raw pointer: this is the base case, return the pointer
template<class T>
void *
ptr_from_iter(T* ptr)
{
    return ptr;
}

// For any other iterator type,
// Assume that the first member of the iterator is a pointer
// This isn't safe, but should work most of the time:
// - Most iterators are just wrapping a raw pointer
// - Zip iterator wraps multiple pointers, this grabs the first one
// - For transform iterator, this will give us the pointer to the thing the
//   functor will be called upon
template<class Iterator>
void*
ptr_from_iter(Iterator iter)
{
    return *reinterpret_cast<void**>(&iter);
}


template<class Iterator>
bool is_striped(Iterator iter)
{
    return pmanip::is_striped(ptr_from_iter(iter));
}

}


namespace emu::execution {



struct policy_base {};

// Execute loop iterations one at a time, in a single thread
struct sequenced_policy : public policy_base {};
// Create N worker threads, which dynamically pull loop iterations off a work queue
struct parallel_dynamic_policy : public policy_base {};

// Base class for policies that take a grain size
struct grain_policy : public policy_base
{
    long grain_;
    explicit constexpr grain_policy(long grain) : grain_(grain) {}
    void operator()(long grain) { grain_ = grain; }
};

// Spawn a thread for each grain-sized chunk
struct parallel_policy : public grain_policy
{ using grain_policy::grain_policy; };
// Create a thread for each execution slot, dividing iterations evenly
// May create fewer threads depending on grain size
struct parallel_fixed_policy : public grain_policy
{ using grain_policy::grain_policy; };

// Max number of elements to assign to each thread
constexpr long default_grain = 128;
// Max number of threads to spawn within a single thread
constexpr long spawn_radix = EMU_CXX_SPAWN_RADIX;
// Target number of threads per nodelet
constexpr long threads_per_nodelet = 64;

// Global tag objects for selecting a grain size
inline constexpr sequenced_policy           seq    {};
inline constexpr parallel_policy            par    {default_grain};
inline constexpr parallel_fixed_policy      fixed  {default_grain};
inline constexpr parallel_dynamic_policy    dyn    {};

inline constexpr auto default_policy = fixed;

// Traits for checking whether an argument is an execution policy
template<class T>
struct is_execution_policy : std::false_type {};
template<> struct is_execution_policy<sequenced_policy> : std::true_type {};
template<> struct is_execution_policy<parallel_policy> : std::true_type {};
template<> struct is_execution_policy<parallel_fixed_policy> : std::true_type {};
template<> struct is_execution_policy<parallel_dynamic_policy> : std::true_type {};

template<class T>
inline constexpr bool is_execution_policy_v = is_execution_policy<T>::value;

// Adjust grain size so we don't spawn too many threads
template<class Iterator>
inline parallel_policy
compute_fixed_grain(parallel_fixed_policy policy, Iterator begin, Iterator end)
{
    // Figure out if the iterator points to a distributed data structure
    long max_threads = threads_per_nodelet;
    if (is_striped(begin)) {
        max_threads *= NODELETS();
    }
    // Calculate a fixed grain size so we spawn exactly enough threads
    long n = end - begin;
    long grain = policy.grain_;
    long n_threads = n / grain;
    if (n_threads > max_threads) {
        grain = n / max_threads;
    }
    return parallel_policy(grain);
}




} // end namespace emu::execution