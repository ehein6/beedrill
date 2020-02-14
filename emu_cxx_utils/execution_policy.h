#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include "pointer_manipulation.h"

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

namespace emu {

// Base class for policies that take a grain size
struct grain_policy
{
    long grain_;
    explicit constexpr grain_policy(long grain) : grain_(grain) {}
};

// Execute loop iterations one at a time, in a single thread
struct sequenced_policy {};
// Unroll and reorder statements in the innermost loop to minimize migrations
// at the cost of extra register pressure.
struct unroll_policy {};
// Spawn a thread for each grain-sized chunk
struct parallel_policy : public grain_policy
{ using grain_policy::grain_policy; };
// Create a thread for each execution slot, dividing iterations evenly
// May create fewer threads depending on grain size
struct static_policy : public grain_policy
{ using grain_policy::grain_policy; };
// Create N worker threads, which dynamically pull iterations off a work queue
struct dynamic_policy : public grain_policy
{ using grain_policy::grain_policy; };

// Unroll+reorder versions of the above
struct parallel_unroll_policy : public parallel_policy
{ using parallel_policy::parallel_policy; };
struct static_unroll_policy : public static_policy
{ using static_policy::static_policy; };
struct dynamic_unroll_policy : public dynamic_policy
{ using dynamic_policy::dynamic_policy; };

// Tags for downgrading a parallel policy to a serial policy
template<class Policy> struct remove_parallel;
template<class T> using remove_parallel_t = typename remove_parallel<T>::type;
template<> struct remove_parallel<parallel_policy> {
    using type = sequenced_policy; };
template<> struct remove_parallel<parallel_unroll_policy> {
    using type = unroll_policy; };
template<> struct remove_parallel<dynamic_policy> {
    using type = sequenced_policy; };
template<> struct remove_parallel<dynamic_unroll_policy> {
    using type = unroll_policy; };

// Tags for converting a static policy to a parallel policy
template<class Policy> struct remove_static;
template<class T> using remove_static_t = typename remove_static<T>::type;
template<> struct remove_static<static_policy> {
    using type = parallel_policy; };
template<> struct remove_static<static_unroll_policy> {
    using type = unroll_policy; };

// Max number of elements to assign to each thread
constexpr long default_grain = 128;
// Max number of threads to spawn within a single thread
constexpr long spawn_radix = EMU_CXX_SPAWN_RADIX;
// Target number of threads per nodelet
constexpr long threads_per_nodelet = 64;

// Global tag objects for convenience, using default grain size
inline constexpr sequenced_policy         seq          {};
inline constexpr parallel_policy          par          {default_grain};
inline constexpr static_policy            fixed        {default_grain};
inline constexpr dynamic_policy           dyn          {1};
inline constexpr unroll_policy            unroll       {};
inline constexpr parallel_unroll_policy   par_unroll   {default_grain};
inline constexpr static_unroll_policy     fixed_unroll {default_grain};
inline constexpr dynamic_unroll_policy    dyn_unroll   {4};

inline constexpr auto default_policy =    fixed;

// Traits for checking whether a tag is an execution policy
template<class T> struct is_execution_policy : std::false_type {};
template<class T>
inline constexpr bool is_execution_policy_v = is_execution_policy<T>::value;

template<> struct is_execution_policy<sequenced_policy> : std::true_type {};
template<> struct is_execution_policy<parallel_policy> : std::true_type {};
template<> struct is_execution_policy<static_policy> : std::true_type {};
template<> struct is_execution_policy<dynamic_policy> : std::true_type {};
template<> struct is_execution_policy<unroll_policy> : std::true_type {};
template<> struct is_execution_policy<parallel_unroll_policy> : std::true_type {};
template<> struct is_execution_policy<static_unroll_policy> : std::true_type {};
template<> struct is_execution_policy<dynamic_unroll_policy> : std::true_type {};

// Traits for checking whether a policy tag indicates parallel execution
template<class T> struct is_parallel_policy : std::false_type {};
template<class T>
inline constexpr bool is_parallel_policy_v = is_parallel_policy<T>::value;
template<> struct is_parallel_policy<parallel_policy> : std::true_type {};
template<> struct is_parallel_policy<parallel_unroll_policy> : std::true_type {};

// Traits for checking whether a policy tag has a static schedule
template<class T> struct is_static_policy : std::false_type {};
template<class T>
inline constexpr bool is_static_policy_v = is_static_policy<T>::value;
template<> struct is_static_policy<static_policy> : std::true_type {};
template<> struct is_static_policy<static_unroll_policy> : std::true_type {};

// Traits for checking whether a policy tag has a dynamic schedule
template<class T> struct is_dynamic_policy : std::false_type {};
template<class T>
inline constexpr bool is_dynamic_policy_v = is_dynamic_policy<T>::value;
template<> struct is_dynamic_policy<dynamic_policy> : std::true_type {};
template<> struct is_dynamic_policy<dynamic_unroll_policy> : std::true_type {};

// Adjusts the grain size so we don't spawn too many threads
template<class Policy, class Iterator>
inline remove_static_t<Policy>
compute_fixed_grain(Policy policy, Iterator begin, Iterator end)
{
    // Calculate a fixed grain size so we spawn exactly enough threads
    long max_threads = threads_per_nodelet;
    long n = end - begin;
    long grain = policy.grain_;
    long n_threads = n / grain;
    if (n_threads > max_threads) {
        grain = n / max_threads;
    }
    // Convert to a parallel execution policy that uses the computed grain size
    return remove_static_t<Policy>(grain);
}

} // end namespace emu