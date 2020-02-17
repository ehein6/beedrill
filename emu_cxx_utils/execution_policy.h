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
template<long Grain>
struct grain_policy
{
    static constexpr long grain = Grain;
};

// Execute loop iterations one at a time, in a single thread
struct sequenced_policy {};
// Unroll and reorder statements in the innermost loop to minimize migrations
// at the cost of extra register pressure.
struct unroll_policy {};
// Spawn a thread for each grain-sized chunk
template<long Grain>
struct parallel_policy : public grain_policy<Grain> {};
// Create a thread for each execution slot, dividing iterations evenly
// May create fewer threads depending on grain size
template<long Grain>
struct static_policy : public grain_policy<Grain> {};
// Create N worker threads, which dynamically pull iterations off a work queue
template<long Grain>
struct dynamic_policy : public grain_policy<Grain> {};

// Unroll+reorder versions of the above
template<long Grain>
struct parallel_unroll_policy : public parallel_policy<Grain> {};
template<long Grain>
struct static_unroll_policy : public static_policy<Grain> {};
template<long Grain>
struct dynamic_unroll_policy : public dynamic_policy<Grain> {};

// Tags for downgrading a parallel policy to a serial policy
template<class Policy> struct remove_parallel;
template<class T> using remove_parallel_t = typename remove_parallel<T>::type;
template<long Grain> struct remove_parallel<parallel_policy<Grain>> {
    using type = sequenced_policy; };
template<long Grain> struct remove_parallel<parallel_unroll_policy<Grain>> {
    using type = unroll_policy; };
template<long Grain> struct remove_parallel<static_policy<Grain>> {
    using type = sequenced_policy; };
template<long Grain> struct remove_parallel<static_unroll_policy<Grain>> {
    using type = unroll_policy; };
template<long Grain> struct remove_parallel<dynamic_policy<Grain>> {
    using type = sequenced_policy; };
template<long Grain> struct remove_parallel<dynamic_unroll_policy<Grain>> {
    using type = unroll_policy; };

// Max number of elements to assign to each thread
constexpr long default_grain = 128;
// Max number of threads to spawn within a single thread
constexpr long spawn_radix = EMU_CXX_SPAWN_RADIX;
// Target number of threads per nodelet
constexpr long threads_per_nodelet = 64;

// Global tag objects for convenience, using default grain size
inline constexpr sequenced_policy                       seq          {};
inline constexpr parallel_policy<default_grain>         par          {};
inline constexpr static_policy<default_grain>           fixed        {};
inline constexpr dynamic_policy<1>                      dyn          {};
inline constexpr unroll_policy                          unroll       {};
inline constexpr parallel_unroll_policy<default_grain>  par_unroll   {};
inline constexpr static_unroll_policy<default_grain>    fixed_unroll {};
inline constexpr dynamic_unroll_policy<4>               dyn_unroll   {};

inline constexpr auto default_policy =    fixed;

// Traits for checking whether a tag is an execution policy
template<class T> struct is_execution_policy : std::false_type {};
template<class T>
inline constexpr bool is_execution_policy_v = is_execution_policy<T>::value;

template<> struct is_execution_policy<sequenced_policy> : std::true_type {};
template<long Grain> struct is_execution_policy<parallel_policy<Grain>> : std::true_type {};
template<long Grain> struct is_execution_policy<static_policy<Grain>> : std::true_type {};
template<long Grain> struct is_execution_policy<dynamic_policy<Grain>> : std::true_type {};
template<> struct is_execution_policy<unroll_policy> : std::true_type {};
template<long Grain> struct is_execution_policy<parallel_unroll_policy<Grain>> : std::true_type {};
template<long Grain> struct is_execution_policy<static_unroll_policy<Grain>> : std::true_type {};
template<long Grain> struct is_execution_policy<dynamic_unroll_policy<Grain>> : std::true_type {};

// Traits for checking whether a policy tag indicates parallel execution
template<class T> struct is_parallel_policy : std::false_type {};
template<class T>
inline constexpr bool is_parallel_policy_v = is_parallel_policy<T>::value;
template<long Grain> struct is_parallel_policy<parallel_policy<Grain>> : std::true_type {};
template<long Grain> struct is_parallel_policy<parallel_unroll_policy<Grain>> : std::true_type {};

// Traits for checking whether a policy tag has a static schedule
template<class T> struct is_static_policy : std::false_type {};
template<class T>
inline constexpr bool is_static_policy_v = is_static_policy<T>::value;
template<long Grain> struct is_static_policy<static_policy<Grain>> : std::true_type {};
template<long Grain> struct is_static_policy<static_unroll_policy<Grain>> : std::true_type {};

// Traits for checking whether a policy tag has a dynamic schedule
template<class T> struct is_dynamic_policy : std::false_type {};
template<class T>
inline constexpr bool is_dynamic_policy_v = is_dynamic_policy<T>::value;
template<long Grain> struct is_dynamic_policy<dynamic_policy<Grain>> : std::true_type {};
template<long Grain> struct is_dynamic_policy<dynamic_unroll_policy<Grain>> : std::true_type {};

// Adjusts the grain size so we don't spawn too many threads
template<class Policy, class Iterator>
inline long
compute_fixed_grain(Policy policy, Iterator begin, Iterator end)
{
    // Calculate a fixed grain size so we spawn exactly enough threads
    long max_threads = threads_per_nodelet;
    long n = std::distance(begin, end);
    long grain = Policy::grain;
    long n_threads = n / grain;
    if (n_threads > max_threads) {
        grain = n / max_threads;
    }
    return grain;
}

} // end namespace emu