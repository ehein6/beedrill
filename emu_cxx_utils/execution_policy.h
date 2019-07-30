#pragma once

namespace emu {
namespace execution {

struct policy_base {};

// Execute loop iterations one at a time, in a single thread
struct sequenced_policy : public policy_base {};

// Base class for policies that take a grain size
struct grain_policy : public policy_base
{
    long grain_;
    explicit constexpr grain_policy(long grain) : grain_(grain) {}
};

// Spawn a thread for each grain-sized chunk
struct parallel_policy : public grain_policy { using grain_policy::grain_policy; };
// Create a thread for each execution slot, dividing iterations evenly
// May create fewer threads depending on grain size
struct parallel_limited_policy : public grain_policy { using grain_policy::grain_policy; };
// Create N worker threads, which dynamically pull loop iterations off a work queue
struct parallel_dynamic_policy : public policy_base {};

inline constexpr sequenced_policy seq{};
inline constexpr parallel_policy par{512};
inline constexpr parallel_limited_policy par_limit{512};
inline constexpr parallel_dynamic_policy par_dyn{};

// Max number of threads to spawn within a single thread
constexpr long spawn_radix = 8;
// Target number of threads per nodelet
constexpr long threads_per_nodelet = 64;

// Adjust grain size so we don't spawn too many threads
inline long limit_grain(long grain, long n, long max_threads)
{
    long n_threads = n / grain;
    if (n_threads > max_threads) {
        grain = n / max_threads;
    }
    return grain;
}

} // end namespace emu::execution
} // end namespace emu