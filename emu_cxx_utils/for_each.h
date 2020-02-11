#pragma once

// #include "striped_for_each.h"

#include <algorithm>
#include <cilk/cilk.h>
#include "execution_policy.h"
#include "nlet_stride_iterator.h"
#include "intrinsics.h"

namespace emu::parallel {
namespace detail {

// Serial version
template<class Iterator, class UnaryFunction>
void
for_each(
   sequenced_policy,
   Iterator begin, Iterator end, UnaryFunction worker
) {
   // Forward to standard library implementation
   std::for_each(begin, end, worker);
}

// Parallel version
template<class Iterator, class UnaryFunction>
void
for_each(
   parallel_policy policy,
   Iterator begin, Iterator end,
   UnaryFunction worker
) {
   // Serial spawn over each granule
   auto grain = policy.grain_;
   for (; begin < end; begin += grain) {
       // Spawn a thread to handle each granule
       // Last iteration may be smaller if things don't divide evenly
       auto last = begin + grain <= end ? begin + grain : end;
       cilk_spawn_at(ptr_from_iter(begin)) for_each(
           seq,
           begin, last, worker
       );
   }
}

// Parallel fixed version
template<class Iterator, class UnaryFunction>
void
for_each(
   parallel_fixed_policy policy,
   Iterator begin, Iterator end, UnaryFunction worker
) {
   // Recalculate grain size to limit thread count
   // and forward to unlimited parallel version
   for_each(
       compute_fixed_grain(policy, begin, end),
       begin, end, worker
   );
}

// Parallel dynamic version
// Assumes that the iterators are raw pointers that can be atomically advanced
template<class T, class UnaryFunction>
void
for_each(
   parallel_dynamic_policy,
   T* begin, T* end,
   UnaryFunction worker
) {
   // Shared pointer to the next item to process
   T * next = begin;
   // Create a worker thread for each execution slot
   for (long t = 0; t < threads_per_nodelet; ++t) {
       cilk_spawn [&](){
           // Atomically grab the next item off the list
           for (T * item = atomic_addms(&next, 1);
                item < end;
                item = atomic_addms(&next, 1))
           {
               // Call the worker function on the item
               worker(*item);
           }
       }();
   }
}

// Parallel dynamic version
// Special case for when our iterator type is a stride iterator
// wrapping a raw pointer
template<class T, class UnaryFunction>
void
for_each(
   parallel_dynamic_policy,
   nlet_stride_iterator<T*> s_begin, nlet_stride_iterator<T*> s_end,
   UnaryFunction worker
) {
   // Shared pointer to the next item to process
   T * next = &*s_begin;
   T * end = &*s_end;
   // Create a worker thread for each execution slot
   for (long t = 0; t < threads_per_nodelet; ++t) {
       cilk_spawn [&](){
           // Atomically grab the next item off the list
           for (T * item = atomic_addms(&next, NODELETS());
                item < end;
                item = atomic_addms(&next, NODELETS()))
           {
               // Call the worker function on the item
               worker(*item);
           }
       }();
   }
}

// Serial version for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
   sequenced_policy,
   Iterator begin, Iterator end, UnaryFunction worker
) {
   // Forward to standard library implementation
   // TODO we could save migrations by doing it one stripe at a time
   std::for_each(begin, end, worker);
}

// Parallel version for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
   parallel_policy policy,
   Iterator begin, Iterator end,
   UnaryFunction worker
) {
   // Total number of elements
   auto size = end - begin;
   // Number of elements in each range
   auto stripe_size = size / NODELETS();
   // How many nodelets have an extra element?
   auto stripe_remainder = size % NODELETS();
   // Spawn a thread on each nodelet:
   for (long nlet = 0; nlet < NODELETS(); ++nlet) {
       // 1. Convert from iterator to raw pointer
       // 2. Advance to the first element on the nth nodelet
       // 3. Convert to striped iterator
       auto stripe_begin = nlet_stride_iterator<Iterator>(begin + nlet);
       // Now that the pointer has stride NODELETS(), we are addressing only
       // the elements on the nth nodelet
       auto stripe_end = stripe_begin + stripe_size;
       // Distribute remainder among first few nodelets
       if (nlet < stripe_remainder) { stripe_end += 1; }
       // Spawn a thread to handle each stripe
       cilk_spawn_at(ptr_from_iter(stripe_begin)) for_each(
           policy, stripe_begin, stripe_end, worker
       );
   }
}

// Parallel dynamic version for striped layouts
// FIXME copy-pasted from above, need to think about how to
// FIXME do tag dispatch so we can combine these functions
//
// In this case, we spawn a thread at each nodelet
// and then create local worker threads which grab iterations
// from the local nodelet. Could be further optimized to allow
// stealing work from other nodelets once all the local iterations
// have been processed.
template<class Iterator, class UnaryFunction>
void
striped_for_each(
   parallel_dynamic_policy policy,
   Iterator begin, Iterator end,
   UnaryFunction worker
) {
   // Total number of elements
   auto size = end - begin;
   // Number of elements in each range
   auto stripe_size = size / NODELETS();
   // How many nodelets have an extra element?
   auto stripe_remainder = size % NODELETS();
   auto max_nlet = NODELETS();
   // We may not spawn a thread on each nodelet if the range is short
   if (size < max_nlet) {
       max_nlet = size;
   }
   // Spawn a thread on each nodelet:
   for (long nlet = 0; nlet < max_nlet; ++nlet) {
       // 1. Convert from iterator to raw pointer
       // 2. Advance to the first element on the nth nodelet
       // 3. Convert to striped iterator
       auto stripe_begin = nlet_stride_iterator<Iterator>(&*(begin) + nlet);
       // Now that the pointer has stride NODELETS(), we are addressing only
       // the elements on the nth nodelet
       auto stripe_end = stripe_begin + stripe_size;
       // Distribute remainder among first few nodelets
       if (nlet < stripe_remainder) { stripe_end += 1; }
       // Spawn a thread to handle each stripe
       cilk_spawn_at(ptr_from_iter(stripe_begin)) for_each(
           policy, stripe_begin, stripe_end, worker
       );
   }
}

// Parallel fixed for striped layouts
template<class Iterator, class UnaryFunction>
void
striped_for_each(
   parallel_fixed_policy policy,
   Iterator begin, Iterator end, UnaryFunction worker
) {
   // Recalculate grain size to limit thread count
   // and forward to unlimited parallel version
   striped_for_each(
       compute_fixed_grain(policy, begin, end),
       begin, end, worker
   );
}

} // end namespace detail


template<class ExecutionPolicy, class Iterator, class UnaryFunction,
   // Disable if first argument is not an execution policy
   std::enable_if_t<is_execution_policy_v<ExecutionPolicy>, int> = 0
>
void
for_each(
   ExecutionPolicy policy,
   Iterator begin, Iterator end, UnaryFunction worker
){
   if (end-begin == 0) {
       return;
   } else if (is_striped(begin)) {
       detail::striped_for_each(policy, begin, end, worker);
   } else {
       detail::for_each(policy, begin, end, worker);
   }
}

template<class Iterator, class UnaryFunction>
void
for_each(Iterator begin, Iterator end, UnaryFunction worker
){
   for_each(emu::default_policy, begin, end, worker);
}

} // end namespace emu::parallel
