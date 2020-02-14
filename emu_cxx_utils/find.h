#pragma once

#include <emu_cxx_utils/for_each.h>
namespace emu::parallel {
namespace detail {

// Serial version
template<class InputIt, class T>
InputIt find(sequenced_policy, InputIt first, InputIt last, const T &value) {
    return std::find(first, last, value);
}

// Unrolled version
template<typename Iterator, typename Function>
class unroll_p {
private:
    Function worker_;
public:
    explicit unroll_p(Function worker) : worker_(worker) {}
    Iterator
    operator()(Iterator begin, Iterator end) {
        // Visit elements one at a time until remainder is evenly divisible by four
        while (std::distance(begin, end) % 4 != 0) {
            if (worker_(*begin++)) { return begin - 1; };
        }
        // Four items, which are meant to be held in registers
        typename std::iterator_traits<Iterator>::value_type e1, e2, e3, e4;
        for (; begin != end;) {
            // Pick up four items
            e4 = *begin++;
            e3 = *begin++;
            e2 = *begin++;
            e1 = *begin++;
            // Visit each element without returning home
            // Once an element has been processed, we can resize to
            // avoid carrying it along with us.
            if (worker_(e1)) { return begin - 1; }
            if (worker_(e2)) { return begin - 2; }
            if (worker_(e3)) { return begin - 3; }
            if (worker_(e4)) { return begin - 4; }
            RESIZE();
        }
        return end;
    }
};

template< class ForwardIt, class UnaryPredicate >
ForwardIt find_if(unroll_policy, ForwardIt first, ForwardIt last,
                   UnaryPredicate p )
{
    return unroll_p<ForwardIt, UnaryPredicate>{p}(first, last);
}

// TODO: implement parallel find.
// Notes:
// - Parent can't terminate threads in cilk, so even if we find the value early,
// still have to wait for all threads to finish. (I guess we could have each
// thread check a shared variable for early termination, but that would
// increase contention and migrations.)
// - If multiple threads find an item, spec says we need to return the first
// match. But this may be unnecessary in many cases. Could provide another
// overload, find_any, find_if_any


} // end namespace detail

template< class Policy, class ForwardIt, class UnaryPredicate,
    std::enable_if_t<is_execution_policy_v<Policy>, int> = 0>
ForwardIt find_if( Policy policy, ForwardIt first, ForwardIt last,
                   UnaryPredicate p )
{
    return detail::find_if(policy, first, last, p);
}

template< class ForwardIt, class UnaryPredicate >
ForwardIt find_if( ForwardIt first, ForwardIt last,
                   UnaryPredicate p ) {
    return detail::find_if(default_policy, first, last, p);
}

} // end namespace emu::parallel
