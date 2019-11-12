#pragma once

#include <cassert>
#include <iterator>

namespace emu {

/**
 * An iterator wrapper, which advances the underlying iterator by several elements at a time.
 *
 * @tparam I Wrapped iterator type
 */
template<typename I>
class stride_iterator
{
public:
    // Standard iterator typedefs for interop with C++ algorithms
    typedef stride_iterator self_type;
    typedef typename std::iterator_traits<I>::iterator_category iterator_category;
    typedef typename std::iterator_traits<I>::value_type value_type;
    typedef typename std::iterator_traits<I>::difference_type difference_type;
    typedef typename std::iterator_traits<I>::pointer pointer;
    typedef typename std::iterator_traits<I>::reference reference;

    using wrapped_iterator = I;

private:
    // The wrapped iterator type
    I it;
public:
    // The amount to add on each increment
    difference_type stride;

    // Create stride iterator from another iterator type
    stride_iterator(I it, difference_type stride=1) : it(it), stride(stride) {}
    // Convert back to
    explicit operator wrapped_iterator() { return it; }

    reference  operator*()                      { return *it; }
    reference  operator*() const                { return *it; }
    pointer    operator->()                     { return it; }
    pointer    operator->() const               { return it; }
    reference  operator[](difference_type i)    { return *(*(this) + i); }

    // Split a range into two sub-ranges consisting of the even and odd
    // elements from the initial range. The resulting iterators will have
    // double the stride of the input ranges.
    // Caller is expected to pass two copies of the input range.
    // e.g. on entry, begin_evens == begin_odds && end_evens == end_odds.
    // Arguments are modified in-place.
    friend void
    split(self_type& begin_evens, self_type& end_evens,
          self_type& begin_odds,  self_type& end_odds)
    {
        // Odd range starts at index 1
        std::advance(begin_odds, 1);

        // Adjust range endpoints. This is tricky!
        // - The past-the-end points for both ranges need to move forwards 1,
        //   since after we double the stride we actually need to be
        //   two-past-the-end.
        // - Then we need to decide which range gets the last element:
        //       - If n is even, n-1 is odd, so the last element belongs to the
        //         odd range. Need to move the even range endpoint back by 1.
        //       - If n is odd, n-1 is even, and the odd range has one fewer
        //         element. Need to move the odd range endpoint back by 1.
        // - Adding these factors together, we advance the odd endpoint
        //   if n is even, and advance the even endpoint n is odd.
        if (std::distance(begin_evens, end_evens) % 2 == 0) {
            std::advance(end_odds, 1);
        } else {
            std::advance(end_evens, 1);
        }

        // Finally, double the stride of both ranges
        begin_evens.stride *= 2;
        end_evens.stride *= 2;
        begin_odds.stride *= 2;
        end_odds.stride *= 2;
    }

    // This is the magic, incrementing moves you forward by 'stride' elements
    // All the other operators are boilerplate.
    self_type& operator+=(difference_type n)
    {
        it += n * stride;
        return *this;
    }
    self_type& operator-=(difference_type n)    { return operator+=(-n); }
    self_type& operator++()                     { return operator+=(+1); }
    self_type& operator--()                     { return operator+=(-1); }
    const self_type  operator++(int)            { self_type tmp = *this; this->operator++(); return tmp; }
    const self_type  operator--(int)            { self_type tmp = *this; this->operator--(); return tmp; }

    // Compare iterators
    friend bool
    operator==(const self_type& lhs, const self_type& rhs) { return lhs.it == rhs.it; }
    friend bool
    operator!=(const self_type& lhs, const self_type& rhs) { return lhs.it != rhs.it; }
    friend bool
    operator< (const self_type& lhs, const self_type& rhs) { return lhs.it <  rhs.it; }
    friend bool
    operator> (const self_type& lhs, const self_type& rhs) { return lhs.it >  rhs.it; }
    friend bool
    operator<=(const self_type& lhs, const self_type& rhs) { return lhs.it <= rhs.it; }
    friend bool
    operator>=(const self_type& lhs, const self_type& rhs) { return lhs.it >= rhs.it; }

    // Add/subtract integer to iterator
    friend self_type
    operator+ (const self_type& iter, difference_type n)
    {
        self_type tmp = iter;
        tmp += n;
        return tmp;
    }
    friend self_type
    operator+ (difference_type n, const self_type& iter)
    {
        self_type tmp = iter;
        tmp += n;
        return tmp;
    }
    friend self_type
    operator- (const self_type& iter, difference_type n)
    {
        self_type tmp = iter;
        tmp -= n;
        return tmp;
    }
    friend self_type
    operator- (difference_type n, const self_type& iter)
    {
        self_type tmp = iter;
        tmp -= n;
        return tmp;
    }

    // Difference between iterators
    // Subtract underlying iterators and divide by the stride
    friend difference_type
    operator- (const self_type& lhs, const self_type& rhs)
    {
        assert(lhs.stride == rhs.stride);
        return (lhs.it - rhs.it) / lhs.stride;
    }
};

} // end namespace emu
