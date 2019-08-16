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
private:
    // The wrapped iterator type
    I it;
public:
    // The amount to add on each increment
    difference_type stride;

    stride_iterator(I it, difference_type stride=1) : it(it), stride(stride) {}

    reference  operator*()                      { return *it; }
    reference  operator*() const                { return *it; }
    pointer    operator->()                     { return it; }
    pointer    operator->() const               { return it; }
    reference  operator[](difference_type i)    { return *(*(this) + i); }

    // Modifies a pair of stride iterators, doubling their stride
    // The resulting range will cover every other element
    friend void
    stretch(self_type& begin, self_type& end)
    {
        // Compute new size of range (divide by 2 and round up)
        difference_type size = (end - begin + 1) / 2;
        // Double the stride
        begin.stride *= 2;
        // Recalculate past-the-end point based on new stride
        end = begin + size;
    }

    friend void
    stretch(self_type& self)
    {
        // Double the stride
        self.stride *= 2;
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
