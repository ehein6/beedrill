#pragma once

extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}
#include <algorithm>
#include <iterator>
#include "iterator_layout.h"

namespace emu {


/**
 * Iterator for striped_array
 * If use_nodelet_stride is false, then this class behaves exactly like a pointer
 * If use_nodelet_stride is true, then the iterator moves in steps of NODELETS()
 *
 * @tparam I Underlying pointer to value_type
 * @tparam use_nodelet_stride Advance pointer by NODELETS() when true
 */
template<typename I, typename std::iterator_traits<I>::difference_type stride>
class stride_iterator
{
private:
    I it;
public:

    // Create iterator from pointer
    striped_array_iterator_base(I it) : it(it) {}

    // Convert between striped/sequential mode
    template<bool U>
    stride_iterator(const stride_iterator<I, U> &other) : it(other.it) {}

    // striped/sequential versions are friends
    friend class striped_array_iterator_base<I, !use_nodelet_stride>;
    // Typedef to allow converting between striped/sequential versions
    typedef striped_array_iterator_base<I, true> as_striped_iterator;
    typedef striped_array_iterator_base<I, false> as_sequential_iterator;
    // Standard iterator typedefs for interop with C++ algorithms
    typedef striped_array_iterator_base self_type;
    typedef typename std::iterator_traits<I>::iterator_category iterator_category;
    typedef typename std::iterator_traits<I>::value_type value_type;
    typedef typename std::iterator_traits<I>::difference_type difference_type;
    typedef typename std::iterator_traits<I>::pointer pointer;
    typedef typename std::iterator_traits<I>::reference reference;

    reference  operator*()                      { return *it; }
    reference  operator*() const                { return *it; }
    pointer    operator->()                     { return it; }
    pointer    operator->() const               { return it; }
    reference  operator[](difference_type i)    { return *(this + i); } // FIXME can we use 'this' like this?

    // This is the magic, incrementing a striped iterator moves you forward by 'stride' elements
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
    friend difference_type
    operator- (const self_type& lhs, const self_type& rhs)
    {
        const long stride = use_nodelet_stride ? NODELETS() : 1;
        return (lhs.it - rhs.it) / stride;
    }
};

template<typename I>
using striped_array_iterator = striped_array_iterator_base<I, false>;
template<typename I>
using striped_array_striped_iterator = striped_array_iterator_base<I, true>;

// Register type trait so that algorithms are aware of the striped layout
template<typename T>
struct iterator_layout<striped_array_iterator<T>>
{
    typedef striped_layout_tag value;
};

template<typename T>
struct iterator_layout<striped_array_striped_iterator<T>>
{
    typedef sequential_layout_tag value;
};

}; // end namespace emu
