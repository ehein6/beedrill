#pragma once

#include <cassert>
#include <iterator>
#include "intrinsics.h"

namespace emu {

/**
 * An iterator wrapper, which advances the underlying iterator by several elements at a time.
 *
 * @tparam Iterator Wrapped iterator type
 */
template<class Iterator>
class nlet_stride_iterator
{
public:
    // Standard iterator typedefs for interop with C++ algorithms
    using self_type = nlet_stride_iterator;
    using iterator_category = typename std::iterator_traits<Iterator>::iterator_category;
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using difference_type = typename std::iterator_traits<Iterator>::difference_type;
    using pointer = typename std::iterator_traits<Iterator>::pointer;
    using reference = typename std::iterator_traits<Iterator>::reference;
private:
    // The wrapped iterator type
    Iterator it;
public:

    // Create stride iterator from another iterator type
    explicit nlet_stride_iterator(Iterator it) : it(it) {}
    // Convert back to
    explicit operator Iterator() { return it; }

    reference  operator*()                      { return *it; }
    reference  operator*() const                { return *it; }
    pointer    operator->()                     { return it; }
    pointer    operator->() const               { return it; }
    reference  operator[](difference_type i)    { return *(*(this) + i); }

    // This is the magic, incrementing moves you forward by 'stride' elements
    // All the other operators are boilerplate.
    self_type& operator+=(difference_type n)
    {
        it += n * NODELETS();
        return *this;
    }
    self_type& operator-=(difference_type n)    { return operator+=(-n); }
    self_type& operator++()                     { return operator+=(+1); }
    self_type& operator--()                     { return operator+=(-1); }
    self_type  operator++(int)            { self_type tmp = *this; this->operator++(); return tmp; }
    self_type  operator--(int)            { self_type tmp = *this; this->operator--(); return tmp; }

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
        return (lhs.it - rhs.it) / NODELETS();
    }

    // Provide overloads for atomic increment
    // These only work if we are wrapping a raw pointer
    friend nlet_stride_iterator
    atomic_addms(nlet_stride_iterator* iter, ptrdiff_t value)
    {
        value *= NODELETS();
        return nlet_stride_iterator(emu::atomic_addms(&iter->it, value));
    }

//    template<class T>
//    friend nlet_stride_iterator<T*>
//    atomic_addm(nlet_stride_iterator<T*>* iter, ptrdiff_t value)
//    {
//        value *= NODELETS();
//        return atomic_addm(&iter->it, value);
//    }
};



} // end namespace emu
