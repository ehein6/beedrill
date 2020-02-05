#pragma once

extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}
#include <cassert>
#include <algorithm>
#include <iterator>

namespace emu {

template<typename T>
class repl_iterator
{
public:
    // Standard iterator typedefs for interop with C++ algorithms
    typedef repl_iterator self_type;
    typedef typename std::iterator_traits<T*>::iterator_category iterator_category;
    typedef typename std::iterator_traits<T*>::value_type value_type;
    typedef typename std::iterator_traits<T*>::difference_type difference_type;
    typedef typename std::iterator_traits<T*>::pointer pointer;
    typedef typename std::iterator_traits<T*>::reference reference;
private:
    // The pointer
    T* ptr_;
public:

    repl_iterator(T& ref) : ptr_(&ref)
    {
        assert(pmanip::get_view(ptr_) == 1);
    }

    reference  operator*()                      { return *ptr_; }
    reference  operator*() const                { return *ptr_; }
    pointer    operator->()                     { return ptr_; }
    pointer    operator->() const               { return ptr_; }
    reference  operator[](difference_type i)    { return emu::pmanip::get_nth(ptr_, i); }

    // This is the magic, incrementing a striped iterator moves you forward by 'stride' elements
    // All the other operators are boilerplate.
    self_type& operator+=(difference_type n)
    {
        pmanip::change_nodelet(ptr_, n);
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
        // TODO extract nlet numbers and subtract
        return 0;
    }
};

template<class T>
repl_iterator 
repl_begin(T& ref) 
{
    assert(pmanip::is_repl(&ref));
    return repl_iterator<T>(pmanip::get_nth(0));
}

template<class T>
repl_iterator
repl_end(T& ref)
{
    assert(pmanip::is_repl(&ref));
    // This is an invalid pointer, it points past the end of the
    // last nodelet. Must be used only for comparison, never dereferenced.
    return repl_iterator<T>(pmanip::get_nth(NODELETS()));
}

} // end namespace emu
