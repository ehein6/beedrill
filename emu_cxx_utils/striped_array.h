#pragma once

extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}
#include <cinttypes>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstdio>
#include <cilk/cilk.h>

#include "stride_iterator.h"
#include "replicated.h"

namespace emu {

/**
 * Encapsulates a striped array ( @c mw_malloc1dlong).
 * @tparam T Element type. Must be a 64-bit type (generally @c long or a pointer type).
 */
template<typename T>
class striped_array
{
    static_assert(sizeof(T) == 8, "emu_striped_array can only hold 64-bit data types");
private:
    long n;
    T * ptr;

public:
    typedef T value_type;

    // Default constructor
    striped_array() : n(0), ptr(nullptr) {};

    /**
     * Constructs a emu_striped_array<T>
     * @param n Number of elements
     */
    explicit striped_array(long n) : n(n)
    {
        ptr = reinterpret_cast<T*>(mw_malloc1dlong(static_cast<size_t>(n)));
    }

    // Initializer-list constructor
    striped_array(std::initializer_list<T> list) : striped_array(list.size())
    {
        for (size_t i = 0; i < list.size(); ++i) {
            ptr[i] = *(list.begin() + i);
        }
    }

    typedef T* iterator;
    typedef const T* const_iterator;
    typedef T* striped_iterator;
    typedef const T* const_striped_iterator;

    iterator begin ()               { return ptr; }
    iterator end ()                 { return ptr + n; }
    const_iterator begin () const   { return ptr; }
    const_iterator end () const     { return ptr + n; }
    const_iterator cbegin () const  { return ptr; }
    const_iterator cend () const    { return ptr + n; }

    T* data() { return ptr; }
    const T* data() const { return ptr; }

    T& front()              { return ptr[0]; }
    const T& front() const  { return ptr[0]; }
    T& back()               { return ptr[n-1]; }
    const T& back() const   { return ptr[n-1]; }

    // Destructor
    ~striped_array()
    {
        if (ptr) { mw_free((void*)ptr); }
    }

    friend void
    swap(striped_array& first, striped_array& second)
    {
        using std::swap;
        swap(first.n, second.n);
        swap(first.ptr, second.ptr);
    }

    // Copy constructor
    striped_array(const striped_array & other) = delete;

    // Assignment operator (using copy-and-swap idiom)
    striped_array& operator= (striped_array other)
    {
        swap(*this, other);
        return *this;
    }

    // Move constructor (using copy-and-swap idiom)
    striped_array(striped_array&& other) noexcept : striped_array()
    {
        swap(*this, other);
    }

    // Shallow copy constructor (used for repl<T>)
    striped_array(const striped_array& other, shallow_copy)
    : n(other.n), ptr(other.ptr) {}

    T&
    operator[] (long i)
    {
        return ptr[i];
    }

    const T&
    operator[] (long i) const
    {
        return ptr[i];
    }

    long size() const { return n; }
};

} // end namespace emu