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
#include <cassert>
#include <cilk/cilk.h>

// Logging macro. Flush right away since Emu hardware usually doesn't
#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout);



namespace emu {

/**
 * Encapsulates a striped array ( @c mw_malloc1dlong).
 * @tparam T Element type. Must be a 64-bit type (generally @c long or a pointer type).
 */
template<typename T>
class blocked_array
{
private:
    long n;
    long chunk_size;
    T * ptr;

    static T*
    stripe_nth(T * ptr, long n)
    {
        unsigned long x = reinterpret_cast<unsigned long>(ptr);
        x += n * BYTES_PER_NODELET();
        return reinterpret_cast<T*>(x);
    }

    /**
     * Low-level helper for computing the address of an element within the blocked array
     * @param base Array base pointer (from mw_mallocstripe)
     * @param chunk_size Number of elements per chunk (must be power of 2)
     * @param offset Number of elements to advance
     * @return Pointer to element that is @
     */
    static T*
    index(T* base, long chunk_size, long offset)
    {
        long nodelet_offset = offset >> PRIORITY(chunk_size);
        T * nodelet_ptr = stripe_nth(base, nodelet_offset);
        return nodelet_ptr + (offset&(chunk_size-1));
    }

    /**
     * Like index(), but also returns the chunk_id that is computed internally
     * @param[in] base Array base pointer (from mw_mallocstripe)
     * @param[in] chunk_size Number of elements per chunk (must be power of 2)
     * @param[in] offset Logical index into array
     * @param[out] chunk_id_out Chunk ID that contains this element
     * @param[out] ptr_out Pointer to element
     */
    static void
    logical_to_physical(
        T* const base, long chunk_size, long offset,
        long& chunk_id_out, T*& ptr_out)
    {
        chunk_id_out = offset >> PRIORITY(chunk_size);
        T * nodelet_ptr = stripe_nth(base, chunk_id_out);
        ptr_out = nodelet_ptr + (offset&(chunk_size-1));
    }


public:
    typedef T value_type;

    // Default constructor
    blocked_array() : n(0), chunk_size(0), ptr(nullptr) {};

    explicit
    blocked_array(long num_elements) : n(num_elements)
    {
        // round N up to power of 2 for efficient indexing
        assert(n > 1);
        n = 1 << (PRIORITY(n-1)+1);

        chunk_size = n / NODELETS();
        size_t sz = chunk_size * sizeof(T);
        ptr = static_cast<T*>(mw_mallocstripe(sz));
        assert(ptr);

        // Call constructor on each element if required
        if (!std::is_trivially_default_constructible<T>::value) {
            // Call default constructor (placement-new) on each element using parallel apply
            // TODO do this in parallel
            for (long i = 0; i < n; ++i) {
                new(&this->operator[](i)) T();
            }
        }

    }

    template<bool is_const>
    class iterator_base {
    public:
        typedef iterator_base self_type;
        typedef std::random_access_iterator_tag iterator_category;
        typedef std::conditional_t<is_const, const T, T> value_type;
        typedef ptrdiff_t difference_type;
        typedef std::conditional_t<is_const, const T*, T*> pointer;
        typedef std::conditional_t<is_const, const T&, T&> reference;
        typedef std::conditional_t<is_const, const blocked_array*, blocked_array*> container_pointer;
    private:

        // TODO what's the right number of fields to store here?
        // Pointer to the current element
        pointer pos;
        // Pointer to the first element in the contiguous chunk containing pos
        pointer chunk_first;
        // Pointer to the last element in the contiguous chunk containing pos
        pointer chunk_last;
        // Id of current chunk
        difference_type chunk_id;
        // Pointer to the container
        container_pointer ref;

        // Advance the position of the iterator forwards by n (or backwards if n is negative)
        // This operation is relatively expensive, we should avoid calling it in the common case
        void advance(difference_type n)
        {
            // Convert pointer to logical id
            difference_type chunk_offset = pos - chunk_first;
            difference_type absolute_pos = chunk_id * ref->chunk_size + chunk_offset;
            // Increment logical id
            absolute_pos += n;

            // Convert back to physical pointer and nodelet id
            logical_to_physical(ref->ptr, ref->chunk_size, absolute_pos,
                chunk_id, pos);
        }
    public:
        iterator_base(container_pointer ref, difference_type offset) : ref(ref)
        {
            logical_to_physical(
                ref->ptr, ref->chunk_size, offset,
                chunk_id, pos);
            chunk_first = stripe_nth(ref->ptr, chunk_id);
            chunk_last = chunk_first + ref->chunk_size - 1;
        }

        reference operator*() { return *pos; }
        pointer operator->() { return pos; }
        reference operator[] (difference_type i)
        {
            self_type tmp = *this;
            advance(tmp, i);
            return *tmp.pos;
        }

        self_type operator++()
        {
            if (pos < chunk_last) {
                // Common case, increment pointer within chunk
                ++pos;
            } else {
                // Rare, skip to next chunk
                advance(1);
            }
            return *this;
        }

        self_type operator--()
        {
            if (pos > chunk_first) {
                // Common case, decrement pointer within chunk
                --pos;
            } else {
                // Rare, skip to prev chunk
                advance(-1);
            }
            return *this;
        }

        self_type& operator+=(difference_type n)
        {
            // Try to advance with simple addition
            pointer new_pos = pos + n;
            // If we end up outside this chunk, fall back to generic logic
            if (new_pos > chunk_last || new_pos < chunk_first) {
                advance(n);
            } else {
                pos = new_pos;
            }
            return *this;
        }
        self_type& operator-=(difference_type n) { return operator+=(-n); }

        self_type operator+(difference_type n)
        {
            self_type iter = *this;
            iter += n;
            return iter;
        }
        self_type operator-(difference_type n) { return operator+(-n); }

        difference_type
        operator-(const self_type& other)
        {
            // Number of elements between me and the start of my chunk
            difference_type offset = pos - chunk_first;
            // Number of elements between other and the start of its chunk
            difference_type other_offset = other.pos - other.chunk_first;
            // Account for elements in chunks between us
            difference_type chunk_offset = (chunk_id - other.chunk_id) * ref->chunk_size;

            return offset - other_offset + chunk_offset;
        }


        bool operator==(const self_type& rhs) const { return pos == rhs.pos; }
        bool operator!=(const self_type& rhs) const { return pos != rhs.pos; }
    };

    typedef iterator_base<false> iterator;
    typedef iterator_base<true> const_iterator;

    iterator begin()                { return iterator(this, 0); }
    iterator end()                  { return iterator(this, n); }
    const_iterator begin() const    { return const_iterator(this, 0); }
    const_iterator end() const      { return const_iterator(this, n); }
    const_iterator cbegin() const   { return const_iterator(this, 0); }
    const_iterator cend() const     { return const_iterator(this, n); }

//    T* data() { return ptr; }
//    const T* data() const { return ptr; }
//
//    T& front()              { return ptr[0]; }
//    const T& front() const  { return ptr[0]; }
//    T& back()               { return ptr[n-1]; }
//    const T& back() const   { return ptr[n-1]; }

    // Destructor
    ~blocked_array()
    {
        if (ptr == nullptr) { return; }
        // TODO destroy in parallel
        if (!std::is_trivially_destructible<T>::value) {
            for (long i = 0; i < n; ++i) {
                this->operator[](i).~T();
            }
        }
        mw_free(ptr);
    }

    friend void
    swap(blocked_array& first, blocked_array& second)
    {
        using std::swap;
        swap(first.n, second.n);
        swap(first.chunk_size, second.chunk_size);
        swap(first.ptr, second.ptr);
    }

    // Copy constructor
    blocked_array(const blocked_array & other) : blocked_array(other.n)
    {
        // TODO Copy elements over in parallel
        for (long i = 0; i < n; ++i) {
            this->operator[](i) = other[i];
        }
    }

    // Assignment operator (using copy-and-swap idiom)
    blocked_array& operator= (blocked_array other)
    {
        swap(*this, other);
        return *this;
    }

    // Move constructor (using copy-and-swap idiom)
    blocked_array(blocked_array&& other) noexcept : blocked_array()
    {
        swap(*this, other);
    }

    // Shallow copy constructor (used for repl<T>)
    blocked_array(const blocked_array& other, shallow_copy)
        : n(other.n), chunk_size(other.chunk_size), ptr(other.ptr) {}

    T&
    operator[] (long i)
    {
        return *index(ptr, chunk_size, i);
    }

    const T&
    operator[] (long i) const
    {
        return *index(ptr, chunk_size, i);
    }

    long size() const { return n; }
};

} // end namespace emu

