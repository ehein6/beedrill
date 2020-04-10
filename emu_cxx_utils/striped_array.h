#pragma once

#include <emu_c_utils/emu_c_utils.h>

#include "replicated.h"
#include "out_of_memory.h"
//#include "copy.h"

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
    repl<long> n_;
    repl<T*> ptr_;

    T* allocate(long size)
    {
        auto ptr = reinterpret_cast<T*>(
            mw_malloc1dlong(static_cast<size_t>(size)));
        if (!ptr) { EMU_OUT_OF_MEMORY(size * sizeof(long)); }
        return ptr;
    }

public:
    typedef T value_type;

    // Default constructor
    striped_array() : n_(0), ptr_(nullptr) {};

    /**
     * Constructs a emu_striped_array<T>
     * @param n Number of elements
     */
    explicit striped_array(long n)
    : n_(n)
    , ptr_(allocate(n))
    {}

    typedef T* iterator;
    typedef const T* const_iterator;

    iterator begin ()               { return ptr_; }
    iterator end ()                 { return ptr_ + n_; }
    const_iterator begin () const   { return ptr_; }
    const_iterator end () const     { return ptr_ + n_; }
    const_iterator cbegin () const  { return ptr_; }
    const_iterator cend () const    { return ptr_ + n_; }

    T* data() { return ptr_; }
    const T* data() const { return ptr_; }

    T& front()              { return ptr_[0]; }
    const T& front() const  { return ptr_[0]; }
    T& back()               { return ptr_[n_ - 1]; }
    const T& back() const   { return ptr_[n_ - 1]; }

    // Destructor
    ~striped_array()
    {
        if (ptr_) { mw_free((void*)ptr_); }
    }

    friend void
    swap(striped_array& first, striped_array& second)
    {
        using std::swap;
        swap(first.n_, second.n_);
        swap(first.ptr_, second.ptr_);
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
    : n_(other.n_), ptr_(other.ptr_) {}

    T&
    operator[] (long i)
    {
        return ptr_[i];
    }

    const T&
    operator[] (long i) const
    {
        return ptr_[i];
    }

    long size() const { return n_; }

    void resize(long new_size)
    {
        // Do we need to reallocate?
        if (new_size > n_) {
            // Allocate new array
            auto new_ptr = allocate(new_size);
            if (ptr_) {
                // Copy elements over into new array
//                emu::parallel::copy(ptr_, ptr_ + n_, new_ptr);
                // Deallocate old array
                mw_free(ptr_);
            }
            // Save new pointer
            ptr_ = new_ptr;
        }
        // Update size
        n_ = new_size;
    }
};

} // end namespace emu