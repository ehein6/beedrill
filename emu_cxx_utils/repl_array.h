#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include "out_of_memory.h"
#include "replicated.h"

namespace emu {

template<class T>
class repl_array {
private:
    T *data_;
    long size_;

    T* allocate(long size)
    {
        T* ptr = reinterpret_cast<T *>(mw_mallocrepl(sizeof(T) * size));
        if (!ptr) { EMU_OUT_OF_MEMORY(sizeof(T) * size * NODELETS()); }
        return ptr;
    }

public:
    // Constructor
    explicit repl_array(long size)
    : data_(allocate(size)), size_(size) {}

    // Default constructor
    repl_array() : data_(nullptr), size_(0) {}

    // Shallow copy constructor
    repl_array(const repl_array &other, emu::shallow_copy)
        : data_(other.data_), size_(other.size_) {
    }

    // Destructor
    ~repl_array() {
        if (data_) { mw_free(data_); }
    }

    T *get_nth(long n) {
        return emu::pmanip::get_nth(data_, n);
    }

    const T *get_nth(long n) const {
        return emu::pmanip::get_nth(data_, n);
    }

    T * get_localto(void * other) {
        return emu::pmanip::get_localto(data_, other);
    }

    const T * get_localto(const void * other) const {
        return emu::pmanip::get_localto(data_, other);
    }

    const T* data() const { return data_; }
    T* data() { return data_; }
    long size() const { return size_; }

    void resize(long new_size)
    {
        if (new_size <= size_) {
            // We already have enough elements, just change the size
            size_ = new_size;
        } else {
            // Allocate new array
            auto new_ptr = allocate(new_size);
            if (data_) {
                // Copy elements over into new array
//                emu::parallel::copy(ptr_, ptr_ + n_, new_ptr);
                // Deallocate old array
                mw_free(data_);
            }
            // Save new pointer
            data_ = new_ptr;
        }
    }
};

} // end namespace emu