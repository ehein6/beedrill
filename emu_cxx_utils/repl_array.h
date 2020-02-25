#pragma once

#include <emu_c_utils/emu_c_utils.h>

namespace emu {

template<class T>
class repl_array {
private:
    T *data_;
    long size_;
public:
    explicit repl_array(long size) : size_(size) {
        data_ = reinterpret_cast<T *>(mw_mallocrepl(sizeof(T) * size));
        assert(data_);
    }

    repl_array(const repl_array &other, emu::shallow_copy)
        : data_(other.data_), size_(other.size_) {
    }

    ~repl_array() {
        mw_free(data_);
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
};

} // end namespace emu