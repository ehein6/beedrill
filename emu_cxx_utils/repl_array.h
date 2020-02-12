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
        : data_(other.data_), size_(other.size) {
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

    // TODO do we actually want this? Index into the local copy of the array?
    const T &operator[](long i) const {
        return data_[i];
    }
    // Note: omitting non-const overload, not sure what semantics should be...

    [[nodiscard]]
    long size() const { return size_; }
};

} // end namespace emu