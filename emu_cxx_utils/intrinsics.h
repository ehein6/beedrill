#pragma once

#include <emu_c_utils/emu_c_utils.h>

namespace emu {

// Increment 64-bit int
inline long
atomic_addms(volatile long * ptr, long value)
{
    return ATOMIC_ADDMS(ptr, value);
}

// Increment pointer
template<class T>
inline T*
atomic_addms(T * volatile * ptr, ptrdiff_t value)
{
    value *= sizeof(T);
    return (T*)ATOMIC_ADDMS((volatile long*)ptr, (long)value);
}

template<class T>
inline T
atomic_cas(T volatile * ptr, T oldval, T newval);

template<>
inline long
atomic_cas(long volatile * ptr, long oldval, long newval)
{
    return ATOMIC_CAS(ptr, oldval, newval);
}

template<>
inline double
atomic_cas(double volatile * ptr, double oldval, double newval)
{
    // TODO create an ATOMIC_CAS intrinsic that takes a double
    // We're fighting the C++ type system, but codegen doesn't care
    union pun {
        double f;
        long i;
    };
    pun oldval_p{oldval};
    pun newval_p{newval};
    pun retval_p;

    retval_p.i = ATOMIC_CAS(
        reinterpret_cast<long volatile*>(ptr),
        newval_p.i,
        oldval_p.i
    );
    return retval_p.f;
}

struct remote_add {
    void operator()(long &lhs, const long &rhs) { REMOTE_ADD(&lhs, rhs); }
};

struct remote_and {
    void operator()(long &lhs, const long &rhs) { REMOTE_AND(&lhs, rhs); }
};

struct remote_or {
    void operator()(long &lhs, const long &rhs) { REMOTE_OR(&lhs, rhs); }
};

struct remote_xor {
    void operator()(long &lhs, const long &rhs) { REMOTE_XOR(&lhs, rhs); }
};

struct remote_min {
    void operator()(long &lhs, const long &rhs) { REMOTE_MIN(&lhs, rhs); }
};

struct remote_max {
    void operator()(long &lhs, const long &rhs) { REMOTE_MAX(&lhs, rhs); }
};

template<typename F>
struct remote_op {
};

template<>
struct remote_op<std::plus<long>> : remote_add {
};
template<>
struct remote_op<std::bit_and<long>> : remote_and {
};
template<>
struct remote_op<std::bit_or<long>> : remote_or {
};
template<>
struct remote_op<std::bit_xor<long>> : remote_xor {
};

// Problem: there are no std functors for min and max. They might even be macros.
// What will binary_op be if the programmer wants a max/min reduction?
// Probably they should use std::max_element or std::min_element instead

//TODO implement all remotes/atomics
};