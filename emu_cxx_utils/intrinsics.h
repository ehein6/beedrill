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


//TODO implement all remotes/atomics
};