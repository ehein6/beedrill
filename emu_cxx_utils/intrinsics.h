#pragma once

#include <functional>
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

// Compare-and-swap on 64-bit int
template<>
inline long
atomic_cas(long volatile * ptr, long oldval, long newval)
{
    return ATOMIC_CAS(ptr, newval, oldval);
}

// Compare-and-swap on 64-bit type
template<typename T>
inline T
atomic_cas(T volatile * ptr, T oldval, T newval)
{
    static_assert(sizeof(T) == sizeof(long), "CAS supported only for 64-bit types");
    // We're fighting the C++ type system, but codegen doesn't care
    union pun {
        T t;
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
    return retval_p.t;
}


// Remote add on 64-bit int
inline void
remote_add(volatile long * ptr, long value)
{
    return REMOTE_ADD(ptr, value);
}

// Remote add on pointer
template<class T>
inline T*
remote_add(T * volatile * ptr, ptrdiff_t value)
{
    value *= sizeof(T);
    REMOTE_ADD((volatile long*)ptr, (long)value);
}

//TODO implement all remotes/atomics
};