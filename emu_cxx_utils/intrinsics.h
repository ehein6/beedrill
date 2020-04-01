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
// Remote bitwise ops only make sense for unsigned types
// The memoryweb definitions take signed type, so we need to case
inline void
remote_and(volatile unsigned long * ptr, unsigned long value) {
    return REMOTE_AND(
        reinterpret_cast<volatile long*>(ptr),
        static_cast<long>(value));
}
inline void
remote_or(volatile unsigned long * ptr, long value) {
    return REMOTE_OR(
        reinterpret_cast<volatile long*>(ptr),
        static_cast<long>(value));
}
inline void
remote_xor(volatile unsigned long * ptr, long value) {
    return REMOTE_XOR(
        reinterpret_cast<volatile long*>(ptr),
        static_cast<long>(value));
}

// Remote min/max can work on signed or unsigned
inline void
remote_max(volatile long * ptr, long value) {
    return REMOTE_MAX(ptr, value);
}
inline void
remote_min(volatile long * ptr, long value) {
    return REMOTE_MIN(ptr, value);
}
inline void
remote_max(volatile unsigned long * ptr, unsigned long value) {
    return REMOTE_MAX(
        reinterpret_cast<volatile long*>(ptr),
        static_cast<long>(value));
}
inline void
remote_min(volatile unsigned long * ptr, unsigned long value) {
    return REMOTE_MIN(
        reinterpret_cast<volatile long*>(ptr),
        static_cast<long>(value));
}

//TODO implement all remotes/atomics
};