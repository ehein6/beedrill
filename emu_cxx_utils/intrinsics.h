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
atomic_cas(T volatile * ptr, T oldval, T newval)
{
    static_assert(sizeof(T) == 8, "ATOMIC_CAS requires 64-bit type");
    return (T)ATOMIC_CAS((volatile long*)ptr, (long)oldval, (long)newval);
}

//TODO implement all remotes/atomics
};