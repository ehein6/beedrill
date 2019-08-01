#pragma once

#include <emu_c_utils/emu_c_utils.h>

namespace emu {
//
//template<class T, class U>
//void remote_add(volatile T& ref, U value)
//{
//    static_assert(sizeof(T) == 8,
//        "Remote operations are defined only for 64-bit types");
//    REMOTE_ADD((volatile long*)&ref, (long)value);
//}

template<class T, class U>
T atomic_addms(volatile T& ref, U value)
{
    static_assert(sizeof(T) == 8,
        "Remote operations are defined only for 64-bit types");
    return (T)ATOMIC_ADDMS((volatile long*)&ref,(long)value);
}

//TODO implement all remotes/atomics
};