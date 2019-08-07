#pragma once

#ifdef __le64__
#include <pmanip.h>
#endif

namespace emu::pmanip {

template <typename T>
unsigned get_view(const T * ptr)
{
#ifdef __le64__
    return ( (((long)(ptr)) & __MW_VIEW_MASK__) >> __MW_VIEW_SHIFT__);
#else
    return 1;
#endif
}

template<typename T>
bool is_repl(const T * ptr){
#ifdef __le64__
    return emu::pmanip::get_view(ptr) == 0;
#else
    return false;
#endif
}

template<typename T>
bool is_striped(const T * ptr){
#ifdef __le64__
    return emu::pmanip::get_view(ptr) > 1;
#else
    return false;
#endif
}

template <typename T>
T * get_nth(T * repladdr, long n)
{
#ifdef __le64__
    unsigned long ptr = reinterpret_cast<unsigned long>(repladdr);
    return reinterpret_cast<T*>(ptr
        // Change to view 1
        | (0x1UL<<__MW_VIEW_SHIFT__)
        // Set node number
        | (n <<__MW_NODE_BITS__)
        // Preserve shared bit
        | ((ptr) & __MW_VIEW_SHARED__)
    );
#else
    return repladdr;
#endif
}

template <typename T>
const T * get_nth(const T * repladdr, long n)
{
    return const_cast<const T*>(get_nth(repladdr, n));
}


} // end namespace emu::pmanip
