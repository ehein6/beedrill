#pragma once

#ifdef __le64__
#include <pmanip.h>
#endif

/**
 * Raw pointer manipulation
 *
 */
namespace emu::pmanip {

/**
 * Returns the view number of a raw pointer
 */
template <typename T>
unsigned get_view(const T * ptr)
{
#ifdef __le64__
    return ( (((long)(ptr)) & __MW_VIEW_MASK__) >> __MW_VIEW_SHIFT__);
#else
    return 1;
#endif
}

/**
 * Determines whether a raw pointer is replicated (view-0)
 */
template<typename T>
bool is_repl(const T * ptr){
#ifdef __le64__
    return emu::pmanip::get_view(ptr) == 0;
#else
    return false;
#endif
}

/**
 * Determines whether a raw pointer is striped (view-2 or higher)
 */
template<typename T>
bool is_striped(const T * ptr){
#ifdef __le64__
    return emu::pmanip::get_view(ptr) > 1;
#else
    return false;
#endif
}

/**
 * Returns the nodelet number from a pointer
 * Result is undefined if view != 1
 */
template <typename T>
T * get_nodelet(T * ptr)
{
#ifdef __le64__
    return mw_ptrtonodelet(ptr);
#else
    return 0;
#endif
}

template <typename T>
T * view2to1(T * ptr)
{
#ifdef __le64__
    return mw_ptr2to1(ptr);
#else
    return ptr;
#endif
}

template <typename T>
T * view1to2(T * ptr)
{
#ifdef __le64__
    return mw_ptr1to2(ptr);
#else
    return ptr;
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
    return get_nth(const_cast<T*>(repladdr), n);
}


} // end namespace emu::pmanip
