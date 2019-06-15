#pragma once

#ifdef __le64__
#include <pmanip.h>

namespace emu {
namespace pmanip {

/** View number of ptr */
template <typename T>
long get_view(T * ptr)
{
    return ( (((long)(ptr)) & __MW_VIEW_MASK__) >> __MW_VIEW_SHIFT__);
}

template <typename T>
T * get_nth(T * repladdr, long n)
{
    long curr; //  the current node id
    T * absaddr; // repladdr converted to an absolute address
    if(n >= NODELETS() || n < 0)
        return NULL;
    curr = NODE_ID();
    absaddr = mw_ptr0to1(repladdr);
    return K_NODES_FROM_ADDR(absaddr, n-curr);
}

} // end namespace pmanip
} // end namespace emu

#endif
