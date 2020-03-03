#pragma once
#include <cstdio>

#define EMU_OUT_OF_MEMORY(BYTES) emu::out_of_memory(__PRETTY_FUNCTION__, BYTES)

namespace emu {

/**
 * Used to crash the program when a memory allocation fails
 * Normally we would throw std::bad_alloc, but the Emu compiler has
 * exceptions disabled
 * @param function_name Name of the function that tried to allocate memory
 * @param bytes How much memory it tried to allocate, in bytes
 */
inline void
out_of_memory(const char* function_name, size_t bytes)
{
    printf("Failed to allocate %li bytes in %s, exiting\n",
        bytes, function_name);
    fflush(stdout);
    std::terminate();
}

} // end namespace emu