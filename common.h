#pragma once

#include <cstdio>
#include <cstdlib>
#include <emu_c_utils/emu_c_utils.h>
#include <cilk/cilk.h>

// HACK so we can compile with old toolchain
#ifndef cilk_spawn_at
#define cilk_spawn_at(X) cilk_spawn
#endif

// Logging macro. Flush right away since Emu hardware usually doesn't
#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout);
