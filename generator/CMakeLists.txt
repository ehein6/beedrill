add_executable(rmat_dataset_dump
    rmat_args.h
    rmat_dataset_dump.cc
    rmat_generator.h
    prng_engine.hpp)

# This target is meant to be run on x86 with OpenMP
# Additionally, it assumes parallel implementations of <algorithm> functions
# See https://gcc.gnu.org/onlinedocs/libstdc++/manual/parallel_mode.html
find_package( OpenMP )
if (OPENMP_FOUND)
    target_compile_definitions(rmat_dataset_dump PRIVATE _GLIBCXX_PARALLEL)
    target_link_libraries(rmat_dataset_dump PRIVATE "${OpenMP_CXX_FLAGS}")
    target_compile_options(rmat_dataset_dump PRIVATE "${OpenMP_CXX_FLAGS}")
endif()
