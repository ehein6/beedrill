# This target is meant to be run on x86 with OpenMP
# Additionally, it assumes parallel implementations of <algorithm> functions
# See https://gcc.gnu.org/onlinedocs/libstdc++/manual/parallel_mode.html
find_package( OpenMP )
if (OPENMP_FOUND)
    add_definitions(-D_GLIBCXX_PARALLEL)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    link_libraries(${OpenMP_CXX_LIBRARIES})
endif()

add_executable(rmat_dataset_dump rmat_dataset_dump.cc)
add_executable(convert convert.cc)
