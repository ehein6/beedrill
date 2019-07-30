cmake_minimum_required (VERSION 3.5.1)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
project(emu_cxx_utils CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

set(headers
    stride_iterator.h
    striped_array.h
    ragged_array.h
    make_unique.h
    chunked_array.h
    blocked_array.h
    spawn_templates.h
    replicated.h
    memoryweb_x86.h
)

add_library(emu_cxx_utils INTERFACE)
#target_sources(emu_cxx_utils INTERFACE ${headers})
target_include_directories(emu_cxx_utils INTERFACE ..)
install(FILES ${headers} DESTINATION include/emu_cxx_utils)

enable_testing()

add_subdirectory(tests)
add_subdirectory(benchmarks)
