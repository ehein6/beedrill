cmake_minimum_required (VERSION 3.3.1)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
project(Beedrill C CXX)

# Use the 2011/2014 standards for C and C++
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
# Turn warnings into errors
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")
# Enable Cilk
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fcilkplus")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcilkplus")

# Link with emu_c_utils
find_package(emu_c_utils REQUIRED)
link_libraries(${EMU_C_UTILS_LIBRARIES})
include_directories(${EMU_C_UTILS_INCLUDE_DIRS})

# Enable libc extenstions like getline
add_definitions(-D_XOPEN_SOURCE=700)

if (NOT CMAKE_SYSTEM_NAME STREQUAL "Emu1")
    add_subdirectory(generator)
endif()

include_directories(.)

add_library(graph_loader OBJECT
    graph.h
    graph.cc
    dist_edge_list.h
    dist_edge_list.cc
)

add_executable(hybrid_bfs
    $<TARGET_OBJECTS:graph_loader>
    ack_control.h
    sliding_queue.h
    bitmap.h
    hybrid_bfs.h
    hybrid_bfs.cc
    hybrid_bfs_main.cc
)

add_executable(tc
    $<TARGET_OBJECTS:graph_loader>
    tc.h
    tc.c
    tc_main.c
)
