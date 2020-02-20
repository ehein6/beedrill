cmake_minimum_required (VERSION 3.3.1)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
project(Beedrill C CXX)
enable_testing()

# Use the 2011/2017 standards for C and C++
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
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

# Macro for running tests on x86 or emusim
function(add_emusim_test name command)
    if (CMAKE_SYSTEM_NAME STREQUAL Emu1)
        # Run the test in the simulator
        # Use untimed mode with randomly-initialized memory
        # Suppress simulator stats, just forward the return code
        add_test(
            NAME ${name}
            COMMAND ${EMU_SIMULATOR}
                -o /dev/null
                --ignore_starttiming
                --forward_return_value
                --log2_nodelet_memory 27
                --initialize_memory
                --
                ${command} ${ARGN}
        )
    else()
        string(REPLACE "\.mwx" "" stripped-cmd ${command})
        # Run the test natively
        add_test(NAME ${name} COMMAND ${stripped-cmd} ${ARGN})
    endif()
endfunction()

# Stop compilation after the first error
#add_definitions(
#    -Wfatal-errors
#)

add_library(mmio
    mmio.c
    mmio.h
)
link_libraries(mmio)

# Build graph generator
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Emu1")
    add_subdirectory(generator)
endif()

# Build graph library
include_directories(.)
add_library(graph STATIC
    graph.h
    graph.cc
    dist_edge_list.h
    dist_edge_list.cc
)
link_libraries(graph)

# Breadth-first search

add_executable(hybrid_bfs
    hybrid_bfs.h
    hybrid_bfs.cc
    hybrid_bfs_main.cc
)
install(TARGETS hybrid_bfs RUNTIME DESTINATION ".")

set(TEST_GRAPH "/home/ehein6/inputs/graph500-scale12")

add_emusim_test( "build_graph"
    hybrid_bfs.mwx --graph ${TEST_GRAPH} --check_graph --alg none
)
add_emusim_test( "bfs_migrating_threads"
    hybrid_bfs.mwx --graph ${TEST_GRAPH} --check_results --alg migrating_threads
)
add_emusim_test( "bfs_remote_writes"
    hybrid_bfs.mwx --graph ${TEST_GRAPH} --check_results --alg remote_writes
)
add_emusim_test( "bfs_beamer_hybrid"
    hybrid_bfs.mwx --graph ${TEST_GRAPH} --check_results --alg beamer_hybrid
)

# Connected Components

add_executable(components
    components.h
    components.cc
    components_main.cc
)
install(TARGETS components RUNTIME DESTINATION ".")

add_emusim_test( "components"
    components.mwx --graph ${TEST_GRAPH} --check_results
)

# Triangle Count

add_executable(triangle_count
   tc.h
   tc.cc
   tc_main.cc
)
install(TARGETS triangle_count RUNTIME DESTINATION ".")

add_emusim_test( "triangle_count"
    triangle_count.mwx --graph ${TEST_GRAPH} --check_results
)

# PageRank

add_executable(pagerank
   pagerank.h
   pagerank.cc
   pagerank_main.cc
)
install(TARGETS pagerank RUNTIME DESTINATION ".")

add_emusim_test( "pagerank"
    pagerank.mwx --graph ${TEST_GRAPH} --check_results
)

add_executable(combined
    combined.cc
    hybrid_bfs.cc hybrid_bfs.h
    components.cc components.h
    pagerank.cc pagerank.h
    tc.cc tc.h
)
install(TARGETS combined RUNTIME DESTINATION ".")
