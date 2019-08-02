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
        # Run the test natively
        add_test(NAME ${name} COMMAND ${command} ${ARGN})
    endif()
endfunction()

# Enable libc extenstions like getline
add_definitions(-D_XOPEN_SOURCE=700)

if (NOT CMAKE_SYSTEM_NAME STREQUAL "Emu1")
    add_subdirectory(generator)
endif()

include_directories(.)

add_executable(hybrid_bfs
    graph.h
    graph.cc
    dist_edge_list.h
    dist_edge_list.cc
    ack_control.h
    sliding_queue.h
    bitmap.h
    hybrid_bfs.h
    hybrid_bfs.cc
    hybrid_bfs_main.cc
)

set(TEST_GRAPH "/home/ehein6/inputs/graph500-scale15")

add_emusim_test( "build_graph"
    hybrid_bfs --graph ${TEST_GRAPH} --check_graph
)
add_emusim_test( "bfs_migrating_threads"
    hybrid_bfs --graph ${TEST_GRAPH} --check_results --alg migrating_threads
)
add_emusim_test( "bfs_remote_writes"
    hybrid_bfs --graph ${TEST_GRAPH} --check_results --alg remote_writes
)
add_emusim_test( "bfs_beamer_hybrid"
    hybrid_bfs --graph ${TEST_GRAPH} --check_results --alg beamer_hybrid
)



#add_executable(tc
#    graph.h
#    graph.cc
#    dist_edge_list.h
#    dist_edge_list.cc
#    ack_control.h
#    tc.h
#    tc.cc
#    tc_main.cc
#)

add_subdirectory(emu_cxx_utils)