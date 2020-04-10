cmake_minimum_required (VERSION 3.3.1)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
project(Beedrill C CXX)
enable_testing()

# Grab the current git revision
include(GetGitRevisionDescription)
get_git_head_revision(GIT_REFSPEC GIT_SHA1)
git_describe(GIT_TAG)
git_local_changes(GIT_DIRTY)
if (${GIT_DIRTY} STREQUAL "DIRTY")
    set(GIT_TAG "${GIT_TAG}-dirty")
endif()

# Use the 2011/2017 standards for C and C++
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
# Turn warnings into errors
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

# Build graph generator
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Emu1")
    add_subdirectory(generator)
endif()

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

set(TEST_GRAPH "/home/ehein6/inputs/graph500-scale12")

# Add current path to include list so emu_cxx_utils headers can be included
include_directories(.)

# Create a file in the build directory which has the current Git hash
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/git_sha1.cc.in"
               "${CMAKE_CURRENT_BINARY_DIR}/git_sha1.cc"
                @ONLY)

# Create single lib for all algorithms and graph construction routines
add_library(beedrill STATIC
    graph.cc
    dist_edge_list.cc
    hybrid_bfs.cc
    components.cc
    tc.cc
    pagerank.cc
    ktruss_graph.cc
    ktruss.cc
    # Include git hash
    ${CMAKE_CURRENT_BINARY_DIR}/git_sha1.cc
)
link_libraries(beedrill)

# Breadth-first search
add_executable(hybrid_bfs hybrid_bfs_main.cc)
install(TARGETS hybrid_bfs RUNTIME DESTINATION ".")
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
add_executable(components components_main.cc)
install(TARGETS components RUNTIME DESTINATION ".")
add_emusim_test( "components"
    components.mwx --graph ${TEST_GRAPH} --check_results
)

# K-truss
add_executable(ktruss ktruss_main.cc)
install(TARGETS ktruss RUNTIME DESTINATION ".")
add_emusim_test( "ktruss"
    ktruss.mwx --graph ${TEST_GRAPH} --check_results
)

# Triangle Count
add_executable(triangle_count tc_main.cc)
install(TARGETS triangle_count RUNTIME DESTINATION ".")
add_emusim_test( "triangle_count"
    triangle_count.mwx --graph ${TEST_GRAPH} --check_results
)

# PageRank
add_executable(pagerank pagerank_main.cc)
install(TARGETS pagerank RUNTIME DESTINATION ".")
add_emusim_test( "pagerank"
    pagerank.mwx --graph ${TEST_GRAPH} --check_results
)

# Single binary that can run (almost) all algorithms at once
add_executable(combined combined.cc)
install(TARGETS combined RUNTIME DESTINATION ".")
