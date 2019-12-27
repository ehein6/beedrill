link_libraries(emu_cxx_utils)

# Stop compilation after first error
# A singl e
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")

add_executable(local_stream_cxx local_stream_cxx.cc)
add_executable(global_stream_cxx global_stream_cxx.cc)
add_executable(global_reduce_cxx global_reduce_cxx.cc)
add_executable(dot_product_cxx dot_product_cxx.cc)

add_custom_target(benchmarks DEPENDS
    local_stream_cxx
    global_stream_cxx
    global_reduce_cxx
    dot_product_cxx
)

add_emusim_test( "local_stream_cxx"
    local_stream_cxx.mwx 15 1
)

add_emusim_test( "global_stream_cxx"
    local_stream_cxx.mwx 15 1
)

add_emusim_test( "global_reduce_cxx"
    local_stream_cxx.mwx 15 1
)

add_emusim_test( "dot_product_cxx"
    local_stream_cxx.mwx 15 1
)
