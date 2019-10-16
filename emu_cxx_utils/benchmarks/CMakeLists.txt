link_libraries(emu_cxx_utils)
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