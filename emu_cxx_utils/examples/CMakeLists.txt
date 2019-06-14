link_libraries(emu_cxx_utils)

# Enable Cilk
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcilkplus")


if (NOT CMAKE_SYSTEM_NAME STREQUAL Emu1)
    link_libraries(cilkrts)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL Emu1)
    # Dump register spill info
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -trace warn-spill -trace spill-sig")
endif()

find_package(GTest)
if (GTEST_FOUND)

    function(add_test_exe filename)
        string(REGEX REPLACE "\\.[^.]*$" "" name ${filename})
        add_executable(${name} ${filename})
        target_include_directories(${name} PRIVATE ${GTEST_INCLUDE_DIRS})
        target_link_libraries(${name} ${GTEST_BOTH_LIBRARIES})
        add_test(${name} ${name})
        GTEST_ADD_TESTS(${name} "" ${filename})
    endfunction()

    add_test_exe(test_local_array.cc)
    add_test_exe(test_blocked_array.cc)
    add_test_exe(test_striped_array.cc)
#    add_test_exe(codegen_bug.cc)
#    add_test_exe(invariant_bug.cc)

    add_custom_target(check
        COMMAND
            ${CMAKE_CTEST_COMMAND} -E env CTEST_OUTPUT_ON_FAILURE=1 --verbose
        DEPENDS
            test_local_array
            test_striped_array
            test_blocked_array)
endif()
#add_test_exe(test_ragged_array.cc)
#add_test_exe(test_repl.cc)