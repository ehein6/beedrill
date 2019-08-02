#pragma once

#include <gtest/gtest.h>
#include <emu_cxx_utils/transform.h>

template<template<typename> class Array>
struct transform_test_types
{
    typedef Array<long> array_of_longs;
    typedef Array<double> array_of_doubles;
};

template<class Types>
class transform_test : public ::testing::Test {
protected:
    const long n = 16;
public:
};
TYPED_TEST_CASE_P(transform_test);

// Convert an array of longs to doubles in parallel using emu::parallel::transform
TYPED_TEST_P(transform_test, cast_to_double)
{
    auto array_of_longs   = typename TypeParam::array_of_longs(this->n);
    auto array_of_doubles = typename TypeParam::array_of_doubles(this->n);

    // References are easier to work with

    // Assign index to each value
    for (long i = 0; i < this->n; ++i) {
        array_of_longs[i] = i;
        array_of_doubles[i] = -1;
    }

    // Convert to doubles in parallel
    emu::parallel::transform( emu::execution::default_policy,
        array_of_longs.begin(), array_of_longs.end(), array_of_doubles.begin(),
        [&](long i) {
            return (double)i;
        }
    );

    // Check
    for (long i = 0; i < this->n; ++i) {
        if (array_of_doubles[i] != i) {
            EXPECT_DOUBLE_EQ(array_of_doubles[i], i);
        }
    }
}

// Convert an array of longs to doubles in parallel using emu::parallel::transform
TYPED_TEST_P(transform_test, stream_add)
{
    const long n = this->n;
    typename TypeParam::array_of_longs a(n), b(n), c(n);

    // Initialize
    for (long i = 0; i < n; ++i) {
        a[i] = 1;
        b[i] = 2;
        c[i] = -1;
    }

    // Add arrays in parallel
    emu::parallel::transform(emu::execution::default_policy,
        a.begin(), a.end(), b.begin(), c.begin(),
        [&](long x, long y) {
            return x + y;
        }
    );

    // Check result
    for (long i = 0; i < n; ++i) {
        EXPECT_EQ(c[i], 3);
    }
}


REGISTER_TYPED_TEST_CASE_P(transform_test
,cast_to_double
,stream_add
);
