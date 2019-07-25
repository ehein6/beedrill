#pragma once

#include <gtest/gtest.h>
#include <algorithm>
#include <numeric>
#include <emu_cxx_utils/reduce.h>

// TODO implement reduce test for class type
// Need to somehow disable compilation of the test for striped_array

template<class Array>
class reduce_test : public ::testing::Test {
protected:
    const long n = 8;
    Array array;
public:
    reduce_test() : array(n)
    {
        std::iota(array.begin(), array.end(), typename Array::value_type());
    }
};
TYPED_TEST_CASE_P(reduce_test);

TYPED_TEST_P(reduce_test, reduce)
{
    auto result = emu::parallel::reduce(this->array.begin(), this->array.end(), 0L);
    auto golden = std::accumulate(this->array.begin(), this->array.end(), 0);
    ASSERT_EQ(result, golden);
}

REGISTER_TYPED_TEST_CASE_P(reduce_test
,reduce
);
