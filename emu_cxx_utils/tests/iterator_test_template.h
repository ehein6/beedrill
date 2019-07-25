#pragma once

#include <gtest/gtest.h>

template<class Array>
class iterator_test: public ::testing::Test {
protected:
    const long n = 8;
    Array array;
public:
    iterator_test() : array(n) {}
};
TYPED_TEST_CASE_P(iterator_test);

// Method 1: use index operator
TYPED_TEST_P(iterator_test, for_loop)
{
    for (long i = 0; i < this->n; ++i) {
        this->array[i] = i;
    }
    for (long i = 0; i < this->n; ++i) {
        ASSERT_EQ(this->array[i], i);
    }
}

// Method 2: use iterators
TYPED_TEST_P(iterator_test, iterator_loop)
{
    long i = 0;
    auto begin = this->array.begin();
    auto end = this->array.end();
    for (auto f = begin; f != end; ++f) {
        *f = i++;
    }
    i = 0;
    for (auto f = this->array.begin(); f != this->array.end(); ++f) {
        ASSERT_EQ(*f, i++);
    }
}

// Method 3: for-each loop
TYPED_TEST_P(iterator_test, for_each)
{
    long i = 0;
    for (long& f : this->array) {
        f = i++;
    }
    i = 0;
    for (long& f : this->array) {
        ASSERT_EQ(f, i++);
    }
}
REGISTER_TYPED_TEST_CASE_P(iterator_test
    ,for_loop
    ,iterator_loop
    ,for_each
);