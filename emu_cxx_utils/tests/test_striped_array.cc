#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/replicated.h>

#include <algorithm>
#include <numeric>
#include <emu_cxx_utils/stride_iterator.h>
#include <emu_cxx_utils/transform.h>

#include <gtest/gtest.h>

#include "iterator_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayIteratorTests, iterator_test, emu::striped_array<long>);

#include "transform_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayTransformTests, transform_test, transform_test_types<emu::striped_array>);

#include "reduce_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayReduceLongTests, reduce_test, emu::striped_array<long>);
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayReduceDoubleTests, reduce_test, emu::striped_array<double>);