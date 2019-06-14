#include <emu_cxx_utils/blocked_array.h>
#include <emu_cxx_utils/replicated.h>

#include <emu_cxx_utils/transform.h>

#include <algorithm>
#include <numeric>

#include "iterator_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayIteratorTests, iterator_test, emu::blocked_array<long>);

#include "transform_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayTransformTests, transform_test, transform_test_types<emu::blocked_array>);

#include "reduce_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayReduceLongTests, reduce_test, emu::blocked_array<long>);
INSTANTIATE_TYPED_TEST_CASE_P(StripedArrayReduceDoubleTests, reduce_test, emu::blocked_array<double>);