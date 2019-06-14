#include <emu_cxx_utils/transform.h>
#include <vector>

#include "transform_test_template.h"
template<typename T>
using vector = std::vector<T, std::allocator<T>>;
INSTANTIATE_TYPED_TEST_CASE_P(LocalArrayTransformTests, transform_test, transform_test_types<vector>);

#include "reduce_test_template.h"
INSTANTIATE_TYPED_TEST_CASE_P(LocalArrayReduceLongTests, reduce_test, std::vector<long>);
//INSTANTIATE_TYPED_TEST_CASE_P(LocalArrayReduceDoubleTests, reduce_test, std::vector<double>);