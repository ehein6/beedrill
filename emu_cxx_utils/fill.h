#pragma once

#include <emu_cxx_utils/for_each.h>
namespace emu::parallel {

template<typename ExecutionPolicy, typename Iterator, typename T,
    // Disable if first argument is not an execution policy
    std::enable_if_t<execution::is_execution_policy_v<ExecutionPolicy>, int> = 0
>
void
fill(ExecutionPolicy policy, Iterator first, Iterator last, const T& value)
{
    for_each(policy, first, last,
        [value](T& item){ item = value; }
    );
}

template<typename Iterator, typename T>
void
fill(Iterator first, Iterator last, const T& value)
{
    fill(execution::default_policy, first, last, value);
}

}
