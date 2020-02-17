#pragma once

#include <emu_cxx_utils/for_each.h>
namespace emu::parallel {

// Functor for setting each item to a single value
template<class T>
class filler
{
private:
    T value_;
public:
    explicit filler(T value) : value_(value) {}
    void operator()(T& item) { item = value_; }
};

template<typename ExecutionPolicy, typename Iterator, typename T,
    // Disable if first argument is not an execution policy
    std::enable_if_t<is_execution_policy_v<ExecutionPolicy>, int> = 0
>
void
fill(ExecutionPolicy policy, Iterator first, Iterator last, const T& value)
{
    for_each(policy, first, last, filler{value});
}

template<typename Iterator, typename T>
void
fill(Iterator first, Iterator last, const T& value)
{
    fill(default_policy, first, last, value);
}

}
