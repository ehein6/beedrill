#pragma once

namespace emu::parallel {
namespace detail {

// Serial version
template<class Iterator, class UnaryFunction>
void
for_each(
    sequenced_policy,
    Iterator begin, Iterator end, UnaryFunction worker
) {
    // Forward to standard library implementation
    std::for_each(begin, end, worker);
}


}

