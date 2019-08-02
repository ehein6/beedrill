#pragma once

#include <emu_cxx_utils/striped_for_each.h>
namespace emu::parallel {

template<typename Policy, typename Iterator, typename T>
void
fill(Policy&& policy, Iterator first, Iterator last, const T& value)
{
    emu::parallel::for_each(std::forward<Policy>(policy), first, last,
        [&](T& item){ item = value; }
    );
}

}
