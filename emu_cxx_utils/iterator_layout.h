#pragma once

namespace emu {
// Define traits to indicate which iterators have a striped layout

// Sequential layout = elements are contiguous
struct sequential_layout_tag {};
// Striped layout = elements are striped round-robin
struct striped_layout_tag {};

// By default all iterators are assumed to have a sequential layout
template<typename T>
struct iterator_layout
{
    typedef sequential_layout_tag value;
};

}