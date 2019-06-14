#pragma once

extern "C" {
#ifdef __le64__
#include <memoryweb.h>
#else
#include "memoryweb_x86.h"
#endif
}
#include <cinttypes>
#include <algorithm>
#include <cmath>
#include <memory>
#include "striped_array.h"
#include "replicated.h"
#include "make_unique.h"

namespace emu {

/**
 * Uses a malloc1d_long to create a CSR-like data structure
 *
 * @tparam T
 */

template<typename T>
class ragged_array
{
    static_assert(sizeof(T) == 8, "ragged_array can only hold 64-bit data types");
protected:
    // Size of each array such that len(ptrs[i]) == sizes[i]
    striped_array<long> offsets;
    striped_array<T> items;

    static void
    compute_local_offsets(void * hint, long nodelet_id,
        striped_array<long>& offsets,
        const striped_array<long>& sizes)
    {
        long cum_sum = nodelet_id;
        long i;
        for (i = nodelet_id; i < sizes.size(); i += NODELETS()) {
            offsets[i] = cum_sum;
            cum_sum += sizes[i] * NODELETS();
        }
        offsets[i] = cum_sum;
    }

    // Helper function to compute list of offsets from array of bucket sizes
    static striped_array<long>
    compute_offsets(const striped_array<long> & sizes)
    {
        // Make another array with the same length
        // TODO should be replicated
        striped_array<long> offsets(sizes.size() + NODELETS());
        // TODO need prefix scan here
        for (long nodelet_id = 0; nodelet_id < NODELETS(); ++nodelet_id) {
            cilk_spawn compute_local_offsets(&offsets[nodelet_id], nodelet_id, offsets, sizes);
        }
        cilk_sync;
        return offsets;
    }

    static long
    longest_chunk(const striped_array<long>& offsets)
    {
        // The last offset for each chunk points to one past the end of the local chunk
        long max = *std::max_element(
            offsets.cend() - NODELETS(),
            offsets.cend()
        );
        return max;
    }

public:

    ragged_array() = default;

    // Shallow copy constructor
    ragged_array(const ragged_array& other, shallow_copy)
    : offsets(other.offsets, shallow_copy()), items(other.items, shallow_copy()) {}

    typedef T value_type;

    // Construct a ragged array from a list of bucket sizes
    ragged_array(const striped_array<long> & sizes)
    : offsets(compute_offsets(sizes))
    , items(longest_chunk(offsets) - (NODELETS() - 1))
    {
    }

    template<bool is_const>
    class subarray_base {
    private:
        typedef typename std::conditional<is_const, const T*, T*>::type pointer;

        pointer first;
        pointer last;
    public:
        subarray_base(pointer first, pointer last) : first(first), last(last) {}

        T&
        operator[] (long i)
        {
            pointer ptr = first + i * NODELETS();
            assert(ptr < last);
            return *ptr;
        }

        const T&
        operator[] (long i) const
        {
            const T* ptr = first + i * NODELETS();
            assert(ptr < last);
            return *ptr;
        }

        template<bool is_const_iterator>
        class iterator_base {
        public:
            typedef iterator_base self_type;
            typedef std::forward_iterator_tag iterator_category;
            typedef typename std::conditional<is_const_iterator, const T, T>::type value_type;
            typedef ptrdiff_t difference_type;
            typedef typename std::conditional<is_const_iterator, const T*, T*>::type pointer;
            typedef typename std::conditional<is_const_iterator, const T&, T&>::type reference;
            iterator_base(pointer pos) : pos(pos) {}
            self_type operator++() { pos += NODELETS(); return *this; }
            reference operator*() { return *pos; }
            pointer operator->() { return pos; }
            operator pointer() { return pos; }

            bool operator==(const self_type& rhs) const { return pos == rhs.pos; }
            bool operator!=(const self_type& rhs) const { return pos != rhs.pos; }
        private:
            pointer pos;
        };

        typedef iterator_base<false> iterator;
        typedef iterator_base<true> const_iterator;

        iterator begin() { return iterator(first); }
        iterator end() { return iterator(last); }
        const_iterator begin() const { return const_iterator(first); }
        const_iterator end() const { return const_iterator(last); }
        const_iterator cbegin() const { return const_iterator(first); }
        const_iterator cend() const { return const_iterator(last); }

        long size() const { return (last - first) / NODELETS(); }
    };

    typedef subarray_base<false> subarray;
    typedef subarray_base<true> const const_subarray;

    subarray
    operator[] (long i)
    {
        assert(i < offsets.size() - NODELETS());
        T* first = &items[offsets[i]];
        T* last = &items[offsets[i + NODELETS()]];
        return subarray(first, last);
    }

    const_subarray
    operator[] (long i) const
    {
        assert(i < offsets.size() - NODELETS());
        const T* first = &items[offsets[i]];
        const T* last = &items[offsets[i + NODELETS()]];
        return const_subarray(first, last);
    }

    template<bool is_const>
    class iterator_base {
    public:
        typedef iterator_base self_type;
        typedef std::random_access_iterator_tag iterator_category;
        typedef subarray_base<is_const> value_type;
        typedef long difference_type;
        typedef long pointer;
        typedef subarray_base<is_const> reference;
        typedef std::conditional<is_const, const ragged_array&, ragged_array&> container_reference;
        iterator_base(container_reference array, long pos) : array(array), pos(pos) {}
        self_type operator++() { ++pos; return *this; }
        reference operator*() { return array[pos]; }
        bool operator==(const self_type& rhs) const { return pos == rhs.pos; }
        bool operator!=(const self_type& rhs) const { return pos != rhs.pos; }
    private:
        container_reference array;
        pointer pos;
    };

    typedef iterator_base<false> iterator;
    typedef iterator_base<true> const_iterator;

    iterator begin() { return iterator(*this, 0); }
    iterator end() { return iterator(*this, offsets.size()-NODELETS()); }
    const_iterator begin() const { return const_iterator(*this, 0); }
    const_iterator end() const { return const_iterator(*this, offsets.size()-NODELETS()); }
    const_iterator cbegin() const { return const_iterator(*this, 0); }
    const_iterator cend() const { return const_iterator(*this, offsets.size()-NODELETS()); }

    // Debug, print out internal structures to stdout
    void dump() {

//        for (long i = 0; i < NODELETS(); ++i) {
//            ragged_array * self = static_cast<ragged_array*>(mw_get_nth(this, i));
//            printf("items[%li] = %p (%li) \n", i, self->items.data(), self->items.size());
//            printf("offsets[%li] = %p (%li)\n", i, self->offsets.data(), self->offsets.size());
//        }

        printf("%li items\n", items.size());
        printf("Offsets: \n");
        for (long i = 0; i < offsets.size() - NODELETS(); ++i) {
            printf("%li: %li-%li\n", i, offsets[i], offsets[i + NODELETS()]);
        }
    }
};

} // end namespace emu