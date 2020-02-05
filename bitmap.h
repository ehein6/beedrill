#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/replicated.h>
#include "common.h"

class bitmap
{
private:
    emu::striped_array<unsigned long> words_;

    // We stripe at the bit level, not the word level
    // For example, the bit at index 3 is stored in (words[3] & 1)
    // and the bit at index 9 is stored in (words[0] & 2) assuming 8 nodelets.

    static unsigned
    word_offset(long n)
    {
        return NODELETS() * (n / (64 * NODELETS())) + n % NODELETS();
    }

    static unsigned
    bit_offset(long n)
    {
        // return (n / NODELETS()) % 64;
        return (n >> PRIORITY(NODELETS())) & (63);
    }

    static long
    div_round_up(long num, long den)
    {
        assert(den != 0);
        return (num + den - 1) / den;
    }

public:

    // Constructor
    explicit
    bitmap(long n)
    // Need 1 word per 64 bits; divide and round up
    // Need 1 word per nodelet minimum
    : words_(NODELETS()*div_round_up(n, 64*NODELETS()))
    {}

    // Shallow copy constructor
    bitmap(const bitmap& other, emu::shallow_copy tag)
    : words_(other.words_, tag)
    {}

    // Set all bits to zero
    void
    clear()
    {
        emu::parallel::striped_for_each(
            emu::parallel_limited_policy(1024),
            words_.begin(), words_.end(),
            [](long& w) {
                w = 0;
            }
        );
    }

    bool
    get_bit(long pos) const
    {
        unsigned word = word_offset(pos);
        unsigned bit = bit_offset(pos);
        return (words_[word] & (1UL << bit)) != 0UL;
    }

    void
    dump()
    {
        for (long i = 0; i < words_.size() * 64; ++i) {
            if (get_bit(i)) {
                printf("%li ", i);
            }
        }
        printf("\n");
        fflush(stdout);
    }

    void
    set_bit(long pos)
    {
        unsigned word = word_offset(pos);
        unsigned bit = bit_offset(pos);
        REMOTE_OR((long*)&words_[word], 1UL << bit);
    }

    // Swap two bitmaps
    friend void
    swap(bitmap & lhs, bitmap & rhs)
    {
        using std::swap;
        swap(lhs.words_, rhs.words_);
    }
};
