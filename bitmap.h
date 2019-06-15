#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include <emu_cxx_utils/replicated.h>

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
        // return n % NODELETS();
        return n & (NODELETS() - 1);
    }

    static unsigned
    bit_offset(long n)
    {
        // return n / NODELETS();
        return n >> PRIORITY(NODELETS());
    }

public:

    // Constructor
    explicit
    bitmap(long n)
    // Need 1 word per 64 bits; divide and round up
    : words_((n + 63) / 64)
    {}

    // Shallow copy constructor
    bitmap(const bitmap& other, emu::shallow_copy tag)
    : words_(other.words_, tag)
    {}

    // Set all bits to zero
    void
    clear()
    {
        // TODO parallelize with emu_local_for
        for (long i = 0; i < words_.size(); ++i) {
            words_[i] = 0;
        }
    }

    bool
    get_bit(long pos) const
    {
        unsigned word = word_offset(pos);
        unsigned bit = bit_offset(pos);
        return words_[word] & (1UL << bit);
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
    //FIXME
    friend void
    swap(bitmap & lhs, bitmap & rhs)
    {
        using std::swap;
        swap(lhs.words_, rhs.words_);
    }

//
//    // Swap two bitmaps
//    static inline void
//    bitmap_replicated_swap(bitmap * a, bitmap * b)
//    {
//        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
//            bitmap * a_n = mw_get_nth(a, nlet);
//            bitmap * b_n = mw_get_nth(b, nlet);
//            bitmap_swap(a_n, b_n);
//        }
//    }

};
