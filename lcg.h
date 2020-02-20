#pragma once

// Linear congruential random number generator
class lcg {
private:
    const unsigned long LCG_MUL64 = 6364136223846793005ULL;
    const unsigned long LCG_ADD64 = 1;

    unsigned long state_;
public:
    explicit lcg(unsigned long step)
    {
        unsigned long mul_k, add_k, ran, un;

        mul_k = LCG_MUL64;
        add_k = LCG_ADD64;

        ran = 1;
        for (un = step; un; un >>= 1) {
            if (un & 1)
                ran = mul_k * ran + add_k;
            add_k *= (mul_k + 1);
            mul_k *= mul_k;
        }

        state_ = ran;
    }

    unsigned long operator() () {
        state_ = LCG_MUL64 * state_ + LCG_ADD64;
        return state_;
    }
};