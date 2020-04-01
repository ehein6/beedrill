#pragma once

namespace emu {

/**
 * Base class for reducers.
 *
 * Unlike CilkPlus x86 reducers, which use thread-local storage for the view,
 * this reducer is meant to be copied by each thread and carried around in
 * registers. Upon copy, the new reducer holds a reference to the reducer it
 * was copied from. When the copy goes out of scope, it reduces into the
 * original reduction variable.
 *
 * @tparam Monoid Class that provides identity and reduce operations
 */
template<class Monoid>
class reducer_base : public Monoid {
public:
    using T = typename Monoid::value_type;
protected:
    // Local view of the reduction variable
    T local_sum_;
    // Pointer to reduction variable that I was copied from
    T *global_sum_;
public:
    // Default constructor: global sum points to self, this is the root
    reducer_base()
        : local_sum_(Monoid::identity())
        , global_sum_(&local_sum_) {}

    // Contructor: capture ref to global sum and init local sum to zero
    // This will use the pointed-to value for the reduction
    explicit reducer_base(T *global_sum)
        : local_sum_(Monoid::identity())
        , global_sum_(global_sum) {}

    // Copy constructor: propagate ref to global sum but init local sum to zero
    reducer_base(const reducer_base &other)
        : local_sum_(Monoid::identity())
        , global_sum_(other.global_sum_) {}

    // Shallow copy constructor: make a new root, do not connect to parent
    reducer_base(const reducer_base &other, emu::shallow_copy)
        : local_sum_(Monoid::identity())
        , global_sum_(nullptr) {}

    // Destructor: add to global sum
    ~reducer_base()
    {
        if (global_sum_ && global_sum_ != &local_sum_) {
            Monoid::reduce(global_sum_, local_sum_);
        }
    }

    // Fetches the final value for this reduction variable
    // Assumes that all copies of this reducer have been destructed
    T& get_value()
    {
        if (emu::pmanip::is_repl(this)) {
            return emu::repl_reduce(local_sum_, Monoid::reduce);
        } else {
            return local_sum_;
        }
    }
};

// Monoid that uses remote atomic add for integers
template<class T>
struct op_add
{
    using value_type = T;
    static T identity() { return 0; }
    static void reduce(T *lhs, T rhs) { emu::remote_add(lhs, rhs); }
    static T reduce(T &lhs, T &rhs) { return lhs + rhs; }
};

// Monoid that uses atomic compare-and-swap to add doubles
template<>
struct op_add<double>
{
    using value_type = double;
    static double identity() { return 0; }
    static void
    reduce(double *lhs, double rhs) {
        // Get a pointer to the local copy
        double new_value, old_value;
        do {
            // Read the value from memory
            old_value = *lhs;
            // Do the add
            new_value = old_value + rhs;
            // Do again if the old value doesn't match
        } while (old_value != atomic_cas(lhs, old_value, new_value));
    }
    static double reduce(double &lhs, double &rhs) { return lhs + rhs; }
};

// Reducer for computing sums
template<class T>
class reducer_opadd : public reducer_base<op_add<T>>
{
public:
    using reducer_base<op_add<T>>::reducer_base;
    using self_type = reducer_opadd;

    void operator+=(T& rhs) {
        this->local_sum_ += rhs;
    }
    void operator++() {
        ++this->local_sum_;
    }
    void operator++(int) {
        ++this->local_sum_;
    }
};

struct op_and
{
    using value_type = uint64_t;
    static uint64_t identity() { return 0xFFFFFFFFUL; }
    static void
    reduce(uint64_t *lhs, uint64_t rhs) { emu::remote_and(lhs, rhs); }
    static uint64_t
    reduce(uint64_t &lhs, uint64_t &rhs) { return lhs & rhs; }
};

class reducer_opand : public reducer_base<op_and>
{
public:
using reducer_base<op_and>::reducer_base;
using self_type = reducer_opand;
void operator&=(T& rhs) {
    this->local_sum_ &= rhs;
}
};

struct op_or
{
    using value_type = uint64_t;
    static uint64_t identity() { return 0; }
    static void
    reduce(uint64_t *lhs, uint64_t rhs) { emu::remote_or(lhs, rhs); }
    static uint64_t
    reduce(uint64_t &lhs, uint64_t &rhs) { return lhs | rhs; }
};

class reducer_opor : public reducer_base<op_or>
{
public:
    using reducer_base<op_or>::reducer_base;
    using self_type = reducer_opor;
    void operator|=(T& rhs) {
        this->local_sum_ |= rhs;
    }
};

struct op_xor
{
    using value_type = uint64_t;
    static uint64_t identity() { return 0; }
    static void
    reduce(uint64_t *lhs, uint64_t rhs) { emu::remote_xor(lhs, rhs); }
    static uint64_t
    reduce(uint64_t &lhs, uint64_t &rhs) { return lhs ^ rhs; }
};

class reducer_opxor : public reducer_base<op_xor>
{
public:
    using reducer_base<op_xor>::reducer_base;
    using self_type = reducer_opxor;
    void operator^=(T& rhs) {
        this->local_sum_ ^= rhs;
    }
};

template<class T>
struct op_max
{
    using value_type = T;
    static T identity() { return std::numeric_limits<T>::lowest(); }
    static void reduce(T *lhs, T rhs) { emu::remote_max(lhs, rhs); }
    static T reduce(T &lhs, T &rhs) { return std::max(lhs, rhs); }
};

template<class T>
class reducer_opmax : public reducer_base<op_max<T>>
{
public:
    using reducer_base<op_add<T>>::reducer_base;
    using self_type = reducer_opmax;

    void calc_max(T& rhs) {
        this->local_sum_ = std::max(this->local_sum_, rhs);
    }
};

template<class T>
struct op_min
{
    using value_type = T;
    static T identity() { return std::numeric_limits<T>::max(); }
    static void reduce(T *lhs, T rhs) { emu::remote_min(lhs, rhs); }
    static T reduce(T &lhs, T &rhs) { return std::min(lhs, rhs); }
};

template<class T>
class reducer_opmin : public reducer_base<op_min<T>>
{
public:
    using reducer_base<op_add<T>>::reducer_base;
    using self_type = reducer_opmin;

    void calc_min(T& rhs) {
        this->local_sum_ = std::min(this->local_sum_, rhs);
    }
};

} // end namespace emu