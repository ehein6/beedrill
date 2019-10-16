#include <vector>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/fill.h>
#include <emu_cxx_utils/transform_reduce.h>
#include <common.h>

using namespace emu;
using namespace emu::execution;

struct dot_bench {
    emu::striped_array<long> a_, b_;
    emu::repl<long> product_;
    explicit dot_bench(long n) : a_(n), b_(n) {}

    dot_bench(const dot_bench& other, emu::shallow_copy shallow)
    : a_(other.a_, shallow)
    , b_(other.b_, shallow)
    {
    }

    void init()
    {
        // forall i, A[i] = 1, B[i] = 2, C[i] = -1
        parallel::fill(a_.begin(), a_.end(), 2L);
        parallel::fill(b_.begin(), b_.end(), 3L);
        product_ = 0;
    }

    void run()
    {
        product_ = parallel::transform_reduce(
            a_.begin(), a_.end(), b_.begin(), 0L,
            [](long lhs, long rhs) { return lhs + rhs; },
            [](long lhs, long rhs) { return lhs * rhs; });
    }

    void validate()
    {
        long expected_product = 2 * 3 * a_.size();
        if (product_ != expected_product) {
            LOG("VALIDATION ERROR: product == %li (supposed to be %li)\n",
                product_.get(), expected_product);
            exit(1);
        }
    }

    long bytes_per_element() { return 2 * sizeof(long); }
};

struct arguments {
    long log2_num_elements_;
    long num_trials_;

    static arguments
    parse(int argc, char** argv)
    {
        arguments args;
        if (argc != 3) {
            LOG("Usage: %s log2_num_elements num_trials\n", argv[0]);
            exit(1);
        } else {
            args.log2_num_elements_ = atol(argv[1]);
            args.num_trials_ = atol(argv[2]);

            if (args.log2_num_elements_ <= 0) { LOG("log2_num_elements must be > 0"); exit(1); }
            if (args.num_trials_ <= 0) { LOG("num_trials must be > 0"); exit(1); }
        }
        return args;
    }
};


int main(int argc, char * argv[])
{
    auto args = arguments::parse(argc, argv);

    long n = 1L << args.log2_num_elements_;
    long mbytes = n * sizeof(long) / (1024*1024);
    long mbytes_per_nodelet = mbytes / NODELETS();
    LOG("Initializing arrays with %li elements each (%li MiB total, %li MiB per nodelet)\n",
        2 * n, 2 * mbytes, 2 * mbytes_per_nodelet);
    auto bench = emu::make_repl_copy<dot_bench>(n);

#ifndef NO_VALIDATE
    bench->init();
#endif
    LOG("Doing dot product...\n");
    for (long trial = 0; trial < args.num_trials_; ++trial) {
        hooks_set_attr_i64("trial", trial);
        hooks_region_begin("dot_bench");
        bench->run();
        double time_ms = hooks_region_end();
        double bytes_per_second = time_ms == 0 ? 0 :
            (n * sizeof(long) * 2) / (time_ms / 1000);
        LOG("%3.2f MB/s\n", bytes_per_second / (1000000));
    }
#ifndef NO_VALIDATE
    LOG("Validating results...");
    bench->validate();
    LOG("OK\n");
#endif
}