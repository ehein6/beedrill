#include <vector>
#include <emu_cxx_utils/replicated.h>
#include <emu_cxx_utils/striped_array.h>
#include <emu_cxx_utils/fill.h>
#include <emu_cxx_utils/reduce.h>
#include <common.h>

using namespace emu;


struct reduce_bench {
    emu::striped_array<long> a_;
    emu::repl<long> sum_;
    explicit reduce_bench(long n) : a_(n) {}

    reduce_bench(const reduce_bench& other, emu::shallow_copy shallow)
    : a_(other.a_, shallow)
    {
    }

    void init()
    {
        // forall i, A[i] = 1
        parallel::fill(a_.begin(), a_.end(), 1L);
        sum_ = 0;
    }

    void run()
    {
        sum_ = parallel::reduce(a_.begin(), a_.end(), 0L, std::plus<>());
    }

    void validate()
    {
        if (sum_ != a_.size()) {
            LOG("VALIDATION ERROR: sum == %li (supposed to be %li)\n",
                sum_.get(), a_.size());
            exit(1);
        }
    }
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
    LOG("Initializing array with %li elements (%li MiB total, %li MiB per nodelet)\n",
        3 * n, 3 * mbytes, 3 * mbytes_per_nodelet);
    auto bench = emu::make_repl_copy<reduce_bench>(n);

#ifndef NO_VALIDATE
    bench->init();
#endif
    LOG("Doing reduction over striped array...\n");
    for (long trial = 0; trial < args.num_trials_; ++trial) {
        hooks_set_attr_i64("trial", trial);
        hooks_region_begin("reduce");
        bench->run();
        double time_ms = hooks_region_end();
        double bytes_per_second = time_ms == 0 ? 0 :
            (n * sizeof(long)) / (time_ms / 1000);
        LOG("%3.2f MB/s\n", bytes_per_second / (1000000));
    }
#ifndef NO_VALIDATE
    LOG("Validating results...");
    bench->validate();
    LOG("OK\n");
#endif
}