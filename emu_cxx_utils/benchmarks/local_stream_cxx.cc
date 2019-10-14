#include <vector>
#include <memory>
#include <emu_cxx_utils/fill.h>
#include <emu_cxx_utils/transform.h>
#include <common.h>

using namespace emu;
using namespace emu::execution;

struct stream {
    std::vector<long> a_, b_, c_;
    explicit stream(long n) : a_(n), b_(n), c_(n) {};

    void init()
    {
        // forall i, A[i] = 1, B[i] = 2, C[i] = -1
        parallel::fill(a_.begin(), a_.end(), 1L);
        parallel::fill(b_.begin(), b_.end(), 2L);
        parallel::fill(c_.begin(), c_.end(), -1L);
    }

    void run()
    {
        parallel::transform(a_.begin(), a_.end(), b_.begin(), c_.begin(),
            [] (long a, long b) {
                return a + b;
            }
        );
    }

    void validate()
    {
        for (size_t i = 0; i < c_.size(); ++i) {
            if (c_[i] != 3) {
                LOG("VALIDATION ERROR: c[%li] == %li (supposed to be 3)\n", i, c_[i]);
                exit(1);
            }
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
    LOG("Initializing arrays with %li elements each (%li MiB total, %li MiB per nodelet)\n",
        3 * n, 3 * mbytes, 3 * mbytes_per_nodelet);
    auto bench = std::make_unique<stream>(n);
#ifndef NO_VALIDATE
    bench->init();
#endif
    LOG("Doing vector addition \n");
    for (long trial = 0; trial < args.num_trials_; ++trial) {
        hooks_set_attr_i64("trial", trial);
        hooks_region_begin("stream");
        bench->run();
        double time_ms = hooks_region_end();
        double bytes_per_second = time_ms == 0 ? 0 :
            (n * sizeof(long) * 3) / (time_ms / 1000);
        LOG("%3.2f MB/s\n", bytes_per_second / (1000000));
    }
#ifndef NO_VALIDATE
    LOG("Validating results...");
    bench->validate();
    LOG("OK\n");
#endif
}
