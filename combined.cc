#include <cstring>
#include <cmath>
#include <getopt.h>
#include <algorithm>
#include <numeric>

#include "graph.h"
#include "dist_edge_list.h"
#include "hybrid_bfs.h"
#include "components.h"
#include "pagerank.h"
#include "tc.h"
#include "lcg.h"

const struct option long_options[] = {
    {"graph_filename"   , required_argument},
    {"num_trials"       , required_argument},
    {"dump_edge_list"   , no_argument},
    {"check_graph"      , no_argument},
    {"dump_graph"       , no_argument},
    {"check_results"    , no_argument},
    {"help"             , no_argument},
    {nullptr}
};

void
print_help(const char* argv0)
{
    LOG( "Usage: %s [OPTIONS]\n", argv0);
    LOG("\t--graph_filename     Path to graph file to load\n");
    LOG("\t--num_trials         Run each algorithm this many times.\n");
    LOG("\t--source_vertex      Use this as the source vertex. If unspecified, pick random vertices.\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--check_results      Validate the BFS results (slow)\n");
    LOG("\t--help               Print command line help\n");
}

struct arguments
{
    const char* graph_filename;
    long num_trials;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
    bool check_results;

    static arguments
    parse(int argc, char *argv[])
    {
        arguments args = {};
        args.graph_filename = NULL;
        args.num_trials = 10;
        args.dump_edge_list = false;
        args.check_graph = false;
        args.dump_graph = false;
        args.check_results = false;

        int option_index;
        while (true) {
            int c = getopt_long(argc, argv, "", long_options, &option_index);
            // Done parsing
            if (c == -1) { break; }
            // Parse error
            if (c == '?') {
                LOG( "Invalid arguments\n");
                print_help(argv[0]);
                exit(1);
            }
            const char* option_name = long_options[option_index].name;

            if (!strcmp(option_name, "graph_filename")) {
                args.graph_filename = optarg;
            } else if (!strcmp(option_name, "num_trials")) {
                args.num_trials = atol(optarg);
            } else if (!strcmp(option_name, "dump_edge_list")) {
                args.dump_edge_list = true;
            } else if (!strcmp(option_name, "check_graph")) {
                args.check_graph = true;
            } else if (!strcmp(option_name, "dump_graph")) {
                args.dump_graph = true;
            } else if (!strcmp(option_name, "check_results")) {
                args.check_results = true;
            } else if (!strcmp(option_name, "help")) {
                print_help(argv[0]);
                exit(1);
            }
        }
        if (args.graph_filename == NULL) { LOG( "Missing graph filename\n"); exit(1); }
        if (args.num_trials <= 0) { LOG( "num_trials must be > 0\n"); exit(1); }
        return args;
    }
};

struct aggregate_stats
{
    double sum;
    double mean;
    double hmean;
    double stddev;
    double min;
    double max;

    static aggregate_stats
    compute(std::vector<double>& times)
    {
        aggregate_stats s;
        s.sum = std::accumulate(times.begin(), times.end(), 0.0);
        // Mean = sum of times / N
        s.mean = s.sum / times.size();
        // Harmonic mean = reciprocal of the mean of the reciprocals
        std::vector<double> reciprocals(times.size());
        std::transform(times.begin(), times.end(), reciprocals.begin(),
            [&](double x) { return 1.0/x; });
        s.hmean = (double) times.size() /
            std::accumulate(reciprocals.begin(), reciprocals.end(), 0.0);
        // Standard deviation = sqrt of sum of squared difference from
        // the mean / (N-1)
        std::vector<double> sq_error(times.size());
        std::transform(times.begin(), times.end(), sq_error.begin(),
            [&](double x) {
                double error = fabs(x - s.mean);
                return error * error;
            }
        );
        s.stddev = sqrt(std::accumulate(sq_error.begin(), sq_error.end(), 0.0)
            / (times.size() - 1));
        // Max and min values
        s.max = *std::max_element(times.begin(), times.end());
        s.min = *std::min_element(times.begin(), times.end());
        return s;
    }
};

static long
pick_random_vertex(graph& g, lcg& rng)
{
    long source;
    do {
        source = rng() % g.num_vertices();
    } while (g.out_degree(source) == 0);
    return source;
}

int main(int argc, char ** argv)
{
    bool success = true;
    // Set active region for hooks
    const char* active_region = getenv("HOOKS_ACTIVE_REGION");
    if (active_region != nullptr) {
        hooks_set_active_region(active_region);
    } else {
        hooks_set_active_region("bfs");
    }

    // Initialize RNG with deterministic seed
    lcg rng(0);

    // Parse command-line arguments
    arguments args = arguments::parse(argc, argv);

    // Load edge list from file
    auto dist_el = dist_edge_list::load(args.graph_filename);
    if (args.dump_edge_list) {
        LOG("Dumping edge list...\n");
        dist_el->dump();
    }

    // Build the graph
    LOG("Constructing graph...\n");
    auto g = graph::from_edge_list(*dist_el);
    // Need to sort the edge lists for triangle count
    LOG("Sorting edge lists...\n");
    g->sort_edge_lists([](long lhs, long rhs) { return lhs < rhs; });

    g->print_distribution();
    if (args.check_graph) {
        LOG("Checking graph...");
        if (g->check(*dist_el)) {
            LOG("PASS\n");
        } else {
            LOG("FAIL\n");
            success = false;
        };
    }
    if (args.dump_graph) {
        LOG("Dumping graph...\n");
        g->dump();
    }

    // Initialize the algorithms and stat trackers
    LOG("Initializing graph algorithm data structures...\n");
    auto bfs = emu::make_repl_shallow<hybrid_bfs>(*g);
    std::vector<double> bfs_teps(args.num_trials);
    auto cc = emu::make_repl_shallow<components>(*g);
    std::vector<double> cc_teps(args.num_trials);
    auto pr = emu::make_repl_shallow<pagerank>(*g);
    std::vector<double> pr_flops(args.num_trials);
    auto tc = emu::make_repl_shallow<triangle_count>(*g);
    std::vector<double> tc_tpps(args.num_trials);

    // Run multiple trials of each algorithm
    for (long trial = 0; trial < args.num_trials; ++trial) {
        hooks_set_attr_i64("trial", trial);
        // Clear out the data structures
        bfs->clear();
        cc->clear();
        pr->clear();
        tc->clear();

        long source = pick_random_vertex(*g, rng);
        LOG("Doing breadth-first search from vertex %li \n", source);
        hooks_region_begin("bfs");
        bfs->run_beamer(source, /*alpha*/15, /*beta*/18);
        double bfs_time_ms = hooks_region_end();
        long num_edges_traversed = bfs->count_num_traversed_edges();
        bfs_teps[trial] = num_edges_traversed / (1e-3 * bfs_time_ms);
        LOG("Traversed %li edges in %3.2f ms, %3.2f GTEPS\n",
            num_edges_traversed,
            bfs_time_ms,
            1e-9 * bfs_teps[trial]
        );

        LOG("Finding connected components...\n");
        hooks_region_begin("components");
        components::stats cc_stats = cc->run();
        hooks_set_attr_i64("num_iters", cc_stats.num_iters);
        hooks_set_attr_i64("num_components", cc_stats.num_components);
        double cc_time_ms = hooks_region_end();

        cc_teps[trial] = g->num_edges() * cc_stats.num_iters
            / (1e-3 * cc_time_ms);
        LOG("Found %li components in %li iterations (%3.2f ms, %3.2f GTEPS)\n",
            cc_stats.num_components,
            cc_stats.num_iters,
            cc_time_ms,
            1e-9 * cc_teps[trial]
        );

        LOG("Computing PageRank...\n");
        hooks_region_begin("pagerank");
        int num_iters = pr->run(/*max_iter*/20, /*damping*/0.85, /*epsilon*/1e-5);
        hooks_set_attr_i64("num_iters", num_iters);
        double pr_time_ms = hooks_region_end();
        // Compute FLOPS per iteration:
        double float_ops = num_iters * (5 * g->num_vertices() + 1 * g->num_edges());
        // Compute memory traffic per iteration:
        long bytes = num_iters * (56 * g->num_vertices() + 8 * g->num_edges());
        pr_flops[trial] = float_ops/(pr_time_ms*1e-3);
        // Output results
        LOG("Computed PageRank in %i iterations (%3.2f ms, %3.0f MFLOPS, %3.0f MB/s)\n",
            num_iters, pr_time_ms,
            1e-6*float_ops/(pr_time_ms*1e-3),
            1e-6*bytes/(pr_time_ms*1e-3));

        LOG("Counting triangles...\n");
        hooks_region_begin("tc");
        triangle_count::stats tc_stats = tc->run();
        hooks_set_attr_i64("num_triangles", tc_stats.num_triangles);
        hooks_set_attr_i64("num_twopaths", tc_stats.num_twopaths);
        double tc_time_ms = hooks_region_end();
        tc_tpps[trial] = tc_stats.num_twopaths / (1e-3 * tc_time_ms);
        LOG("Found %li triangles and %li two-paths in %3.2f ms, %3.2f GTTPS\n",
            tc_stats.num_triangles,
            tc_stats.num_twopaths,
            tc_time_ms,
            1e-9 * tc_tpps[trial]
        );

        if (args.check_results) {
            LOG("Checking BFS results...");
            if (bfs->check(source))
            { LOG("PASS\n"); } else { LOG("FAIL\n"); success = false; }
            LOG("Checking Connected Component results...");
            if (cc->check())
            { LOG("PASS\n"); } else { LOG("FAIL\n"); success = false; }
            LOG("Checking PageRank results...");
            if (pr->check(/*damping*/0.85, /*epsilon*/1e-5))
            { LOG("PASS\n"); } else { LOG("FAIL\n"); success = false; }
            LOG("Checking Triangle Count results...");
            if (tc->check())
            { LOG("PASS\n"); } else { LOG("FAIL\n"); success = false; }
        }
    } // end for each trial

    if (args.num_trials > 1) {
        // Summarize results
        auto bfs_stats = aggregate_stats::compute(bfs_teps);
        auto cc_stats = aggregate_stats::compute(cc_teps);
        auto pr_stats = aggregate_stats::compute(pr_flops);
        auto tc_stats = aggregate_stats::compute(tc_tpps);

        // Note: we use harmonic mean since we are averaging rates, not times.
        // Not sure how stddev is supposed to work in this case, we'll
        // just use stddev of the rates.
        LOG("\nMean performance over %li trials on %li nodelets:\n",
            args.num_trials, NODELETS());
        LOG("    BFS: %3.2f +/- %3.2f GTEPS, min/max %3.2f/%3.2f GTEPS\n",
            1e-9 * bfs_stats.hmean, 1e-9 * bfs_stats.stddev,
            1e-9 * bfs_stats.min,  1e-9 * bfs_stats.max);
        LOG("    Connected Components: %3.2f +/- %3.2f GTEPS, min/max %3.2f/%3.2f GTEPS\n",
            1e-9 * cc_stats.hmean, 1e-9 * cc_stats.stddev,
            1e-9 * cc_stats.min,  1e-9 * cc_stats.max);
        LOG("    PageRank: %3.2f +/- %3.2f MFLOPS, min/max %3.2f/%3.2f MFLOPS\n",
            1e-6 * pr_stats.hmean, 1e-6 * pr_stats.stddev,
            1e-6 * pr_stats.min,  1e-6 * pr_stats.max);
        LOG("    Triangle Counting: %3.2f +/- %3.2f GTPPS, min/max %3.2f/%3.2f GTPPS\n",
            1e-9 * tc_stats.hmean, 1e-9 * tc_stats.stddev,
            1e-9 * tc_stats.min,  1e-9 * tc_stats.max);
    }

    return !success;
}