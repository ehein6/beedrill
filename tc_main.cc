#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "graph.h"
#include "dist_edge_list.h"
#include "tc.h"

const struct option long_options[] = {
    {"graph_filename"   , required_argument},
    {"distributed_load" , no_argument},
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
    LOG("\t--distributed_load   Load the graph from all nodes at once (File must exist on all nodes, use absolute path).\n");
    LOG("\t--num_trials         Run BFS this many times.\n");
    LOG("\t--source_vertex      Use this as the source vertex. If unspecified, pick random vertices.\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--check_results      Validate the BFS results (slow)\n");
    LOG("\t--help               Print command line help\n");
}

struct tc_args
{
    const char* graph_filename;
    bool distributed_load;
    long num_trials;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
    bool check_results;

    static tc_args
    parse(int argc, char *argv[])
    {
        tc_args args = {};
        args.graph_filename = NULL;
        args.distributed_load = false;
        args.num_trials = 1;
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
            } else if (!strcmp(option_name, "distributed_load")) {
                args.distributed_load = true;
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


int main(int argc, char ** argv)
{
    bool success = true;
    // Set active region for hooks
    const char* active_region = getenv("HOOKS_ACTIVE_REGION");
    if (active_region != NULL) {
        hooks_set_active_region(active_region);
    } else {
        hooks_set_active_region("tc");
    }

    // Parse command-line arguments
    tc_args args = tc_args::parse(argc, argv);

    // Load edge list from file
    auto dist_el = dist_edge_list::load(args.graph_filename);
    if (args.dump_edge_list) {
        LOG("Dumping edge list...\n");
        dist_el->dump();
    }

    // Build the graph
    LOG("Constructing graph...\n");
    auto g = graph::from_edge_list(*dist_el);
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

    // Initialize the algorithm
    LOG("Initializing TC data structures...\n");
    auto tc = emu::make_repl_shallow<triangle_count>(*g);

    // Run multiple trials of the algorithm
    for (long trial = 0; trial < args.num_trials; ++trial) {
        // Clear out the data structures
        tc->clear();

        LOG("Counting triangles...\n");
        hooks_region_begin("tc");
        triangle_count::stats s = tc->run();
        hooks_set_attr_i64("num_triangles", s.num_triangles);
        hooks_set_attr_i64("num_twopaths", s.num_twopaths);
        double time_ms = hooks_region_end();

        LOG("Found %li triangles and %li two-paths in %3.2f ms, %3.2f MTPPS \n",
            s.num_triangles,
            s.num_twopaths,
            time_ms,
            (1e-9 * s.num_twopaths) / (1e-3 * time_ms)
        );
    }

    if (args.check_results) {
        LOG("Checking results...\n");
        if (tc->check()) {
            LOG("PASS\n");
        } else {
            LOG("FAIL\n");
            success = false;
        }
    }

    return !success;
}