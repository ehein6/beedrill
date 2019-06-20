#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "dist_edge_list.h"
#include "graph_from_edge_list.h"
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
    {NULL}
};

void
print_help(const char* argv0)
{
    LOG( "Usage: %s [OPTIONS]\n", argv0);
    LOG("\t--graph_filename     Path to graph file to load\n");
    LOG("\t--distributed_load   Load the graph from all nodes at once (File must exist on all nodes, use absolute path).\n");
    LOG("\t--num_trials         Run BFS this many times.\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--check_results      Validate the BFS results (slow)\n");
    LOG("\t--help               Print command line help\n");
}

typedef struct tc_args {
    const char* graph_filename;
    bool distributed_load;
    long num_trials;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
    bool check_results;
} tc_args;

struct tc_args
parse_args(int argc, char *argv[])
{
    tc_args args;
    args.graph_filename = NULL;
    args.distributed_load = false;
    args.num_trials = 1;
    args.dump_edge_list = false;
    args.check_graph = false;
    args.dump_graph = false;
    args.check_results = false;

    int option_index;
    while (true)
    {
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

int
main(int argc, char ** argv)
{
    // Set active region for hooks
    const char* active_region = getenv("HOOKS_ACTIVE_REGION");
    if (active_region != NULL) {
        hooks_set_active_region(active_region);
    }

    // Parse command-line argumetns
    tc_args args = parse_args(argc, argv);

    // Load the edge list
    if (args.distributed_load) {
        load_edge_list_distributed(args.graph_filename);
    } else {
        load_edge_list(args.graph_filename);
    }
    if (args.dump_edge_list) {
        LOG("Dumping edge list...\n");
        dump_edge_list();
    }

    // Build the graph
    LOG("Constructing graph...\n");
    construct_graph_from_edge_list(LONG_MAX); // No heavy vertices for TC
    LOG("Sorting edge blocks...\n");
    sort_edge_blocks();
    print_graph_distribution();
    hooks_set_attr_i64("num_undirected_edges", G.num_edges/2);
    hooks_set_attr_i64("num_vertices", G.num_vertices);
    if (args.check_graph) {
        LOG("Checking graph...");
        if (check_graph()) {
            LOG("PASS\n");
        } else {
            LOG("FAIL\n");
        };
    }
    if (args.dump_graph) {
        LOG("Dumping graph...\n");
        dump_graph();
    }

    // Initialize the algorithm
    LOG("Initializing TC data structures...\n");
    tc_init();

    for (long trial = 0; trial < args.num_trials; ++trial) {
        LOG("Counting triangles (trial %li of %li)\n",
            trial + 1, args.num_trials);
        // Run the triangle count
        hooks_region_begin("tc");
        tc_run();
        double time_ms = hooks_region_end();
        if (args.check_results) {
            LOG("Checking results...\n");
            if (tc_check()) {
                LOG("PASS\n");
            } else {
                LOG("FAIL\n");
            }
        }
        // Output results
        LOG("Found %li triangles in %3.2f ms\n",
            TC.num_triangles, time_ms
        );
        // Reset for next run
        tc_data_clear();
    }
    return 0;
}