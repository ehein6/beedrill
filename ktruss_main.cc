#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "graph.h"
#include "dist_edge_list.h"
#include "ktruss.h"
#include "git_sha1.h"

const struct option long_options[] = {
    {"graph_filename"   , required_argument},
    {"distributed_load" , no_argument},
    {"num_trials"       , required_argument},
    {"dump_edge_list"   , no_argument},
    {"check_graph"      , no_argument},
    {"dump_graph"       , no_argument},
    {"check_results"    , no_argument},
    {"k_limit"          , required_argument},
    {"version"          , no_argument},
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
    LOG("\t--k_limit            Stop after reaching this value of k.\n");
    LOG("\t--version            Print git version info\n");
    LOG("\t--help               Print command line help\n");
}

struct ktruss_args
{
    const char* graph_filename;
    bool distributed_load;
    long num_trials;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
    bool check_results;
    long k_limit = std::numeric_limits<long>::max();

    static ktruss_args
    parse(int argc, char *argv[])
    {
        ktruss_args args = {};
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
            } else if (!strcmp(option_name, "k_limit")) {
                args.k_limit = atol(optarg);
            } else if (!strcmp(option_name, "version")) {
                LOG("%s\n", g_GIT_TAG);
                exit(0);
            } else if (!strcmp(option_name, "help")) {
                print_help(argv[0]);
                exit(1);
            }
        }
        if (args.graph_filename == NULL) { LOG( "Missing graph filename\n"); exit(1); }
        if (args.num_trials <= 0) { LOG( "num_trials must be > 0\n"); exit(1); }
        if (args.k_limit < 3) { LOG( "k_limit must be >= 3\n"); exit(1); }
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
        hooks_set_active_region("ktruss");
    }
    hooks_set_attr_str("git_tag", g_GIT_TAG);

    // Parse command-line arguments
    ktruss_args args = ktruss_args::parse(argc, argv);

    // Load edge list from file
    dist_edge_list::handle dist_el;
    hooks_region_begin("load_edge_list");
    if (args.distributed_load) {
        dist_el = dist_edge_list::load_distributed(args.graph_filename);
    } else {
        dist_el = dist_edge_list::load_binary(args.graph_filename);
    }
    hooks_set_attr_i64("num_edges", dist_el->num_edges());
    hooks_set_attr_i64("num_vertices", dist_el->num_vertices());
    auto load_time_ms = hooks_region_end();
    LOG("Loaded %li edges in %3.2f ms, %3.2f MB/s\n",
        dist_el->num_edges(),
        load_time_ms,
        (1e-6 * dist_el->num_edges() * sizeof(edge)) / (1e-3 * load_time_ms));
    if (args.dump_edge_list) {
        LOG("Dumping edge list...\n");
        dist_el->dump();
    }

    // Build the graph
    LOG("Constructing graph...\n");
    auto g = create_graph_from_edge_list<ktruss_graph>(*dist_el);
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

    // Initialize the algorithm
    LOG("Initializing ktruss data structures...\n");
    auto kt = emu::make_repl_shallow<ktruss>(*g);
    kt->clear();

    if (args.dump_graph) {
        LOG("Dumping graph...\n");
        // Note: using ktruss's dump_graph, which treats the graph as directed
        kt->dump_graph();
    }

    // Run multiple trials of the algorithm
    for (long trial = 0; trial < args.num_trials; ++trial) {
        // Clear out the data structures
        kt->clear();

        LOG("Computing K-truss...\n");
        hooks_region_begin("ktruss");
        ktruss::stats s = kt->run(args.k_limit);
        hooks_set_attr_i64("max_k", s.max_k);
        hooks_set_attr_i64("num_iters", s.num_iters);
        double time_ms = hooks_region_end();

        LOG("Computed k-truss in %3.2f ms, max k is %li\n", time_ms, s.max_k);
        for (long k = 2; k <= s.max_k; ++k) {
            LOG("\t%li-truss: %li vertices and %li edges\n",
            // NOTE There is no 0-truss or 1-truss, so edges_per_truss[0] is
            // used for the 2-truss, and so on
            k, s.vertices_per_truss[k-2], s.edges_per_truss[k-2]);
        }
    }

    if (args.check_results) {
        LOG("Checking results...\n");
        if (kt->check()) {
            LOG("PASS\n");
        } else {
            LOG("FAIL\n");
            success = false;
        }
    }

    return !success;
}