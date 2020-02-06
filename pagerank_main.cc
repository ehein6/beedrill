#include <getopt.h>

#include "graph.h"
#include "dist_edge_list.h"
#include "pagerank.h"

const struct option long_options[] = {
    {"graph_filename"   , required_argument},
    {"distributed_load" , no_argument},
    {"num_trials"       , required_argument},
    {"max_iterations"   , required_argument},
    {"epsilon"          , required_argument},
    {"damping"          , required_argument},
    {"sort_edge_blocks" , no_argument},
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
    LOG("\t--max_iterations     Maximum number of iterations.\n");
    LOG("\t--epsilon            Error tolerance; run until aggregate score change is less than epsilon.\n");
    LOG("\t--damping            Damping factor for pagerank.\n");
    LOG("\t--sort_edge_blocks   Sort edge blocks to group neighbors by home nodelet.\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--check_results      Validate the BFS results (slow)\n");
    LOG("\t--help               Print command line help\n");
}

struct pagerank_args
{
    const char* graph_filename = NULL;
    bool distributed_load = false;
    long num_trials = 1;
    long max_iterations = 20;
    double epsilon = 1e-5;
    double damping = 0.85;
    bool sort_edge_blocks = false;
    bool dump_edge_list = false;
    bool check_graph = false;
    bool dump_graph = false;
    bool check_results = false;

    static pagerank_args
    parse(int argc, char *argv[])
    {
        pagerank_args args = {};

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
            } else if (!strcmp(option_name, "max_iterations")) {
                args.max_iterations = atol(optarg);
            } else if (!strcmp(option_name, "epsilon")) {
                args.epsilon = atof(optarg);
            } else if (!strcmp(option_name, "damping")) {
                args.damping = atof(optarg);
            } else if (!strcmp(option_name, "sort_edge_blocks")) {
                args.sort_edge_blocks = true;
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
        if (args.max_iterations <= 0) { LOG( "max_iterations must be > 0\n"); exit(1); }
        if (args.damping <= 0) { LOG( "damping must be > 0\n"); exit(1); }
        if (args.epsilon < 0) { LOG( "epsilon must not be negative\n"); exit(1); }
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
        hooks_set_active_region("pagerank");
    }

    // Parse command-line arguments
    auto args = pagerank_args::parse(argc, argv);

    // Load edge list from file
    auto dist_el = dist_edge_list::load(args.graph_filename);
    if (args.dump_edge_list) {
        LOG("Dumping edge list...\n");
        dist_el->dump();
    }

    // Build the graph
    LOG("Constructing graph...\n");
    auto g = graph::from_edge_list(*dist_el);
    if (args.sort_edge_blocks) {
        LOG("Sorting edge lists by nodelet...\n");
        g->sort_edge_lists([](long lhs, long rhs) {
            unsigned long nlet_mask = NODELETS() - 1;
            unsigned long lhs_nlet = lhs & nlet_mask;
            unsigned long rhs_nlet = rhs & nlet_mask;
            return lhs_nlet < rhs_nlet;
        });
    }

    // Print graph statistics
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
    LOG("Initializing PageRank data structures...\n");

    auto pr = emu::make_repl_copy<pagerank>(*g);

    // Run trials
    double time_ms_all_trials = 0;
    for (long s = 0; s < args.num_trials; ++s) {
        // Clear out the bfs data structures
        pr->clear();

        LOG("Computing PageRank...\n");
        // Run the BFS
        hooks_region_begin("pagerank");
        int num_iters = pr->run(args.max_iterations, args.damping, args.epsilon);
        hooks_set_attr_i64("num_iters", num_iters);
        double time_ms = hooks_region_end();
        if (args.check_results) {
            LOG("Checking results...\n");
            if (pr->check(args.damping, args.epsilon)) {
                LOG("PASS\n");
            } else {
                LOG("FAIL\n");
                success = false;
            }
        }

        // Compute FLOPS per iteration:
        // 1 FLOP per vertex: (contrib = score / degree)
        // 1 FLOP per edge: (incoming += contrib[dst])
        // 2 FLOPs per vertex: (scores_[src] = base_score + damping * incoming);
        // 2 FLOPs per vertex: error_ += fabs(scores_[src] - old_score);
        // Total 5 flops per vertex and 1 FLOP per edge, per iteration
        double flops = num_iters * (5 * g->num_vertices() + 1 * g->num_edges());

        // Compute memory traffic per iteration:
        // 24 bytes per vertex: (contrib = score / degree)
        // 8 bytes per edge: (incoming += contrib[dst])
        // 8 bytes per vertex: double old_score = scores_[src];
        // 8 bytes per vertex: (scores_[src] = base_score + damping * incoming);
        // 16 bytes per vertex: error_ += fabs(scores_[src] - old_score);
        // Total 56 bytes per vertex and 8 bytes per edge, per iteration
        long bytes = num_iters * (56 * g->num_vertices() + 8 * g->num_edges());

        // Output results
        time_ms_all_trials += time_ms;
        LOG("Computed PageRank in %i iterations (%3.2f ms, %3.0f MFLOPS, %3.0f MB/s) \n",
            num_iters, time_ms,
            1e-6*flops/(time_ms*1e-3),
            1e-6*bytes/(time_ms*1e-3));
    }
    return !success;
}