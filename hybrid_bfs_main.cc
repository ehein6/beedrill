#include <cstring>
#include <getopt.h>

#include "graph.h"
#include "dist_edge_list.h"
#include "hybrid_bfs.h"
#include "lcg.h"
#include "git_sha1.h"

const struct option long_options[] = {
    {"graph_filename"   , required_argument},
    {"distributed_load" , no_argument},
    {"heavy_threshold"  , required_argument},
    {"num_trials"       , required_argument},
    {"source_vertex"    , required_argument},
    {"max_level"        , required_argument},
    {"algorithm"        , required_argument},
    {"alpha"            , required_argument},
    {"beta"             , required_argument},
    {"sort_edge_blocks" , no_argument},
    {"dump_edge_list"   , no_argument},
    {"check_graph"      , no_argument},
    {"dump_graph"       , no_argument},
    {"check_results"    , no_argument},
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
    LOG("\t--heavy_threshold    Vertices with this many neighbors will be spread across nodelets\n");
    LOG("\t--num_trials         Run BFS this many times.\n");
    LOG("\t--source_vertex      Use this as the source vertex. If unspecified, pick random vertices.\n");
    LOG("\t--max_level          Stop when the BFS tree is this many levels deep (for K-hop benchmark).\n");
    LOG("\t--algorithm          Select BFS implementation to run\n");
    LOG("\t--alpha              Alpha parameter for direction-optimizing BFS\n");
    LOG("\t--beta               Beta parameter for direction-optimizing BFS\n");
    LOG("\t--sort_edge_blocks   Sort edge blocks to group neighbors by home nodelet.\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--check_results      Validate the BFS results (slow)\n");
    LOG("\t--version            Print git version info\n");
    LOG("\t--help               Print command line help\n");
}

struct bfs_args
{
    const char* graph_filename;
    bool distributed_load;
    long num_trials;
    long source_vertex;
    long max_level;
    const char* algorithm;
    long alpha;
    long beta;
    bool sort_edge_blocks;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
    bool check_results;

    static bfs_args
    parse(int argc, char *argv[])
    {
        bfs_args args = {};
        args.graph_filename = NULL;
        args.distributed_load = false;
        args.num_trials = 1;
        args.source_vertex = -1;
        args.max_level = std::numeric_limits<long>::max();
        args.algorithm = "beamer_hybrid";
        args.alpha = 15;
        args.beta = 18;
        args.sort_edge_blocks = false;
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
            } else if (!strcmp(option_name, "source_vertex")) {
                args.source_vertex = atol(optarg);
            } else if (!strcmp(option_name, "max_level")) {
                args.max_level = atol(optarg);
            } else if (!strcmp(option_name, "algorithm")) {
                args.algorithm = optarg;
            } else if (!strcmp(option_name, "alpha")) {
                args.alpha = atol(optarg);
            } else if (!strcmp(option_name, "beta")) {
                args.beta = atol(optarg);
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
        if (args.max_level <= 0) { LOG( "max_level must be > 0\n"); exit(1); }
        if (args.alpha <= 0) { LOG( "alpha must be > 0\n"); exit(1); }
        if (args.beta <= 0) { LOG( "beta must be > 0\n"); exit(1); }

        if (args.check_results
        && args.max_level != std::numeric_limits<long>::max()) {
            // TODO: make hybrid_bfs::check handle max_level
            LOG("Can't check results when max_level is set");
            exit(1);
        }
        return args;
    }
};


long
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
    if (active_region != NULL) {
        hooks_set_active_region(active_region);
    } else {
        hooks_set_active_region("bfs");
    }
    hooks_set_attr_str("git_tag", g_GIT_TAG);

    // Initialize RNG with deterministic seed
    lcg rng(0);

    // Parse command-line arguments
    bfs_args args = bfs_args::parse(argc, argv);

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
    auto g = create_graph_from_edge_list<graph>(*dist_el);
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

    // Check for valid source vertex
    if (args.source_vertex >= g->num_vertices()) {
        LOG("Source vertex %li out of range.\n", args.source_vertex);
        exit(1);
    }

    // Quit early if algorithm == none
    if (!strcmp(args.algorithm, "none")) {
        return !success;
    }

    // Initialize the algorithm
    LOG("Initializing BFS data structures...\n");
    hooks_set_attr_str("algorithm", args.algorithm);

    enum algorithm {
        REMOTE_WRITES,
        MIGRATING_THREADS,
        REMOTE_WRITES_HYBRID,
        BEAMER_HYBRID,
    } alg;
    if        (!strcmp(args.algorithm, "remote_writes")) {
        alg = REMOTE_WRITES;
    } else if (!strcmp(args.algorithm, "migrating_threads")) {
        alg = MIGRATING_THREADS;
    } else if (!strcmp(args.algorithm, "remote_writes_hybrid")) {
        alg = REMOTE_WRITES_HYBRID;
    } else if (!strcmp(args.algorithm, "beamer_hybrid")) {
        alg = BEAMER_HYBRID;
    } else {
        LOG("Algorithm '%s' not implemented!\n", args.algorithm);
        exit(1);
    }
    auto bfs = emu::make_repl_shallow<hybrid_bfs>(*g);

    // Run trials
    long num_edges_traversed_all_trials = 0;
    double time_ms_all_trials = 0;
    long source;
    for (long s = 0; s < args.num_trials; ++s) {
        // Randomly pick a source vertex with positive degree
        if (args.source_vertex >= 0) {
            source = args.source_vertex;
        } else {
            source = pick_random_vertex(*g, rng);
        }

        // Clear out the bfs data structures
        bfs->clear();

        LOG("Doing breadth-first search from vertex %li (sample %li of %li)\n",
            source, s + 1, args.num_trials);
        // Run the BFS
        hooks_set_attr_i64("source_vertex", source);
        hooks_region_begin("bfs");
        switch(alg) {
            case (REMOTE_WRITES):
                bfs->run_with_remote_writes(source, args.max_level);
                break;
            case (MIGRATING_THREADS):
                bfs->run_with_migrating_threads(source, args.max_level);
                break;
            case (REMOTE_WRITES_HYBRID):
                bfs->run_with_remote_writes_hybrid(source, args.max_level, args.alpha, args.beta);
                break;
            case (BEAMER_HYBRID):
                bfs->run_beamer(source, args.max_level, args.alpha, args.beta);
                break;
        }
        double time_ms = hooks_region_end();
        if (args.check_results) {
            LOG("Checking results...\n");
            if (bfs->check(source)) {
                LOG("PASS\n");
            } else {
                LOG("FAIL\n");
                success = false;
                // bfs->print_tree();
            }
        }
        // Output results
        hybrid_bfs::stats stats = bfs->compute_stats();
        num_edges_traversed_all_trials += stats.num_edges_traversed;
        time_ms_all_trials += time_ms;
        LOG("Traversed %li edges in %3.2f ms, %3.2f MTEPS. %li levels in BFS tree.\n",
            stats.num_edges_traversed,
            time_ms,
            (1e-6 * stats.num_edges_traversed) / (time_ms / 1000),
            stats.max_level
        );
    }

    LOG("Mean performance over all trials: %3.2f MTEPS \n",
        (1e-6 * num_edges_traversed_all_trials) / (time_ms_all_trials / 1000)
    );

    return !success;
}
