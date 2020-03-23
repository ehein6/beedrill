#include <vector>
#include <emu_cxx_utils/replicated.h>
#include "ktruss_graph.h"
#include "worklist.h"

class ktruss
{
public:
    struct stats {
        // Maximum truss size found
        long max_k = 0;
        // Number of triangle count iterations
        long num_iters = 0;
        // Number of edges in each truss
        std::vector<long> edges_per_truss;
        // Number of vertices in each truss
        std::vector<long> vertices_per_truss;
        // NOTE There is no 0-truss or 1-truss, so edges_per_truss[0] is used
        // for the 2-truss, and so on.
    };
private:
    emu::repl<ktruss_graph*> g_;
    // Instead of actually removing edges from the graph, we will gradually
    // move this pointer to the end of the edge list backwards
    emu::striped_array<ktruss_graph::edge_iterator> active_edges_end_;
    // Max K-truss value per vertex
    emu::striped_array<long> vertex_max_k_;
    // Number of edges removed in current step
    emu::repl<long> num_removed_;
    // Work list of edges to process
    worklist<ktruss_graph::edge_type> worklist_;

    // Counts the number of triangles per edge
    void count_triangles();
    // Removes edges with triangle count < k-2
    long remove_edges(long k);
    // Counts the number of vertices in each truss
    std::vector<long> compute_truss_sizes(long max_k);
    // Handles special logic for quitting the algorithm early
    stats early_exit(long k, stats& s);
public:
    explicit ktruss(ktruss_graph& g);
    ktruss(const ktruss& other, emu::shallow_copy);

    stats run(long k_limit);
    void clear();
    bool check();
    void dump_graph();
};
