#include <vector>
#include <emu_cxx_utils/replicated.h>
#include "ktruss_graph.h"
#include "worklist.h"

class ktruss
{
public:
    struct stats {
        // Maximum truss size found
        long max_k;
        // Number of edges in each truss
        std::vector<long> edges_per_truss;
        // Number of vertices in each truss
        std::vector<long> vertices_per_truss;
        // NOTE There is no 0-truss or 1-truss, so edges_per_truss[0] is used
        // for the 2-truss, and so on.
        explicit stats(long k)
        : max_k(k)
        , edges_per_truss(k-1, 0)
        , vertices_per_truss(k-1, 0)
        {}
    };
private:
    emu::repl<ktruss_graph*> g_;
    // Instead of actually removing edges from the graph, we will gradually
    // move this pointer to the end of the edge list backwards
    emu::striped_array<ktruss_graph::edge_iterator> active_edges_end_;
    // Max K-truss value per vertex
    emu::striped_array<long> vertex_max_k_;
    // Work list of edges to process
    worklist<ktruss_graph::edge_type> worklist_;

    void count_triangles();
    long remove_edges(long k);
    stats compute_truss_sizes(long max_k);

public:
    explicit ktruss(ktruss_graph& g);
    ktruss(const ktruss& other, emu::shallow_copy);

    stats run();
    void clear();
    bool check();
    void dump_graph();
};
