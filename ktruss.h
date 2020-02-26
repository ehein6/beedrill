#include <emu_cxx_utils/replicated.h>
#include "ktruss_graph.h"
#include "worklist.h"

class ktruss
{
public:
    struct stats {
        long max_k;
        std::vector<long> edges_per_truss;
        std::vector<long> vertices_per_truss;
        explicit stats(long k)
        : max_k(k)
        , edges_per_truss(k, 0)
        , vertices_per_truss(k, 0)
        {}
    };
private:
    emu::repl<ktruss_graph*> g_;
    // Max K-truss value per vertex
    emu::striped_array<long> vertex_max_k_;
    // Work list of edges to process
    worklist<ktruss_graph::edge_type> worklist_;

    // Helper functions
    void build_worklist();
    void count_initial_triangles();
    void unroll_wedges(long k);
    void unroll_supported_triangles(long k);
    long remove_edges(long k);
    stats compute_truss_sizes(long max_k);

public:
    explicit ktruss(ktruss_graph& g);
    ktruss(const ktruss& other, emu::shallow_copy);

    stats run();
    void clear();
    bool check();
};
