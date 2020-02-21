#include <emu_cxx_utils/replicated.h>
#include "graph.h"
#include "worklist.h"

class triangle_count
{
private:
    emu::repl<graph*> g_;
    // Reduction variable for counting the number of triangles in the graph
    emu::repl<long> num_triangles_;
    // Reduction variable for number of two-paths in the graph
    emu::repl<long> num_twopaths_;
    // Work list of edges to process
    worklist worklist_;

public:
    explicit triangle_count(graph& g);
    triangle_count(const triangle_count& other, emu::shallow_copy);

    struct stats {
        long num_triangles;
        long num_twopaths;
    };

    stats run();
    void clear();
    bool check();
};
