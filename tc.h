#include <emu_cxx_utils/replicated.h>
#include "graph.h"

class triangle_count
{
private:
    emu::repl<graph*> g_;
    // Number of triangles this vertex is a part of
    emu::repl<long> num_triangles_;

    void count_triangles(long u);
public:
    explicit triangle_count(graph& g);
    triangle_count(const triangle_count& other, emu::shallow_copy);
    long run();
    void clear();
    bool check();
};
