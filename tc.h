#include <emu_cxx_utils/replicated.h>
#include "graph.h"

class tc {
private:
    emu::repl<graph*> g_;
    // Number of triangles this vertex is a part of
    emu::repl<long> num_triangles_;
public:

    explicit tc(graph& g);
    tc(const tc& other, emu::shallow_copy);
    long run();
    void clear();
    bool check();
};
