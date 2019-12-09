#include <emu_cxx_utils/replicated.h>
#include "graph.h"

class components
{
private:
    emu::repl<graph*> g_;
    // component that this vertex belongs to
    emu::striped_array<long> component_;

public:
    explicit components(graph& g);
    components(const components& other, emu::shallow_copy);
    long run();
    void clear();
    bool check();
};
