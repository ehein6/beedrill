#include "graph.h"

// Instantiate basic graph class
template class graph_base<edge_slot>;
// Instantiate factory function
template std::unique_ptr<emu::repl_shallow<graph>>
create_graph_from_edge_list(dist_edge_list & dist_el);
