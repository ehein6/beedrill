#include "ktruss_graph.h"

// Instantiate ktruss graph class
template class graph_base<ktruss_edge_slot>;
// Instantiate factory method
template<>
std::unique_ptr<emu::repl_shallow<ktruss_graph>>
create_graph_from_edge_list(dist_edge_list & dist_el);
