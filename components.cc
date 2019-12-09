#include "components.h"
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <queue>
#include <unordered_map>

using namespace emu;
using namespace emu::parallel;
using namespace emu::execution;

components::components(graph & g)
: g_(&g)
, component_(g.num_vertices())
{
    clear();
}

components::components(const components& other, emu::shallow_copy shallow)
: g_(other.g_)
, component_(other.component_, shallow)
{}

void
components::clear()
{
}

long
components::run()
{
    // Put each vertex in its own component
    parallel::for_each(component_.begin(), component_.end(), [this](long & c) {
        long index = &c - component_.begin();
        c = index;
    });


    while (true) {
        long changed = 0;
        g_->for_each_vertex([this, &changed](long src) {
            g_->for_each_out_edge(src, [this, src, &changed](long dst) {
                long comp_src = component_[src];
                long comp_dst = component_[dst];
                if (comp_src == comp_dst) { return; }

                long high_comp = std::max(comp_src, comp_dst);
                long low_comp = std::min(comp_src, comp_dst);
                if (high_comp == component_[high_comp]) {
                    changed = 1;
                    component_[high_comp] = low_comp;
                }
            });
        });

        if (!changed) break;

        g_->for_each_vertex([this](long v) {
            while (component_[v] != component_[component_[v]]) {
                component_[v] = component_[component_[v]];
            }
        });
    }

    // TODO return unique number of components
    return 0;
}

// Do serial triangle count
bool
components::check()
{
    // Make sure we reach each vertex at least once
    std::vector<long> visited(g_->num_vertices(), 0);

    // Build a map from component labels to a vertex in that component
    std::unordered_map<long, long> label_to_source;
    for (long v = 0; v < g_->num_vertices(); ++v) {
        label_to_source[component_[v]] = v;
    }

    for (auto p : label_to_source) {
        long my_component = p.first;
        long source = p.second;
        visited[source] = 1;

        // Do a serial BFS
        // Push source into the queue
        std::queue<long> q;
        q.push(source);
        // For each vertex in the queue...
        while (!q.empty()) {
            long u = q.front(); q.pop();
            // For each out-neighbor of this vertex...
            long * edges_begin = g_->out_neighbors(u);
            long * edges_end = edges_begin + g_->out_degree(u);
            for (long * e = edges_begin; e < edges_end; ++e) {
                long v = *e;
                // Check that all neighbors have the same component ID
                if (component_[v] != my_component) {
                    LOG("Connected vertices in different components: \n");
                    LOG("%li (component %li) -> %li (component %li)\n",
                        source, my_component, v, component_[v]);
                    return false;
                }
                // Add unexplored neighbors to the queue
                if (!visited[v]) {
                    visited[v] = 1;
                    q.push(v);
                }
            }
        }
    }

    // Make sure we visited all vertices
    for (long v = 0; v < g_->num_vertices(); ++v) {
        if (visited[v] == 0) {
            LOG("Failed to visit %li\n", v);
            return false;
        }
    }

    return true;
}
