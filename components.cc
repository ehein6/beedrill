#include "components.h"
#include <cassert>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <queue>
#include <unordered_map>
#include <emu_cxx_utils/fill.h>

using namespace emu;
using namespace emu::parallel;


components::components(graph & g)
: g_(&g)
, component_(g.num_vertices())
, component_size_(g.num_vertices())
, num_components_(g.num_vertices())
{
    clear();
}

components::components(const components& other, emu::shallow_copy shallow)
: g_(other.g_)
, component_(other.component_, shallow)
, component_size_(other.component_size_, shallow)
, num_components_(other.num_components_)
{}

void
components::clear()
{
}

components::stats
components::run()
{
    // Put each vertex in its own component
    cilk_spawn parallel::for_each(component_.begin(), component_.end(),
        [this](long & c) {
            long index = &c - component_.begin();
            c = index;
        }
    );
    // Set size of each component to zero
    parallel::fill(component_size_.begin(), component_size_.end(), 0L);
    cilk_sync;

    long num_iters;
    for (num_iters = 1; ; ++num_iters) {
        long changed = 0;
        // For all edges that connect vertices in different components...
        g_->for_each_vertex([this, &changed](long src) {
            g_->for_each_out_edge(src, [this, src, &changed](long dst) {
                long comp_src = component_[src];
                long comp_dst = component_[dst];
                if (comp_src == comp_dst) { return; }

                // Assign the lower component ID to both
                // This algorithm is stable for undirected graphs
                long high_comp = std::max(comp_src, comp_dst);
                long low_comp = std::min(comp_src, comp_dst);
                if (high_comp == component_[high_comp]) {
                    changed = 1;
                    component_[high_comp] = low_comp;
                }
            });
        });

        // No changes? We're done!
        if (!changed) break;

        g_->for_each_vertex([this](long v) {
            while (component_[v] != component_[component_[v]]) {
                component_[v] = component_[component_[v]];
            }
        });
    }
    // Count up the size of each component
    parallel::for_each(component_.begin(), component_.end(),
        [this](long c) { emu::remote_add(&component_size_[c], 1); }
    );

    stats s;
    s.num_iters = num_iters;

    // TODO should use parallel count_if here (can use transform_reduce)
    // s.num_components = parallel::count_if(
    //     component_size_.begin(), component_size_.end(),
    //     [](long size) { return size > 0; }
    // );

    // Count number of components
    num_components_ = 0;
    for_each(component_size_.begin(), component_size_.end(),
        [&](long size) {
            if (size > 0) { emu::remote_add(&num_components_, 1); }
        }
    );
    s.num_components = emu::repl_reduce(num_components_, std::plus<>());

    return s;
}

void
components::dump()
{
    long max_component = *std::max_element(component_.begin(), component_.end());

    auto print_range = [](bool first_entry, long c, long first, long last) {
        if (first_entry) {
            printf("Component %li: ", c);
        } else {
            printf(", ");
        }
        if (first == last) {
            printf("%li", first);
        } else {
            printf("%li-%li", first, last);
        }
    };

    // For each component...
    for (long c = 0; c <= max_component; ++c) {
        long range_start = -1;
        bool first_entry = true;
        for (long v = 0; v < g_->num_vertices(); ++v) {
            // Is the vertex in the component?
            if (c == component_[v]) {
                // Record the start of a range of vertices
                if (range_start < 0) { range_start = v; }
            } else {
                // End of the range
                if (range_start >= 0) {
                    // Print start of line only once, omit for empty components
                    // Print vertices as a range if possible
                    print_range(first_entry, c, range_start, v - 1);
                    // Reset range
                    range_start = -1;
                    first_entry = false;
                }
            }
        }
        // Print last range
        if (range_start >= 0) {
            print_range(first_entry, c, range_start, g_->num_vertices() - 1);
        }
        if (!first_entry) { printf("\n"); }
        fflush(stdout);
    }
}

// Do serial BFS on each component to check labels
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
