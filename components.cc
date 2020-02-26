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
, worklist_(g.num_vertices())
, component_(g.num_vertices())
, component_size_(g.num_vertices())
, num_components_(g.num_vertices())
, changed_(false)
{
    clear();
}

components::components(const components& other, emu::shallow_copy shallow)
: g_(other.g_)
, worklist_(other.worklist_, shallow)
, component_(other.component_, shallow)
, component_size_(other.component_size_, shallow)
, num_components_(other.num_components_)
, changed_(other.changed_)
{}

void
components::clear()
{
}

components::stats
components::run()
{
    worklist_.clear_all();
    for_each(fixed, g_->vertices_begin(), g_->vertices_end(), [this] (long v){
        // Put each vertex in its own component
        component_[v] = v;
        // Set size of each component to zero
        component_size_[v] = 0;
        // Build the worklist for the first iteration
        // Later, we do this during the tree-climbing step
        worklist_.append(v, g_->out_edges_begin(v), g_->out_edges_end(v));
    });

    long num_iters;
    for (num_iters = 1; ; ++num_iters) {
        changed_ = false;
        // For all edges that connect vertices in different components...
        worklist_.process_all_edges(dynamic_policy<64>(),
            [this](long src, long dst) {
                long &comp_src = component_[src];
                long &comp_dst = component_[dst];
                if (comp_dst < comp_src) {
                    comp_src = comp_dst;
                    changed_ = true;
                }
            }
        );

        // No changes? We're done!
        if (!repl_reduce(changed_, std::logical_or<>())) break;

        worklist_.clear_all();
        g_->for_each_vertex(fixed, [this](long v) {
            // Merge connected components
            while (component_[v] != component_[component_[v]]) {
                component_[v] = component_[component_[v]];
            }
            // Add this vertex to the worklist for the next step
            worklist_.append(v, g_->out_edges_begin(v), g_->out_edges_end(v));
        });
    }
    // Count up the size of each component
    for_each(fixed, component_.begin(), component_.end(),
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
            auto edges_begin = g_->out_neighbors(u);
            auto edges_end = edges_begin + g_->out_degree(u);
            for (auto e = edges_begin; e < edges_end; ++e) {
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
