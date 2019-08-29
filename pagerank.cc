#include "pagerank.h"
#include <vector>
#include <emu_cxx_utils/execution_policy.h>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/fill.h>


using namespace emu;
using namespace emu::execution;

pagerank::pagerank(graph & g)
: g_(&g)
, scores_(g.num_vertices())
, contrib_(g.num_vertices())
, error_(0)
{}

// Shallow copy constructor
pagerank::pagerank(const pagerank& other, emu::shallow_copy tag)
: g_(other.g_)
, scores_(other.scores_, tag)
, contrib_(other.contrib_, tag)
, error_(other.error_)
{}

// Allows us to add to a replicated double
// Uses a CAS loop, since we don't have float atomics
void
operator+=(emu::repl<double>& lhs, double rhs)
{
    // Get a pointer to the local copy
    double * my_view = &lhs;
    double new_value, old_value;
    do {
        // Read the value from memory
        old_value = *my_view;
        // Do the add
        new_value = old_value + rhs;
        // Do again if the old value doesn't match
    } while (old_value != atomic_cas(my_view, old_value, new_value));
}

void
pagerank::run (int max_iters, double damping, double epsilon)
{
    double init_score = 1.0 / g_->num_vertices();
    double base_score = (1.0 - damping) / g_->num_vertices();
    parallel::fill(scores_.begin(), scores_.end(), init_score);
    for (int iter = 0; iter < max_iters; ++iter) {
        error_ = 0;

        g_->for_each_vertex([&](long v) {
            // Compute outgoing contribution for each vertex
            contrib_[v] = scores_[v] / g_->out_degree(v);
        });

        g_->for_each_vertex([&](long src) {
            // Sum incoming contribution from all my neighbors
            double incoming = 0;
            g_->for_each_out_edge_grouped(seq, src, [&](long dst) {
                incoming += contrib_[dst];
            });
            // Update my score, combining old score and new
            double old_score = scores_[src];
            scores_[src] = base_score + damping * incoming;
            // By how much did my score error_ this iteration?
            // Uses our special operator overload
            error_ += fabs(scores_[src] - old_score);
        });
        double err = emu::repl_reduce(error_, std::plus<>());
        printf(" %2d    %lf\n", iter, err);
        if (err < epsilon)
            break;
    }
}

void
pagerank::clear()
{
}

bool
pagerank::check(double damping, double target_error)
{
  double base_score = (1.0 - damping) / g_->num_vertices();
  std::vector<double> incoming_sums(g_->num_vertices(), 0);
  double error = 0;
  g_->for_each_vertex(seq, [&](long u){
    double outgoing_contrib = scores_[u] / g_->out_degree(u);
    g_->for_each_out_edge(seq, u, [&](long v) {
      incoming_sums[v] += outgoing_contrib;
    });
  });

  g_->for_each_vertex(seq, [&](long n){
    error += fabs(base_score + damping * incoming_sums[n] - scores_[n]);
    incoming_sums[n] = 0;
  });

  return error < target_error;
}
