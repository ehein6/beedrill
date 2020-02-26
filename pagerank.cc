#include "pagerank.h"
#include <cmath>
#include <vector>
#include <emu_cxx_utils/execution_policy.h>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/fill.h>

using namespace emu;
using namespace emu::parallel;

pagerank::pagerank(graph & g)
: g_(&g)
, scores_(g.num_vertices())
, contrib_(g.num_vertices())
, incoming_(g.num_vertices())
, error_(0)
, base_score_(0)
, damping_(0)
, worklist_(g.num_vertices())
{}

// Shallow copy constructor
pagerank::pagerank(const pagerank& other, emu::shallow_copy shallow)
: g_(other.g_)
, scores_(other.scores_, shallow)
, contrib_(other.contrib_, shallow)
, incoming_(other.incoming_, shallow)
, error_(other.error_)
, base_score_(other.base_score_)
, damping_(other.damping_)
, worklist_(other.worklist_, shallow)
{}

// Performs atomic add on a double
// Uses a CAS loop, since we don't have float atomics
static inline void
atomic_add_double(double* lhs, double rhs)
{
    // Get a pointer to the local copy
    double new_value, old_value;
    do {
        // Read the value from memory
        old_value = *lhs;
        // Do the add
        new_value = old_value + rhs;
        // Do again if the old value doesn't match
    } while (old_value != atomic_cas(lhs, old_value, new_value));
}

// Allows us to add to a replicated double with pretty syntax
void
operator+=(emu::repl<double>& lhs, double rhs)
{
    atomic_add_double(&lhs, rhs);
}

class my_reducer
{
private:
    double local_sum_;
    double& global_sum_;
public:
    // Contructor: capture ref to global sum and init local sum to zero
    explicit my_reducer(double & global_sum)
    : local_sum_(0)
    , global_sum_(global_sum)
    {}

    // Copy constructor: propagate ref to global sum but init local sum to zero
    my_reducer(const my_reducer& other)
    : local_sum_(0)
    , global_sum_(other.global_sum_)
    {}

    // Operator: add to local sum
    my_reducer&
    operator+=(double rhs)
    {
        local_sum_ += rhs;
        return *this;
    }

    // Destructor: add to global sum
    ~my_reducer()
    {
        atomic_add_double(&global_sum_, local_sum_);
    }
};

int
pagerank::run (int max_iters, double damping, double epsilon)
{
    // Initialize scores for all vertices
    double init_score = 1.0 / g_->num_vertices();
    fill(scores_.begin(), scores_.end(), init_score);
    // Init replicated constants
    base_score_ = (1.0 - damping) / g_->num_vertices();
    damping_ = damping;
    int iter;
    for (iter = 0; iter < max_iters; ++iter) {
        error_ = 0;
        worklist_.clear_all();
        g_->for_each_vertex(fixed, [this](long v) {
            // Initialize incoming contribution to zero
            incoming_[v] = 0;
            // Look up the edge list for this vertex
            auto begin = g_->out_edges_begin(v);
            auto end = g_->out_edges_end(v);
            auto degree = end - begin;
            if (degree == 0) {
                contrib_[v] = 0;
            } else {
                // Compute outgoing contribution and add edges to work list
                contrib_[v] = scores_[v] / degree;
                worklist_.append(v, begin, end);
            }
        });

        worklist_.process_all_ranges(dynamic_policy<256>(),
            [contrib=contrib_.data(),incoming=incoming_.data()]
            (long src, graph::edge_iterator e1, graph::edge_iterator e2) {
                // Sum incoming contribution from all my neighbors
                my_reducer accum(incoming[src]);
                for_each(unroll, e1, e2,
                    // Note: capture accum by value, use mutable lambda
                    [accum, contrib] (long dst) mutable {
                        accum += contrib[dst];
                    }
                );
            }
        );

        g_->for_each_vertex(fixed, [this](long src) {
            // Update my score, combining old score and new
            double old_score = scores_[src];
            scores_[src] = base_score_ + damping_ * incoming_[src];
            // By how much did my score change this iteration?
            // Uses our special operator overload
            error_ += fabs(scores_[src] - old_score);
        });
        double err = repl_reduce(error_, std::plus<>());
        if (err < epsilon)
            break;
    }
    return iter;
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
      long degree = g_->out_degree(u);
      double outgoing_contrib = degree == 0 ? 0 :
          scores_[u] / degree;
      g_->for_each_out_edge(seq, u, [&](long v) {
          incoming_sums[v] += outgoing_contrib;
      });
  });

  g_->for_each_vertex(seq, [&](long n){
    error += fabs(base_score + damping * incoming_sums[n] - scores_[n]);
    incoming_sums[n] = 0;
  });

  bool success = error < target_error;
  if (!success) {
      LOG("Error (%3.2e) is greater than epsilon (%3.2e)\n",
          error, target_error);
  }
  return success;
}
