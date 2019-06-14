#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <algorithm>
#include <cassert>

#include "pvector.h"
#include "rmat_args.h"
#include "rmat_generator.h"

using std::cerr;

void
die()
{
    exit(1);
}

void
print_help_and_quit()
{
    cerr << "Usage: ./rmat_dataset_dump <rmat_args>\n";
    die();
}

template<typename Edge>
class parallel_rmat_edge_generator
{
protected:
    rmat_args args;
    rmat_edge_generator generator;
    pvector<Edge> edges;

    struct {
        // Does every edge only appear in one direction?
        bool is_undirected;
        // Are edges in sorted order?
        bool is_sorted;
        // False if there are any duplicate edges
        bool is_deduped;
        // False if we are still using the generated vertex ID's
        bool is_permuted;
    } flags;

    std::string
    get_header(const std::string& format)
    {
        std::ostringstream oss;
        oss << " --format " << format;
        // unlike args.num_edges, this is the actual number of edges after dups were removed
        oss << " --num_edges " << edges.size();
        oss << " --num_vertices " << args.num_vertices;
        if (flags.is_undirected) { oss << " --is_undirected"; }
        else                     { oss << " --is_directed"; }
        if (flags.is_sorted)     { oss << " --is_sorted"; }
        if (flags.is_deduped)    { oss << " --is_deduped"; }
        if (flags.is_permuted)   { oss << " --is_permuted"; }
        oss << "\n";
        return oss.str();
    }


    // Fill up the array with randomly generated edges
    void fill_edges()
    {
        // Make a local copy of the edge generator
        rmat_edge_generator local_rng = generator;

        // Keeps track of this thread's position in the random number stream relative to the loop index
        int64_t pos = 0;
        const int64_t num_edges = static_cast<int64_t>(edges.size());

        // Generate edges in parallel, while maintaining RNG state as if we did it serially
        // Mark the RNG with firstprivate so each thread gets a copy of the inital state
        // Also mark the RNG with lastprivate so the master thread gets the state after the last iteration
        #pragma omp parallel for \
        firstprivate(local_rng) lastprivate(local_rng) \
        firstprivate(pos) \
        schedule(static)
        for (int64_t i = 0; i < num_edges; ++i)
        {
            // Assuming we will always execute loop iterations in order (we can't jump backwards)
            assert(pos <= i);
            // Advance RNG whenever we skip ahead in the iteration space
            if (pos < i) {
                uint64_t skip_distance = static_cast<uint64_t>(i - pos);
                local_rng.discard(skip_distance);
            }
            // Generate the next random edge
            Edge& e = edges[i];
            local_rng.next_edge(&e.src, &e.dst);

            // Remember position, in case OpenMP jumps through the iteration space
            pos = i+1;
        }

        // Go back through the list and regenerate self-edges
        // This step is serial, since we don't know how many times each edge has to be
        // re-rolled before we get a non-self-edge
        for (int64_t i = 0; i < num_edges; ++i)
        {
            Edge& e = edges[i];
            while (e.src == e.dst) {
                local_rng.next_edge(&e.src, &e.dst);
            }
        }

        // Copy final RNG state back to caller
        generator = local_rng;
    }


    // Swap src and dst so src < dst
    void
    flip_edges()
    {
        std::transform(edges.begin(), edges.end(), edges.begin(), [&](const Edge & e) -> Edge {
            if (e.src > e.dst) {
                return {e.dst, e.src};
            } else {
                return e;
            }
        });
        flags.is_undirected = true;
        flags.is_sorted = false;
        flags.is_deduped = false;
    }

    void
    sort_edges()
    {
        std::sort(edges.begin(), edges.end(), [](const Edge& lhs, const Edge& rhs) {
            if (lhs.src == rhs.src) {
                return lhs.dst < rhs.dst;
            }
            return lhs.src < rhs.src;
        });
        flags.is_sorted = true;
    }

    void
    dedup_edges()
    {
        assert(flags.is_sorted);
        pvector<Edge> deduped_edges(edges.size());
        // Using std::unique_copy since there is no parallel version of std::unique
        auto end = std::unique_copy(edges.begin(), edges.end(), deduped_edges.begin(),
            [](const Edge& a, const Edge& b) {
                return a.src == b.src && a.dst == b.dst;
            }
        );
        // Copy deduplicated edges back into this batch
        std::transform(deduped_edges.begin(), end, edges.begin(),
            [](const Edge& e) { return e; });
        // Adjust size
        size_t num_deduped_edges = (end - deduped_edges.begin());
        edges.resize(num_deduped_edges);
        flags.is_deduped = true;
    }

    void
    remap_vertex_ids()
    {
        pvector<int64_t> mapping(args.num_vertices);
        std::iota(mapping.begin(), mapping.end(), 0);
        std::random_shuffle(mapping.begin(), mapping.end());
        std::for_each(edges.begin(), edges.end(),
            [&](Edge& e) {
                e.src = mapping[e.src];
                e.dst = mapping[e.dst];
            }
        );
        flags.is_sorted = false;
    }

    void
    shuffle_edges()
    {
        std::random_shuffle(edges.begin(), edges.end());
        flags.is_sorted = false;
    }

public:

    // Construct from rmat_args
    explicit
    parallel_rmat_edge_generator(rmat_args args_in)
        : args(args_in)
        , generator(args.num_vertices, args.a, args.b, args.c, args.d)
        , edges(static_cast<size_t>(args.num_edges))
        , flags{0}
    {
    }

    // Make the graph, convert to undirected, and remove duplicates
    void
    generate_and_preprocess()
    {
        fill_edges();
        flip_edges();
        sort_edges();
        dedup_edges();
        remap_vertex_ids();
        shuffle_edges();
    }

    // This one follows the rules of graph500
    void
    generate()
    {
        fill_edges();
        remap_vertex_ids();
        shuffle_edges();
    }

    void
    dump(std::string filename)
    {
        // Open output file
        FILE* fp = fopen(filename.c_str(), "wb");
        if (!fp) {
            std::cerr << "Cannot open " << filename << "\n";
            die();
        }
        // Generate header
        std::string header = get_header("el64");
        // Write header
        fwrite(header.c_str(), sizeof(char), header.size(), fp);
        // Write edges
        fwrite(edges.begin(), sizeof(Edge), edges.size(), fp);
        // Clean up
        fclose(fp);
    }
};


int
main(int argc, const char* argv[])
{
    if (argc != 2) { print_help_and_quit(); }

    std::string filename = argv[1];

    // Parse rmat arguments
    rmat_args args = rmat_args::from_string(argv[1]);
    std::string error = args.validate();
    if (!error.empty())
    {
        std::cerr << error << "\n";
        print_help_and_quit();
    }

    struct edge
    {
        int64_t src, dst;
    };
    parallel_rmat_edge_generator<edge> pg(args);
    std::cerr << "Generating list of " << args.num_edges << " edges...\n";
    pg.generate_and_preprocess();
    std::cerr << "Writing to file...\n";
    pg.dump(filename);
    std::cerr << "...Done\n";
}