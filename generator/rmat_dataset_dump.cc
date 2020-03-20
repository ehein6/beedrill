#include <iostream>
#include <algorithm>

#include "pvector.h"
#include "rmat_args.h"
#include "rmat_generator.h"
#include "edge_list_utils.h"

using std::cerr;

void
print_help_and_quit()
{
    cerr << "Usage: ./rmat_dataset_dump <rmat_args>\n";
    cerr << "    Format 1: A-B-C-D-edges-vertices.rmat\n";
    cerr << "    Format 2: graph500-scaleN\n";
    cerr << "    Format 3: graph500-scaleN.mtx\n";
    exit(1);
}

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
        cerr << error << "\n";
        print_help_and_quit();
    }

    // Init RMAT edge generator
    rmat_edge_generator
    generator(args.num_vertices, args.a, args.b, args.c, args.d);

    cerr << "Generating list of " << args.num_edges << " edges...\n";
    // Allocate edge list
    pvector<edge> edges(args.num_edges);

    // Fill with randomly generated edges
    rmat_fill(generator, edges.begin(), edges.end());
    // Make all edges point from lower to higher vertex ID
    flip_edges(edges.begin(), edges.end());
    // Sort edges in ascending order
    sort_edges(edges.begin(), edges.end());
    // Deduplicate edges
    auto new_end = dedup_edges(edges.begin(), edges.end());
    edges.resize(new_end - edges.begin());
    // Randomly remap vertex ID's
    remap_vertex_ids(args.num_vertices, edges.begin(), edges.end());
    // Shuffle edges randomly
    std::random_shuffle(edges.begin(), edges.end());

    cerr << "Writing to file...\n";
    if (!strcmp(".mtx", &*filename.end() - 4)) {
        dump_mm(filename, args.num_vertices, edges.begin(), edges.end());
    } else {
        dump_bin(filename, args.num_vertices, edges.begin(), edges.end());
    }
    cerr << "...Done\n";
}