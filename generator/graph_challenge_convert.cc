#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <algorithm>
#include <cassert>

#include "pvector.h"

using std::cerr;

void
die()
{
    exit(1);
}

void
print_help_and_quit()
{
    cerr << "Usage: ./graph_challenge_convert <infilename>\n";
    die();
}

template<typename Edge>
class graph_challenge_edge_reader
{
protected:
    pvector<Edge> edges;
    int64_t num_vertices;

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
        oss << " --num_edges " << edges.size();
        oss << " --num_vertices " << num_vertices;
        if (flags.is_undirected) { oss << " --is_undirected"; }
        else                     { oss << " --is_directed"; }
        if (flags.is_sorted)     { oss << " --is_sorted"; }
        if (flags.is_deduped)    { oss << " --is_deduped"; }
        if (flags.is_permuted)   { oss << " --is_permuted"; }
        oss << "\n";
        return oss.str();
    }


    // Fill up the array with edges from the input file
    void read_edges(std::string filename)
    {
        // Open input file
        FILE* fp = fopen(filename.c_str(), "r");
        if (!fp) {
            std::cerr << "Cannot open " << filename << "\n";
            die();
        }

	// read edges, adding to array
	char instr[512];
	while (fgets(instr, 512, fp))
	{
	    int64_t read_src, read_dst;
	    if (sscanf(instr, "%ld %ld", &read_src, &read_dst) == 2) {
              Edge e = { read_src, read_dst };
	      edges.push_back(e);
	      int64_t emax = std::max<int64_t>(read_src, read_dst);
	      num_vertices = std::max<int64_t>(num_vertices, emax);
	    }
        }
        fclose(fp);
	num_vertices++;
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
        pvector<int64_t> mapping(num_vertices);
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

    // Construct
    explicit
    graph_challenge_edge_reader(int64_t n)
        : edges(n)
        , flags{0}
        , num_vertices(0)
    {
    }

    // Make the graph, convert to undirected, and remove duplicates
    void
    generate_and_preprocess(std::string filename)
    {
        read_edges(filename);
        flip_edges();
        sort_edges();
        dedup_edges();
	//	remap_vertex_ids();
	//      shuffle_edges();
    }

    // This one follows the rules of graph500
    void
    generate(std::string filename)
    {
        read_edges(filename);
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
    std::string fileext (".el64");

    struct edge
    {
        int64_t src, dst;
    };
    graph_challenge_edge_reader<edge> pg(0);
    std::cerr << "Generating from file " << argv[1] << "...\n";
    pg.generate_and_preprocess(filename);
    std::cerr << "Writing to file...\n";
    pg.dump(filename + fileext);
    std::cerr << "...Done\n";
}
