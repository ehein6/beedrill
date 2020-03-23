#include <cassert>
#include <iterator>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <unordered_map>

#include "pvector.h"

// Swap src and dst so src < dst
template<class Iterator>
void flip_edges(Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    std::transform(begin, end, begin, [&](const Edge & e) -> Edge {
        if (e.src > e.dst) {
            return {e.dst, e.src};
        } else {
            return e;
        }
    });
}

// Sort edges in ascending order
template<class Iterator>
void sort_edges(Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    std::sort(begin, end, [](const Edge& lhs, const Edge& rhs) {
        if (lhs.src == rhs.src) {
            return lhs.dst < rhs.dst;
        }
        return lhs.src < rhs.src;
    });
}


template<class Iterator>
Iterator dedup_edges(Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    pvector<Edge> deduped_edges(std::distance(begin, end));
    // Using std::unique_copy since there is no parallel version of std::unique
    auto deduped_end = std::unique_copy(begin, end, deduped_edges.begin(),
        [](const Edge& a, const Edge& b) {
            return a.src == b.src && a.dst == b.dst;
        }
    );
    // Copy deduplicated edges back into this batch
    std::transform(deduped_edges.begin(), deduped_end, begin,
        [](const Edge& e) { return e; });
    // Return new endpoint
    size_t num_deduped_edges = (deduped_end - deduped_edges.begin());
    return begin + num_deduped_edges;
}

template<class Iterator>
void
remap_vertex_ids(long num_vertices, Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    // Create a list of numbers 1 through N
    pvector<int64_t> mapping(num_vertices);
    std::iota(mapping.begin(), mapping.end(), 0);
    // Shuffle randomly
    std::random_shuffle(mapping.begin(), mapping.end());
    // Use the random array to remap vertex ID's
    std::for_each(begin, end,
        [&](Edge& e) {
            assert(e.src < num_vertices);
            assert(e.dst < num_vertices);
            e.src = mapping[e.src];
            e.dst = mapping[e.dst];
        }
    );
}


template<class Iterator>
long
max_vertex_id(Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    auto max_edge = *std::max_element(begin, end,
        [](const Edge& lhs, const Edge& rhs) {
            return std::max(lhs.src, lhs.dst) < std::max(rhs.src, rhs.dst);
        }
    );
    return std::max(max_edge.src, max_edge.dst);
}

template<class Iterator>
long
compress_vertex_ids(Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    // Build a list of all the vertex ID's
    pvector<long> vertex_ids;
    vertex_ids.resize(std::distance(begin, end) * 2);
    std::for_each(begin, end, [&](const Edge& e) {
        auto i = &e - begin;
        vertex_ids[i*2] = e.src;
        vertex_ids[i*2 + 1] = e.dst;
    });
    // Sort
    std::sort(vertex_ids.begin(), vertex_ids.end());
    // Dedup
    pvector<long> unique_vertex_ids(vertex_ids.size());
    auto unique_end = std::unique_copy(vertex_ids.begin(), vertex_ids.end(),
        unique_vertex_ids.begin());
    long num_unique = unique_end - unique_vertex_ids.begin();
    unique_vertex_ids.resize(num_unique);
    // Do all the ID's 0 through N-1 appear in the list?
    if (unique_vertex_ids.back() == num_unique) {
        // Already compressed, nothing to do
        return num_unique;
    }
    // Create a list of numbers 1 through N
    pvector<int64_t> new_ids(num_unique);
    std::iota(new_ids.begin(), new_ids.end(), 0);
    // Shuffle randomly
    std::random_shuffle(new_ids.begin(), new_ids.end());

    // Create reverse mapping
    // TODO can't insert into map in parallel
    std::unordered_map<long, long> mapping(num_unique);
    for (auto iter = unique_vertex_ids.begin(); iter != unique_vertex_ids.end(); ++iter) {
        // Compute unique position within the list
        long i = iter - unique_vertex_ids.begin();
        long v = *iter;
        // Use to select new vertex ID
        mapping[v] = new_ids[i];
    }

    // Use the random array to remap vertex ID's
    std::for_each(begin, end,
        [&](Edge& e) {
            e.src = mapping[e.src];
            e.dst = mapping[e.dst];
        }
    );
    return num_unique;
}

template<class Iterator>
long
count_unique_vertex_ids(Iterator begin, Iterator end)
{
    using Edge = typename std::iterator_traits<Iterator>::value_type;
    // Build a list of all the vertex ID's
    pvector<long> vertex_ids;
    vertex_ids.resize(std::distance(begin, end) * 2);
    std::for_each(begin, end, [&](const Edge& e) {
        auto i = &e - begin;
        vertex_ids[i*2] = e.src;
        vertex_ids[i*2 + 1] = e.dst;
    });
    // Sort and dedup
    std::sort(vertex_ids.begin(), vertex_ids.end());
    pvector<long> unique_vertex_ids(vertex_ids.size());
    std::unique_copy(vertex_ids.begin(), vertex_ids.end(),
        unique_vertex_ids.begin());
    return unique_vertex_ids.size();
}

struct edge
{
    int64_t src, dst;
};


// Write to file in binary format for PaperWasp/Beedrill
inline void
dump_bin(std::string filename, long num_vertices, edge* edges_begin, edge* edges_end)
{
    long num_edges = std::distance(edges_begin, edges_end);
    // Open output file
    FILE* fp = fopen(filename.c_str(), "wb");
    if (!fp) {
        std::cerr << "Cannot open " << filename << "\n";
        exit(1);
    }
    // Generate header
    std::ostringstream oss;
    oss << " --format el64";
    // unlike args.num_edges, this is the actual number of edges after dups were removed
    oss << " --num_edges " << num_edges;
    oss << " --num_vertices " << num_vertices;
    oss << " --is_undirected";
    oss << " --is_deduped";
    oss << " --is_permuted";
    oss << "\n";
    std::string header = oss.str();
    // Write header
    fwrite(header.c_str(), sizeof(char), header.size(), fp);
    // Write edges
    fwrite(edges_begin, sizeof(edge), num_edges, fp);
    // Clean up
    fclose(fp);
}

// Write to file in Matrix Market format for GraphBLAS
inline void
dump_mm(std::string filename, long num_vertices, edge* edges_begin, edge* edges_end)
{
    // Open file
    std::ofstream f(filename);
    // Write header
    f << "%%MatrixMarket matrix coordinate integer symmetric" << "\n";
    f << "%%GraphBLAS GrB_BOOL\n";
    // Number of rows == number of columns == number of vertices
    f << num_vertices << " ";
    f << num_vertices << " ";
    // Number of non-zeros == number of edges
    f << std::distance(edges_begin, edges_end) << "\n";
    // Write out edges
    for (auto e = edges_begin; e < edges_end; ++e) {
        // Note: The symmetric format expects the lower half of a triangular
        // matrix (src > dst). The resulting graph will be the same
        // since it is undirected.
        // Also convert to 1-based indexing
        long src = std::max(e->src, e->dst) + 1;
        long dst = std::min(e->src, e->dst) + 1;
        f << src << " " << dst << " 1\n";
    }
    // Clean up
    f.close();
}
