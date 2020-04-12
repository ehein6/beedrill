#pragma once
#include <cstddef>
#include <cstdio>
#include <cstdlib>

struct edge_list_file_header {
    // Number of vertices in the file
    //  There can be fewer actual unique vertex ID's, but
    //  every vertex ID must be < num_vertices
    long num_vertices;
    // Number of edges in the file, including duplicates
    long num_edges;
    // Is the edge list sorted?
    bool is_sorted;
    // Have duplicate edges been removed?
    bool is_deduped;
    // Format of the edge list: (for example, el64)
    //   el   : src, dst
    //   wel  : src, dst, weight
    //   welt : src, dst, weight, timestamp
    // Suffixes:
    //       : text, delimited by spaces and newlines
    //   32  : binary, 32 bits per field
    //   64  : binary, 64 bits per field
    char* format;
    // Number of bytes in the file header.
    // Includes the newline character
    // There is no null terminator
    size_t header_length;
};

struct edge {
    long src;
    long dst;
};

// Local edge list
struct edge_list
{
    // Number of edges in the array
    long num_edges;
    // Number of vertices in the edge list;
    // All vertex ID's are guaranteed to be < num_vertices
    long num_vertices;
    // Pointer to local array of edges
    edge * edges;

    edge * begin() { return edges; }
    edge * end() { return edges + num_edges; }
};

void parse_edge_list_file_header(FILE* fp, edge_list_file_header *header);
void load_edge_list_local(const char* path, edge_list * el);

