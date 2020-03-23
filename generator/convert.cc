// Initializes the distributed edge list EL from the file

#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <string>

#include "pvector.h"
#include "edge_list_utils.h"

extern "C" {
#include "mmio.h"
}

// Check if string has a given suffix
// Adapted from https://stackoverflow.com/a/874160/2570605
static bool
has_suffix (std::string const &str, std::string const &suffix)
{
    if (str.length() >= suffix.length()) {
        return (0 == str.compare (
            str.length() - suffix.length(), suffix.length(), suffix));
    } else {
        return false;
    }
}

void 
convert_from_mtx_to_binary(const char* file_in, const char* file_out)
{
    // Open input file
    printf("Opening %s...\n", file_in);
    FILE* fp_in = fopen(file_in, "r");
    if (fp_in == nullptr) {
        printf("Unable to open %s\n", file_in);
        exit(1);
    }

    // Open output file
    printf("Opening %s...\n", file_out);
    FILE* fp_out = fopen(file_out, "wb");
    if (fp_out == nullptr) {
        printf("Unable to open %s\n", file_out);
        exit(1);
    }

    // Read type of matrix
    MM_typecode matcode;
    if (mm_read_banner(fp_in, &matcode)!= 0) {
        printf("Could not process Matrix Market banner.\n");
        exit(1);
    }

    // Check matrix format
    if (!(mm_is_symmetric(matcode)
          && mm_is_sparse(matcode)
          && mm_is_coordinate(matcode))) {
        printf("Need symmetric sparse matrix in coordinate format.\n");
        exit(1);
    }

    // Read size of matrix
    int num_rows, num_cols, num_entries;
    if (mm_read_mtx_crd_size(fp_in, &num_rows, &num_cols, &num_entries) !=0)
        exit(1);
    long num_vertices = std::max(num_rows, num_cols);
    long num_edges = num_entries;

    printf("Converting %li edges from %s into %s\n", num_edges, file_in, file_out);
    // Write header to output file
    fprintf(fp_out, "--num_vertices %li ", num_vertices);
    fprintf(fp_out, "--num_edges %li ", num_edges);
    fprintf(fp_out, "--is_deduped --is_undirected --format el64\n");

    // Read the edges, discarding the data
    int src, dst;
    int integer;
    double real, imag;
    for (long i = 0; i < num_edges; ++i) {
        switch (matcode[2]) {
            // Pattern: src dst
            case 'P': fscanf(fp_in, "%d %d\n", &src, &dst); break;
                // Integer: src dst integer
            case 'I': fscanf(fp_in, "%d %d %d\n", &src, &dst, &integer); break;
                // Real: src dst real
            case 'R': fscanf(fp_in, "%d %d %lg\n", &src, &dst, &real); break;
                // Complex: src dst real imag
            case 'C': fscanf(fp_in, "%d %d %lg %lg\n", &src, &dst, &real, &imag); break;
            default: exit(1);
        }

        // Write edge to file in binary format, converting from 1-based indexing
        edge e {src - 1, dst - 1};
        fwrite(&e, sizeof(edge), 1, fp_out);

        // Print progress meter
        // Uses carriage return to update the same line over and over
        if (i % 10000 == 0) {
            printf("\r%3.0f%%...", 100.0 * i / num_edges);
        }
    }
    printf("\r100%%   \n");

    fclose(fp_in);
    fclose(fp_out);
}

long read_long(char*& pos)
{
    char* new_pos;
    long value = strtol(pos, &new_pos, 10);
    if (pos == new_pos) {
        printf("Couldn't parse vertex ID from: %s\n", pos);
        exit(1);
    }
    pos = new_pos;
    return value;
}

void
convert_from_txt_to_binary(const char* file_in, const char* file_out)
{
    // Open input file
    printf("Opening %s...\n", file_in);
    FILE* fp_in = fopen(file_in, "r");
    if (fp_in == nullptr) {
        printf("Unable to open %s\n", file_in);
        exit(1);
    }

    pvector<edge> edges;
    long num_vertices = -1;
    long num_edges = -1;
    // Read one line at a time from the file, counting edges as we go
    char buffer[255];
    while (fgets(buffer, sizeof buffer, fp_in)) {
        // Ignore comments
        if (buffer[0] == '#') {
            // Try to read number of edges/vertices
            // If this fails, nothing bad happens
            sscanf(buffer, "# Nodes: %li Edges: %li",
                &num_vertices, &num_edges);
            if (num_edges > 0) {
                edges.reserve(num_edges);
            }
            continue;
        } else if (buffer[0] == '\n') {
            // If the file has windows line endings, fgets will handle the \r
            // and leave the \n in the buffer.
            continue;
        }
        // Read two edges from the line. Ignore other data.
        char *pos = buffer;
        long src = read_long(pos);
        long dst = read_long(pos);
        // Copy edge into buffer
        edges.push_back({src, dst});
    }
    fclose(fp_in);

    // Make sure the header matches the number of edges read
    if (num_edges != static_cast<long>(edges.size())) {
        printf("Error: we read %li edges from file, expected %li\n",
            edges.size(), num_edges);
        exit(1);
    }

    // Make sure num_vertices is correct
    auto num_ids = compress_vertex_ids(edges.begin(), edges.end());
    if (num_vertices != num_ids) {
        printf("Error: Found %li unique vertex ID's, expected %li\n",
            num_ids, num_vertices);
        exit(1);
    }

    printf("Dumping %li edges to %s...\n", edges.size(), file_out);
    // Dump edge list to file
    dump_bin(file_out, num_vertices, edges.begin(), edges.end());
    printf("Done\n");
}

int main(int argc, char * argv[])
{
    if (argc != 3) {
        printf("Usage: %s file_in file_out\n", argv[0]);
        exit(1);
    }

    const char * file_in = argv[1];
    const char * file_out = argv[2];

    if (has_suffix(file_in, ".mtx")) {
        convert_from_mtx_to_binary(file_in, file_out);
    } else if (has_suffix(file_in, ".txt")) {
        convert_from_txt_to_binary(file_in, file_out);
    } else {
        printf("Unrecognized file extension\n");
        exit(1);
    }

    return 0;
}
