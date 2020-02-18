// Initializes the distributed edge list EL from the file

#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <string>
extern "C" {
#include "../mmio.h"
}
struct edge {
    long src;
    long dst;
};

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

int main(int argc, char * argv[])
{

    if (argc != 3) {
        printf("Usage: %s file_in.mtx file_out\n", argv[0]);
        exit(1);
    }

    const char * file_in = argv[1];
    const char * file_out = argv[2];

    if (!has_suffix(file_in, ".mtx")) {
        printf("Expected Matrix Market input file.\n");
        exit(1);
    } else if (has_suffix(file_out, ".mtx")) {
        printf("Output file should not use mtx format.\n");
        exit(1);
    }

    convert_from_mtx_to_binary(file_in, file_out);

    return 0;
}