// Initializes the distributed edge list EL from the file

#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <sstream>
#include <cstring>

#include "../edge_list.h"

void 
convert_to_fileset(const char* file_in, long num_nlets)
{
    // Open output files
    long nlet;
    std::vector<FILE*> files(num_nlets);
    for (nlet = 0; nlet < num_nlets; ++nlet) {
        // Append suffix to each file: <nlet>of<nlets>
        std::ostringstream oss;
        oss << file_in << "." << nlet << "of" << num_nlets;
        std::string slice_filename = oss.str();
        // Open the file
        FILE* fp = fopen(slice_filename.c_str(), "wb");
        if (fp == nullptr) {
            printf("Failed to open %s\n", slice_filename.c_str());
            exit(1);
        }
        // Store the handle
        files[nlet] = fp;
    }

    // Load edge list
    edge_list el;
    load_edge_list_local(file_in, &el);

    // Define helper function to write an integer to the file
    auto write64 = [](FILE* fp, long& value) {
        static_assert(sizeof(long) == 8, "Need 8-byte long type");
        if (1 != fwrite(&value, sizeof(long), 1, fp)) {
            printf("Error writing to file, quitting");
            exit(1);
        }
    };

    // Compare with dist_edge_list::deserialize()
    // Changes to that function should be mirrored here

    // Write number of vertices to each file
    for (nlet = 0; nlet < num_nlets; ++nlet) {
        write64(files[nlet], el.num_vertices);
    }
    // Write number of edges to each file
    for (nlet = 0; nlet < num_nlets; ++nlet) {
        write64(files[nlet], el.num_edges);
    }
    // Write length of src array to each file
    for (nlet = 0; nlet < num_nlets; ++nlet) {
        write64(files[nlet], el.num_edges);
    }
    // Write source vertex ID's in a round-robin fashion
    nlet = 0;
    for (auto edge : el) {
        write64(files[nlet], edge.src);
        nlet = (nlet + 1) % num_nlets;
    }
    // Write length of array to each file
    for (nlet = 0; nlet < num_nlets; ++nlet) {
        write64(files[nlet], el.num_edges);
    }
    // Write destination vertex ID's in a round-robin fashion
    nlet = 0;
    for (auto edge : el) {
        write64(files[nlet], edge.dst);
        nlet = (nlet + 1) % num_nlets;
    }
}

int main(int argc, char * argv[])
{
    if (argc != 3) {
        printf("Usage: %s graph num_nodelets\n", argv[0]);
        exit(1);
    }

    const char * file_in = argv[1];
    long nlets = atol(argv[2]);
    if (nlets <= 0) {
        printf("Number of nodelets must be positive");
        exit(1);
    }

    printf("Creating fileset from %s for %li nodelets\n", file_in, nlets);
    convert_to_fileset(file_in, nlets);
    printf("Done\n");

    return 0;
}
