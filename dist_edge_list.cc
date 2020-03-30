#include "dist_edge_list.h"
#include "common.h"
#include <getopt.h>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <emu_cxx_utils/for_each.h>
#include <sstream>

using namespace emu;

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
};

static const struct option header_options[] = {
    {"num_vertices"     , required_argument},
    {"num_edges"        , required_argument},
    {"is_sorted"        , no_argument},
    {"is_deduped"       , no_argument},
    {"is_permuted"      , no_argument},
    {"is_directed"      , no_argument},
    {"is_undirected"    , no_argument},
    {"format"           , required_argument},
    {NULL}
};

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

void
dist_edge_list::dump() const
{
    for (long i = 0; i < num_edges_; ++i) {
        LOG ("%li -> %li\n", src_[i], dst_[i]);
    }
}

void
parse_edge_list_file_header(FILE* fp, edge_list_file_header *header)
{
    // Get the first line of text from the file
    char line[256];
    if (!fgets(line, 256, fp)) {
        LOG("Failed to read edge list header\n");
        exit(1);
    }
    size_t line_len = strlen(line);
    header->header_length = line_len;
    // Strip endline character
    if (line[line_len-1] != '\n') {
        LOG("Invalid edge list file header\n");
        exit(1);
    }
    line[--line_len] = '\0';

    // Split on spaces
    //    Assumption: never more than 32 arguments in header
    const int max_argc = 32;
    char * argv[max_argc];
    // Leave empty slot for expected program name
    argv[0] = strdup("edge list header");
    int argc = 1;
    char * token = strtok(line, " ");
    while (token && argc < max_argc) {
        argv[argc++] = token;
        token = strtok(NULL, " ");
    }
    // Null terminator for argv
    argv[argc] = NULL;

    // Assume default values
    header->num_vertices = -1;
    header->num_edges = -1;
    header->is_sorted = false;
    header->is_deduped = false;
    header->format = NULL;

    // Reset getopt
    optind = 1;
    // Parse header like an option string
    int option_index;
    while (true)
    {
        int c = getopt_long(argc, argv, "", header_options, &option_index);
        // Done parsing
        if (c == -1) { break; }
        // Invalid arg
        if (c == '?') {
            LOG( "Invalid field edge list header\n");
            exit(1);
        }
        const char* option_name = header_options[option_index].name;

        if (!strcmp(option_name, "num_vertices")) {
            header->num_vertices = atol(optarg);
        } else if (!strcmp(option_name, "num_edges")) {
            header->num_edges = atol(optarg);
        } else if (!strcmp(option_name, "is_sorted")) {
            header->is_sorted = true;
        } else if (!strcmp(option_name, "is_deduped")) {
            header->is_deduped = true;
        } else if (!strcmp(option_name, "format")) {
            header->format = strdup(optarg);
        }
    }
    // It's up to the caller to validate and interpret the arguments
}

void
load_edge_list_local(const char* path, edge_list * el)
{
    LOG("Opening %s...\n", path);
    FILE* fp = fopen(path, "rb");
    if (fp == NULL) {
        LOG("Unable to open %s\n", path);
        exit(1);
    }

    edge_list_file_header header;
    parse_edge_list_file_header(fp, &header);

    if (header.num_vertices <= 0 || header.num_edges <= 0) {
        LOG("Invalid graph size in header\n");
        exit(1);
    }
    // TODO add support for other formats
    if (!header.format || !!strcmp(header.format, "el64")) {
        LOG("Unsuppported edge list format %s\n", header.format);
        exit(1);
    }
    // Future implementations may be able to handle duplicates
    if (!header.is_deduped) {
        LOG("Edge list must be sorted and deduped.");
        exit(1);
    }

    el->num_edges = header.num_edges;
    el->num_vertices = header.num_vertices;
    el->edges = (edge*)mw_localmalloc(sizeof(edge) * header.num_edges, el);
    if (el->edges == NULL) {
        LOG("Failed to allocate memory for %ld edges\n", header.num_edges);
        exit(1);
    }

    LOG("Loading %li edges from %s...\n", header.num_edges, path);
    size_t rc = fread(&el->edges[0], sizeof(edge), header.num_edges, fp);
    if (rc != (size_t)header.num_edges) {
        LOG("Failed to load edge list from %s ", path);
        if (feof(fp)) {
            LOG("unexpected EOF\n");
        } else if (ferror(fp)) {
            perror("fread returned error\n");
        }
        exit(1);
    }
    fclose(fp);
}

// Check if string has a given suffix
// Adapted from https://stackoverflow.com/a/874160/2570605
bool
has_suffix (std::string const &str, std::string const &suffix)
{
    if (str.length() >= suffix.length()) {
        return (0 == str.compare (
            str.length() - suffix.length(), suffix.length(), suffix));
    } else {
        return false;
    }
}

dist_edge_list::handle
dist_edge_list::load(const char* filename)
{
    return load_binary(filename);
}

// Initializes the distributed edge list EL from the file
dist_edge_list::handle
dist_edge_list::load_binary(const char* filename)
{
    LOG("Opening %s...\n", filename);
    FILE* fp = fopen(filename, "rb");
    if (fp == nullptr) {
        LOG("Unable to open %s\n", filename);
        exit(1);
    }

    edge_list_file_header header;
    parse_edge_list_file_header(fp, &header);

    if (header.num_vertices <= 0 || header.num_edges <= 0) {
        LOG("Invalid graph size in header\n");
        exit(1);
    }
    // TODO add support for other formats
    if (!header.format || !!strcmp(header.format, "el64")) {
        LOG("Unsuppported edge list format %s\n", header.format);
        exit(1);
    }
    // Future implementations may be able to handle duplicates
    if (!header.is_deduped) {
        LOG("Edge list must be sorted and deduped.");
        exit(1);
    }

    // Allocate the distributed edge list
    auto dist_el = emu::make_repl_shallow<dist_edge_list>(
        header.num_vertices, header.num_edges
    );

    LOG("Loading %li edges from %s\n", header.num_edges, filename);

    // Double-buffering: read edges into one buffer while we scatter the other
    size_t buffer_len = 65536;
    std::vector<edge> buffer_A(buffer_len);
    std::vector<edge> buffer_B(buffer_len);
    auto* file_buffer = &buffer_A;
    auto* scatter_buffer = &buffer_B;

    // Skip scatter on first iteration
    scatter_buffer->resize(0);
    // Next index to target in the dist_edge_list
    size_t scatter_pos = 0;

    hooks_region_begin("load_edge_list_buffered");
    for (size_t edges_remaining = header.num_edges;
        edges_remaining || !scatter_buffer->empty();
        edges_remaining -= buffer_len)
    {
        // Shrink buffer if there are few edges remaining
        buffer_len = std::min(edges_remaining, buffer_len);
        file_buffer->resize(buffer_len);

        // Print progress meter
        // Uses carriage return to update the same line over and over
        LOG("\rLoaded %3.0f%%...",
            100.0 * ((double)header.num_edges - edges_remaining)
            / header.num_edges);

        // Spawn local threads to scatter edges across the system from scatter buffer
        cilk_spawn parallel::for_each(fixed, scatter_buffer->begin(), scatter_buffer->end(),
            [&](edge &e) {
                long i = &e - &*scatter_buffer->begin();
                dist_el->src_[i + scatter_pos] = e.src;
                dist_el->dst_[i + scatter_pos] = e.dst;
            }
        );

        // Read a chunk of edges from the file into file buffer
        size_t rc = fread(file_buffer->data(), sizeof(edge), file_buffer->size(), fp);
        // Check return code from fread
        if (rc != file_buffer->size()) {
            LOG("Failed to load edge list from %s ", filename);
            if (feof(fp)) { LOG("unexpected EOF\n"); }
            else if (ferror(fp)) { perror("fread returned error\n"); }
            exit(1);
        }
        // Wait for scatter to complete
        cilk_sync;
        // Advance position in buffer
        scatter_pos += scatter_buffer->size();
        // Swap buffers
        std::swap(file_buffer, scatter_buffer);
    }
    LOG("\n");
    hooks_region_end();

    // Close file handle
    fclose(fp);

    return dist_el;
}

void
serialize(emu::fileset &f, dist_edge_list& self)
{
    serialize(f, self.num_vertices_);
    serialize(f, self.num_edges_);
    serialize(f, self.src_);
    serialize(f, self.dst_);
}

void
deserialize(fileset& f, dist_edge_list& self)
{
    deserialize(f, self.num_vertices_);
    deserialize(f, self.num_edges_);
    deserialize(f, self.src_);
    deserialize(f, self.dst_);
}
