#include "dist_edge_list.h"
#include "common.h"
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <emu_cxx_utils/for_each.h>

using namespace emu;
using namespace emu::execution;

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
    fgets(line, 256, fp);
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

void
scatter_edges(edge_list& el, dist_edge_list& dist_el)
{
    // Scatter from local to distributed edge list
    parallel::for_each(fixed, el.edges, el.edges + el.num_edges, [&] (edge &e) {
        long i = &e - el.edges;
        dist_el.src_[i] = e.src;
        dist_el.dst_[i] = e.dst;
    });
}

// Initializes the distributed edge list EL from the file
std::unique_ptr<emu::repl_copy<dist_edge_list>>
dist_edge_list::load(const char* filename)
{
    edge_list el;

    hooks_region_begin("load_edge_list");
    load_edge_list_local(filename, &el);
    hooks_region_end();

    auto dist_el = emu::make_repl_copy<dist_edge_list>(
        el.num_vertices, el.num_edges
    );

    LOG("Scattering edge list from nodelet 0...\n");
    hooks_region_begin("scatter_edge_list");
    scatter_edges(el, *dist_el);
    hooks_region_end();

    return dist_el;
}


// FIXME
//size_t
//list_offset_to_file_offset(long pos, long num_edges)
//{
//    // Divide and round up
//    long edges_per_nodelet = (num_edges)/ NODELETS();
//    long edge_offset = edges_per_nodelet * (pos % NODELETS()) + (pos / NODELETS());
//    long file_offset = sizeof(edge) * edge_offset;
//    return file_offset;
//}
//
//#define EDGE_BUFFER_SIZE ((16 * 1024 * 1024) / sizeof(edge))
//
//void
//buffered_edge_list_reader(long * array, long begin, long end, va_list args)
//{
//    /*
//     * We can't read directly into the distributed edge list.
//     * Need to convert to in-memory struct-of-arrays from the file's array-of-structs.
//     * So we read a large chunk at a time (minimize # of calls to fread).
//     * And then unpack it into the local portion of the distributed EL
//     */
//    // Open the file
//    char * filename = va_arg(args, char*);
//    size_t file_header_len = va_arg(args, size_t);
//    FILE * fp = mw_fopen(filename, "rb", &EL.src[begin]);
//    if (fp == NULL) {
//        MIGRATE(&EL.src[begin]);
//        long nlet = NODE_ID();
//        LOG("Error opening %s on nodelet %li\n", filename, nlet);
//        exit(1);
//    }
//    // Skip past the header and jump to this threads portion of the edge list
//    size_t offset = file_header_len + list_offset_to_file_offset(begin, EL.num_edges);
//    int rc = fseek(fp, offset, SEEK_SET);
//    if (rc) {
//        MIGRATE(&EL.src[begin]);
//        long nlet = NODE_ID();
//        LOG("Error loading %li-th edge from nodelet %li\n", begin, nlet);
//        exit(1);
//    }
//
//    // The range (begin, end] is striped, with a stride of NODELETS
//    // Every time we read an edge, we'll decrement this to keep track
//    size_t num_to_read = (end - begin + NODELETS() - 1) / NODELETS();
//    size_t buffer_size = EDGE_BUFFER_SIZE;
//    edge buffer[buffer_size];
//
//    for (size_t pos = begin; num_to_read > 0;) {
//
//        // Fill the buffer with edges from the file
//        size_t n = buffer_size < num_to_read ? buffer_size : num_to_read;
//        size_t rc = mw_fread(buffer, 1, sizeof(edge) * n, fp);
//        if (rc != n * sizeof(edge)) {
//            LOG("Error during graph loading, expected %li but only read %li\n",
//                n * sizeof(edge), rc);
//            exit(1);
//        }
//
//        // Copy into the edge list
//        for (size_t i = 0; i < n; ++i) {
//            EL.src[pos] = buffer[i].src;
//            EL.dst[pos] = buffer[i].dst;
//            // Next edge on this nodelet is at NODELETS stride away
//            pos += NODELETS();
//            num_to_read -= 1;
//        }
//    }
//    // Clean up
//    mw_fclose(fp);
//}
//
//void
//load_edge_list_distributed(const char* filename)
//{
//    // Open the file just to check the header
//    LOG("Opening %s...\n", filename);
//    FILE* fp = fopen(filename, "rb");
//    if (fp == NULL) {
//        LOG("Unable to open %s\n", filename);
//        exit(1);
//    }
//    edge_list_file_header header;
//    parse_edge_list_file_header(fp, &header);
//    fclose(fp);
//
//    if (header.num_vertices <= 0 || header.num_edges <= 0) {
//        LOG("Invalid graph size in header\n");
//        exit(1);
//    }
//    // TODO add support for other formats
//    if (!header.format || !!strcmp(header.format, "el64")) {
//        LOG("Unsuppported edge list format %s\n", header.format);
//        exit(1);
//    }
//    // Future implementations may be able to handle duplicates
//    if (!header.is_deduped) {
//        LOG("Edge list must be deduped.");
//        exit(1);
//    }
//
//
//    init_dist_edge_list(header.num_vertices, header.num_edges);
//    LOG("Loading %li edges into distributed edge list from all nodes...\n", EL.num_edges);
//    hooks_region_begin("load_edge_list");
//    emu_1d_array_apply(EL.src, EL.num_edges,
//        // Force one thread per nodelet
//        EL.num_edges / NODELETS(),
//        buffered_edge_list_reader, filename, header.header_length
//    );
//    hooks_region_end();
//}




