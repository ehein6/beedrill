#include "edge_list.h"
#include <cstring>
#include <getopt.h>

// Logging macro. Flush right away since Emu hardware usually doesn't
#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout);

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
    el->edges = (edge*)malloc(sizeof(edge) * header.num_edges);
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
