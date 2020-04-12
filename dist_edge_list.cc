#include "dist_edge_list.h"
#include "common.h"
#include "edge_list.h"
#include <getopt.h>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <sstream>
#include <emu_cxx_utils/for_each.h>
#include <emu_cxx_utils/fileset.h>

using namespace emu;

void
dist_edge_list::dump() const
{
    for (long i = 0; i < num_edges_; ++i) {
        LOG ("%li -> %li\n", src_[i], dst_[i]);
    }
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
