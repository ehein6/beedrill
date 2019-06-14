#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "dist_edge_list.h"
#include "graph_from_edge_list.h"

const struct option long_options[] = {
  {"graph_filename"   , required_argument},
  {"heavy_threshold"  , required_argument},
  {"help"             , no_argument},
  {NULL}
};

void print_help(const char* argv0)
{
  LOG( "Usage: %s [OPTIONS]\n", argv0);
  LOG("\t--graph_filename  Path to graph file to load\n");
  LOG("\t--heavy_threshold Neighbor limit for spreading across nodelets\n");
  LOG("\t--help            Print command line help\n");
}

typedef struct graph_args {
  const char* graph_filename;
  long heavy_threshold;
} graph_args;

struct graph_args parse_args(int argc, char *argv[])
{
  graph_args args;
  args.graph_filename = NULL;
  args.heavy_threshold = LONG_MAX;

  int option_index;
  while (true) {
    int c = getopt_long(argc, argv, "", long_options, &option_index);
    if (c == -1) { break; } // Done parsing
    if (c == '?') { // Parse error
      LOG( "Invalid arguments\n");
      print_help(argv[0]);
      exit(1);
    }
    const char* option_name = long_options[option_index].name;

    if (!strcmp(option_name, "graph_filename")) {
      args.graph_filename = optarg;
    } else if (!strcmp(option_name, "heavy_threshold")) {
      args.heavy_threshold = atol(optarg);
    } else if (!strcmp(option_name, "help")) {
      print_help(argv[0]);
      exit(1);
    }
  }
  if (args.graph_filename == NULL)
    { LOG( "Missing graph filename\n"); exit(1); }
  if (args.heavy_threshold <= 0)
    { LOG( "heavy_threshold must be > 0\n"); exit(1); }
  return args;
}

int main(int argc, char ** argv)
{
  // Parse command-line argumetns
  graph_args args = parse_args(argc, argv);

  // Load the edge list
  load_edge_list(args.graph_filename);
  LOG("Dumping edge list...\n");
  dump_edge_list();

  // Build the graph
  LOG("Constructing graph...\n");
  construct_graph_from_edge_list(args.heavy_threshold);
  print_graph_distribution();

  // Check the graph
  LOG("Checking graph...");
  if (check_graph()) { LOG("PASS\n"); }
  else { LOG("FAIL\n"); }

  // Dump to file
  LOG("Dumping graph...\n");
  dump_graph();

  return 0;
}
