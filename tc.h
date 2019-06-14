#include "graph.h"

typedef struct tc_data {
    // Number of triangles this vertex is a part of
    long num_triangles;
} tc_data;

// Global replicated struct with BFS data pointers
extern tc_data TC;

void tc_init();
long tc_run();
void tc_data_clear();
bool tc_check();
void tc_deinit();

