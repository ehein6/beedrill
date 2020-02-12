## Overview 

Beedrill is graph library written in C++/Cilk
and optimized for the Emu architecture. It is intended to demonstrate both the
best performance and the best practices for writing good Emu C++ code.

All of my graph benchmarks are named with a yellow jacket theme, to honor my
alma mater Georgia Tech. See also MeatBee (an early streaming graph benchmark
for Emu) and PaperWasp (the predecessor and C equivalent of this project). 

## Building the code

Build for Emu hardware/simulator:
```
mkdir build-hw && cd build-hw
cmake .. \
-DCMAKE_TOOLCHAIN_FILE=../cmake/emu-toolchain.cmake
-DLLVM_CILK=/usr/local/emu
make -j4
```

Thanks to the `memoryweb_x86` header, it is also possible to build and run 
Beedrill on an x86 system. This allows the use of a debugger and 
facilitates more rapid development.  

Build for testing on x86 (requires a Cilk-enabled compiler like Tapir: 
http://cilk.mit.edu/):
```
mkdir build-x86 && cd build-x86
cmake .. \
-DCMAKE_BUILD_TYPE=Debug
make -j4
```

## Generating graph inputs

Source code for an input generator is provided in the `generator` subdirectory. 
This is a separate subproject, which uses C++/OpenMP and is meant to run on an 
x86 system.  

The `rmat_dataset_dump` binary accepts a single argument, the name of the graph
file to generate. Two formats are allowed:

* `A-B-C-D-num_edges-num_vertices.rmat`: Generates a random graph using the RMAT
algorithm, with input parameters A, B, C, D, and the specified number of edges 
and vertices. Suffixes K/M/G/T can be used in place of 2^10, 2^20, 2^30, 2^40.   
* `graph500-scaleN`: Generates a random graph suitable for running the graph500
benchmark at scale N (but see caveots below). Uses the RMAT algorithm with 
parameters A=0.57, B=0.19, C=0.19, D=0.05, num_edges=16*2^N, num_vertices=2^N. 

The graph generation algorithm benefits from multiple cores and uses a lot of 
memory. Be careful when generating graphs at scale greater than 20 on a personal 
computer or laptop. 

## Running the benchmarks

Quick start: `./hybrid_bfs.mwx --alg beamer_hybrid --graph graph500-scale20`

Pass `--help` to see the available flags for each benchmark.

## Benchmarks

- hybrid_bfs: Runs a parallel breadth-first search over the graph. 
- components: Finds all the connected components in the graph
- pagerank: Runs the PageRank algorithm
- triangle_count: Counts the number of triangles in the graph
