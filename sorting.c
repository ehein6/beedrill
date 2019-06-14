#include <stddef.h>
#include <cilk/cilk.h>
#include "sorting.h"

typedef int (*Comparator)(const void *, const void*);

// A utility function to swap two elements
static inline void
swap ( long* a, long* b )
{
    long t = *a;
    *a = *b;
    *b = t;
}

// Partition elements of the range [begin, end)
static inline long *
partition (long * begin, long * end, Comparator compare)
{
    // Choose last element as pivot
    long p = *(end - 1);

    // i: Marks pivot position
    // j: Scans the array from begin to end
    // If we encounter an element < p, swap and move pivot forwards
    long * i = (begin - 1);
    for (long * j = begin; j < end - 1; j++) {
        if (compare(j, &p) < 0) {
            i++;
            swap (i, j);
        }
    }
    // Put the pivot in place, and return a pointer to it
    swap (i + 1, end-1);
    return (i + 1);
}

// Recursive function calls are very expensive for us right now,
// since we need to create a stack frame for each level. Here we
// use an iterative algorithm that uses a single stack and does
// no function calls.
//
// Adapted from https://www.geeksforgeeks.org/iterative-quick-sort/
static void
iterative_quick_sort_longs(long * begin, long * end, Comparator compare)
{
    // Create an auxiliary stack
    // TODO This is a VLA, prefer explicit malloc instead?
    long * stack[end - begin];

    // Initialize top of stack
    long top = -1;

    // Push initial range to top of stack
    stack[ ++top ] = begin;
    stack[ ++top ] = end;

    // Keep popping from stack
    while ( top >= 0 ) {
        // Pop next range to partition
        end = stack[ top-- ];
        begin = stack[ top-- ];

        // Set pivot element at its correct position
        // in sorted array
        long * p = partition( begin, end, compare );

        // If there are elements on left side of pivot,
        // then push left side to stack
        if ( p-1 > begin ) {
            stack[ ++top ] = begin;
            stack[ ++top ] = p - 1;
        }

        // If there are elements on right side of pivot,
        // then push right side to stack
        if ( p+1 < end ) {
            stack[ ++top ] = p + 1;
            stack[ ++top ] = end;
        }
    }
}

static void
quick_sort_longs(long * begin, long * end, Comparator compare, long depth)
{
    // Arrays with fewer than this many elements will use serial sort
    const long grain = 32768;
    const long max_depth = 6;
    ptrdiff_t count = end - begin;
    if (count <= 0) {
        return;
    } else if (count > grain && depth < max_depth) {
        // Partition the array around a pivot
        long * p = partition(begin, end, compare);
        // Spawn thread to sort the left half
        if (begin < p - 1) {
            cilk_spawn quick_sort_longs(begin, p - 1, compare, depth+1);
        }
        // Recurse into the right half.
        if (p + 1 < end) {
            quick_sort_longs(p + 1, end, compare, depth+1);
        }
    } else {
        // Sort using a non-recursive, serial algorithm
        iterative_quick_sort_longs(begin, end, compare);
    }
}

void
emu_quick_sort_longs(long * begin, long * end, Comparator compare)
{
    quick_sort_longs(begin, end, compare, 0);
}

int
is_sorted(long * begin, long * end, Comparator compare)
{
    if (begin == end) { return 1; }
    for (long * i = begin; i < end-1; ++i) {
        if (compare(i, i + 1) > 0) {
            return 0;
        }
    }
    return 1;
}
