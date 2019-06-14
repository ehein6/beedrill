#pragma once

void emu_quick_sort_longs(long * begin, long * end, int (*compare)(const void *, const void *));
int is_sorted(long * begin, long * end, int (*compare)(const void *, const void *));