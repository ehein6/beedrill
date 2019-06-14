# Emu C++ Utilities

### Container Classes: 
Containers:
- `emu::striped_array<T>`: Wraps `mw_malloc1dlong()`. Can be used for things like `long`, `double`, or an array of pointers (must be 8 bytes). 
  - If pointer views are added, can extend this to support types that are longer than 8 bytes.
- `emu::blocked_array<T>`: Wraps `mw_mallocstripe()`. Can be used for 1D arrays of arbitrary objects. Uses a blocked allocation.
- `emu::ragged_array<T>` (Tentative): A 2D array, sub-arrays can have arbitrary lengths and are co-located on a single nodelet. Useful for CSR, sparse matrix, graph, etc.    

Note: `std::vector<T>` will still be a local array.

Advantages compared to C: 
- Automatic memory alloc/free.
- Iterators and operator overloads.
- Type safety 
- Handles construction/destruction of arbitrary types. 
Basically everything you get with a `std::vector<T>`, except distributed across the system.

### Parallel Function Templates

- Implement parallel versions of C++ standard library algorithms.
  - Collectives: `all_of`, `any_of`, `none_of`
  - Parallel iteration: `for_each`, `count_if`
  - Reductions/Prefix sum: `accumulate`, `partial_sum`
  - Sort/Dedup `sort`, `shuffle`, `unique_copy`
  - See https://en.cppreference.com/w/cpp/algorithm for full list
- Compiler selects the appropriate function based on data structure:
  - Calling `emu::parallel::for_each()` on a `std::vector` will spawn threads locally. 
  - Calling `emu::parallel::for_each()` on a `emu::blocked_array` will spawn threads remotely, across each block. 
  - Calling `emu::parallel::for_each()` on a `emu::striped_array` will use a striped indexing strategy to minimize migrations.
  - Can also pass a sub-range to any function.

### Replicated allocation   

- Provides support for placing C++ objects in replicated storage.
  - `repl<T>` : Convenience type for initializing primitives and pointers on each nodelet.
  - `make_repl_ctor<T>()`: Constructs an object with the same arguments on each nodelet.
    - For example, programmer defines a queue for BFS, then uses this to put a local queue on each nodelet. 
  - `make_repl_copy<T>()`: Constructs an object, then makes a shallow copy on each nodelet.
    - Used for distributed data structures, only one copy gets constructed/destructed, the others just mirror the data. 
    
## Example

```cpp
bool stream_example() 
{
    bool success = true;
    // Allocate distributed array of integers
    long n = 1024;
    auto array_ptr = emu::make_repl_copy<emu::striped_array<long>>(n);
    // References are easier to work with
    auto& array = *array_ptr;
    // Assign index to each value
    std::iota(array.begin(), array.end(), 0);
    // Sum of all integers from 0 to n
    long expected_sum = (n * (n - 1)) / 2;
    long actual_sum = emu::parallel::accumulate(array.begin(), array.end(), 0L);
    success &= (actual_sum == expected_sum);
    return success;
}
```