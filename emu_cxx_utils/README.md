# Emu C++ Utilities

## Overview

A header-only C++ library for Emu which provides parallel function templates,
distributed container classes, replicated object wrappers, and type-safe 
versions of Emu intrinsics.

### intrinsics.h

Implements type-safe wrappers for the Emu intrinsic functions (atomic_addms, 
remote_add, atomic_cas, etc. ). 

```c++
long counter;
MyClass * pointer;
emu::atomic_addms(&counter, 1); // increment counter by 1
emu::atomic_addms(&pointer, 1); // increment pointer by sizeof(MyClass)
```

### pointer_manipulation.h

Provides several functions for doing type-safe manipulation of Emu pointers. 
While equivalent functionality is available in the memoryweb library, these
functions tend to be more efficient since they are defined in a header and
can be inlined. 

In most cases these functions should not be used directly; rely on the library
to handle pointer magic for you. 

### execution_policy.h

Defines execution policy tags. Algorithms in the `emu::parallel` namespace such 
as `for_each`, `reduce`, and `fill` all accept an execution policy as the first
argument which specifies how the function should be executed by the hardware. 
If no argument is passed, a default policy is used. 

Serial policies:
- `emu::sequenced_policy` (`seq`) : Execute in a single thread

Parallel policies: Each of these policies has a grain size argument. 
- `emu::parallel_policy` (`par`): Execute using multiple threads. One thread 
will be spawned for each granule.
- `emu::parallel_fixed_policy` (`fixed`): Execute using a team of worker 
threads, giving each thread the same number of iterations. The grain size is 
interpreted as a minimum. 
- `emu::parallel_dynamic_policy` (`dyn`): Execute using a team of worker 
threads. Each thread will grab iterations from a work queue using atomic add. 
When operating on distributed arrays, there will be one work queue per nodelet.
The iterator must be a raw pointer. 

Compare with https://en.cppreference.com/w/cpp/algorithm/execution_policy_tag_t

### for_each.h

Implements parallel overloads of the `std::for_each` function,
documented at https://en.cppreference.com/w/cpp/algorithm/for_each. 

- Compiler selects the appropriate spawning pattern based on data structure:
  - Calling `emu::parallel::for_each()` on a `std::vector` will spawn threads 
  locally. 
  - Calling `emu::parallel::for_each()` on a `emu::striped_array` will spawn 
  threads across the entire syste that use a striped indexing strategy to 
  minimize migrations.

### reduce.h

Implements parallel overloads of the `std::reduce` function,
documented at https://en.cppreference.com/w/cpp/algorithm/reduce.

### fill.h

Implements parallel versions of the `std::fill` function,
documented at https://en.cppreference.com/w/cpp/algorithm/fill.

### nlet_stride_iterator.h

Defines `emu::nlet_stride_iterator<Iterator>`, an iterator wrapper that 
advances the underlying iterator by `NODELETS()` every time it is incremented.
This is useful when walking over a subset of a striped data structure, as every
element will be on the same nodelet. Used internally by `for_each` and `reduce`.

### striped_array.h 
Provides the `emu::striped_array<T>` container class, which wraps 
`mw_malloc1dlong()`-style striped arrays. Can be used for things like `long`, 
`double`, or any pointer (must be 8 bytes). 

Advantages compared to `mw_malloc1dlong`: 
- Automatic memory alloc/free.
- Iterators and operator overloads.
- Type safety 
- Handles construction/destruction of arbitrary types. 

This class is repl-aware, it can be safely nested in replicated classes 
or within `emu::repl_copy`. 

### repl_array.h
Provides the `emu::repl_array<T>` container class, which wraps `mw_mallocrepl`. 
Creates an array of the same size on each nodelet. Functions `get_nth` and 
`get_localto` are provided for getting a pointer to the array on a given 
nodelet.  

This class is repl-aware, it can be safely nested in replicated classes 
or within `emu::repl_copy`.

### replicated.h

Provides support for replicated objects in C++. 

#### Replicated storage:

The lifetime of an ordinary object in C++ looks like this:
1. Allocation
2. Construction
3. Destruction
4. Deallocation

For objects on the heap, steps 1+2 are handled by `operator new`, and steps 3+4
are handled by `operator delete`. 
For objects on the stack, these actions occur automatically when the object
comes into scope and goes out of scope. 

The Emu architecture allows us to place objects in replicated storage, which
means there is storage allocated for the object on each nodelet at the same
offset. Taking the address of a replicated object will result in a view-0 
(nodelet-relative) pointer, which always accesses the local copy of the object
when the thread migrates. The use of replicated objects can reduce the size of
the migrating thread context and reduce the number of spurious migrations.     

The memoryweb library provides several features for working with replicated 
storage:

- `replicated` : place a global object in replicated storage
- `mw_mallocrepl`: allocate storage for a replicated object on each nodelet, and
 return a view-0 (nodelet-relative) pointer to the storage.
- `mw_get_nth` : convert the nodelet-relative (view-0) pointer to an absolute 
(view-1) pointer to the copy of the object on a given nodelet.
- `mw_free`: deallocate storage for the replicated object

But note that these only handle allocation/deallocation of replicated objects.
This header provides template wrappers to handle the full lifetime of 
replicated objects and to provide the expected semantics. 

#### Replicated object wrapper templates:

`emu::repl<T>`: Replicated primitive (`int`, `long`, `MyClass*`, etc.)

- Reads will return the value of the local copy.
- Writes will assign the value to all copies.
- Must not be created on the stack. Use `new`, `emu::make_repl<T>()`, or compose
 within another replicated object. 
- If you want a replicated primitive that does not broadcast writes to
all nodelets, just use a regular primitive in replicated storage (i.e. `long` 
instead of `emu::repl<long>`.  
- Replicated references (i.e. `emu::repl<MyClass&>`) are not supported yet. 
Use a pointer instead.
 
`emu::repl_ctor<T>`: Replicated object with deep copies. 

- Constructor will be called separately on each nodelet with the same arguments.
- Each copy will be destructed individually. 
- Within method calls, `this` will be a nodelet-relative (view-0) pointer. If 
you wish to call a method on a particular copy of the object, use `get_nth`.
- Must not be created on the stack. Use `new` or `emu::make_repl_ctor<T>()`.
 
`emu::repl_copy<T>`: Replicated object with shallow copies

- Calls the constructor on the local replicated copy, then makes a shallow copy
of the local object on each remote nodelet. The semantics of "shallow copy" 
depend on T and can be customized by the user
  - If T is trivially copyable (no nested objects or custom copy constructor), 
remote copies will be initialized as bitwise copies using `memcpy()`.
  - If T defines a "shallow copy constructor", it will be used to construct 
the shallow copies. The "shallow copy constructor" has the signature
`T(const T& other, emu::shallow_copy)`, where the second argument can be 
ignored. The shallow copy constructor should perform shallow copies on all
members of the class (by simple assignment or calling their shallow copy 
constructors as appropriate). 
- The destructor will be called on only one copy, the other shallow copies are
not destructed. Note that this is not guaranteed to be the same storage that
the constructor was called upon.
- Within method calls, `this` will be a nodelet-relative (view-0) pointer. If 
you wish to call a method on a particular copy of the object, use `get_nth`.
- Must not be created on the stack. Use `new` or `emu::make_repl_copy<T>()`.

#### Function templates for replicated objects

`emu::repl_for_each` maps a function onto each copy of a replicated variable.

`emu::repl_reduce` uses a binary function to combine the values from each copy
of a replicated variable into a single value (for example, to add up all the
subtotals of a replicated accumulator variable). 


### TODO

- Implement parallel versions of remaining C++ standard library algorithms. 
Most of these can be done in terms of `for_each` and `reduce`:
  - Collectives: `all_of`, `any_of`, `none_of`
  - Reductions/Prefix sum: `accumulate`, `partial_sum`, `count_if`
  - Sort/Dedup `sort`, `shuffle`, `unique_copy`  