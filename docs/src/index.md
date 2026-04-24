# MemPool: A framework for out-of-core and parallel execution

MemPool.jl is both a framework and in-memory datababse for storing and accessing Julia
objects, where those objects may live on local or remote (distributed) Julia processes.
This allows for communicating about data stored on remote workers, and even data
potentially paged-out to disk, with a single simple reference (the `DRef`).

As a database, MemPool stores references to objects, and also acts as a "gatekeeper"
when those objects are later accessed through their reference. It can be configured to
page data out to disk, and then when data is accessed, will page out other data to make
space in RAM for this newly-loaded data. This allows MemPool to provide "out-of-core"
data management for libraries or applications - Dagger.jl is one such library that utilizes
MemPool for this purpose.


### Remote Workers Caveat

When using MemPool with multiple workers, make sure that the workers are
initialized *before* importing MemPool. This ensures the package is loaded on all nodes:
```julia-repl
julia> using Distributed

julia> addprocs(2)

julia> using MemPool
```

-----

## Quickstart: Data Management

For more details: [Data Management](@ref)

The core of MemPool revolves around the `DRef` (Distributed Reference). A `DRef` is a pointer
to data that might live in local RAM, remote RAM, or on disk.

### Creating and retreiving data

Use `poolset` to register data with the pool and `poolget` to retrieve the actual value:

```julia
using MemPool

A = rand(1000, 1000)
ref = poolset(A)

A_retrieved = poolget(ref)
```
This will track a large array (`A`) as a `DRef` using `poolset(A)`. 
You can now safely clear the reference `A` (such as by `A = nothing`),
and later retrieve `A` from the `DRef` using `poolget(ref)`.


### Manual Worker Assignment

You can force data to be stored on a specific worker by passing a worker ID to 'poolset':

```julia
ref_w2 = poolset(rand(500), 2)
```

Note that if the current worker is not worker 2, this will make a copy of the array
from `rand(500)` on worker 2, and will not share memory with the original array.

## Quickstart: Out-of-Core Configuration

MemPool provides helper functions to setup out-of-core data management for all
`DRef`s created with `poolset`.

### Enabling the Disk Cache

```julia
# 1. Define the configuration
cfg = MemPool.DiskCacheConfig(
    toggle = true,
    membound = 4 * 1024^3,                  # 4GB RAM Limit
    diskpath = "/tmp/mempool_cache",        # Disk storage location
    allocator_type = "LRU"                  # Least Recently Used eviction
)

# 2. Apply the configuration
MemPool.setup_global_device!(cfg)
```

When the amount of data tracked by MemPool exceeds `membound` in byte size,
MemPool will perform activities such as triggering a GC sweep, or swapping other
data to `diskpath` and removing that other data from memory. Note that `diskpath`
must be a directory - each piece of data gets it own file.

## Memory Reservation Logic

MemPool includes a `ensure_memory_reserved` mechanism. When a `poolset` is called, the system checks if the OS is running
tight on memory. If so, it will:
1. Trigger a local GC.
2. If memory is still tight, trigger a full `GC.gc(true)`.
3. Finally, trigger a cluster-wide GC (`@everywhere GC.gc(true)`). 


### Quickstart: Persistence & Migration

## Migrating Data Between Workers

You can move data from one worker to another without breaking existing references:

```julia
# Move data from current owner to worker 3
new_ref = MemPool.migrate!(ref, 3)
```

## Managed File I/O

Treat files as managed `DRef` objects to avoid loading massive datasets into RAM all at once:

```julia
#Create a lazy refence to a serialized Julia file
f = MemPool.File("large_dataset.jls")

#Data is only loaded when explicitly requested
data = poolget(f)
```
