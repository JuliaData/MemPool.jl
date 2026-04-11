# MemPool: A framework for out-of-core and parallel execution

MemPool.jl acts as a gatekeeper and looks at its internal table of "chunks". If space exists
for a DAG, it tells the Dagger Scheduler that it is good to run. It then handles the 
bit-by-bit transfer of data from where it is (RAM/Disk) into that GPU's memory. If there is
no space and it is full, it tells the Scheduler that the capacity is reached. 


Note: when using MemPool with multiple workers, make sure that the workers are
initialized *before* importing MemPool This ensures the package is loaded on all nodes:
```julia-repl
julia> using Distributed

julia> addprocs(2)

julia> using MemPool
```

-----

## Quickstart: Data Management

For more details: [Data Management](@ref)

The core of MemPool revolves around `DRef`(Distributed Reference). A `DRef` is a pointer
to data that might live in local RAM, remote RAM, or on disk.

### Creating and retreiving data

Use `poolset` to register data with the pool and `poolget` to retreive the actual value
```julia
using MemPool

A = rand(1000, 1000)
ref = poolset(A)

A_retrieved = poolget(ref)
```
This will move a large array (A) into a ref using `poolset(A)`. 
If you wanted to clear the data from local scope and retrive it later 
from the `DRef`, run `poolget(ref)`.


### Manual Worker Assignment

You can force data to be stored on a specific worker by passing a worker ID to 'poolset':

```julia
ref_w2 = poolset(rand(500), 2)
```

### Quickstart: Out-of-Core Configuration

When `membound` is reached, MemPool will trigger a GC sweep or move data to the `diskpath`.

## Enabling the Disk Cache
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
