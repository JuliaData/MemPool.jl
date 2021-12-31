abstract type StorageResource end

struct CPURAMResource <: StorageResource end

struct FilesystemResource <: StorageResource
    mountpoint::String
end

"""
    StorageDevice

The abstract supertype of all storage devices. A `StorageDevice` must implement
`movetodevice!`, `readfromdevice`, and `deletefromdevice!`. A `StorageDevice`
may reflect a mechanism for storing data on persistent storage, or it may be an
allocator which manages other `StorageDevice`s.

See the `SerializationFileDevice` for an example of how to implement a data
store.

When implementing an allocator, it's recommended to provide options that users
can tune to control how many bytes of storage may be used for each managed
`StorageDevice`. This makes it easier for users to predict the amount of
storage that a given piece of code can use. See the `LRUAllocator` for a simple
example of how to implement an allocator.
"""
abstract type StorageDevice end

"Returns the storage handle referring to the data contained in reference `id`."
storage_handle(id::Int) = (datastore[id].file::StorageState).handle

"Returns the true storage device for reference `id`."
function storage_device(id::Int)
    rstate = datastore[id]
    if rstate.data !== nothing
        return CPURAMDevice()
    end
    (rstate.file::StorageState).device
end

"""
    writetodevice!(device::StorageDevice, id::Int)

Writes reference `id`'s data to `device`. Reads the data into memory first (via
`readfromdevice(CPURAMDevice(), id)`) if necessary.
"""
writetodevice!

"""
    readfromdevice(device::StorageDevice, id::Int, ret::Bool) -> Any

Access the value of reference `id` from `device`, and return it if `ret` is
`true`; if `ret` is `false`, then the value is not actually retrieved, but
internal counters may be updated to account for this access.

Callers must ensure that the data is currently stored on `device` before
calling this. Callees that read/write data directly must set
`datastore[id].data` appropriately when `ret` is `true`.
"""
readfromdevice

"Force a read into memory from the true storage device."
function readfromdevice(id::Int)
    s = datastore[id]
    if !isinmemory(s)
        readfromdevice((s.file::StorageState).device, id, true)
    end
    something(s.data)
end

"""
    deletefromdevice!(device::StorageDevice, id::Int)

Delete reference `id`'s data from `device`, such that upon return, the space
used by the previously-referenced data is now available for allocating to other
data.
"""
deletefromdevice!

"""
    CPURAMDevice <: StorageDevice

Stores data in memory. This is the default device.
"""
struct CPURAMDevice <: StorageDevice end
function writetodevice!(device::CPURAMDevice, ref_id)
    s = datastore[ref_id]
    if !isinmemory(s)
        sstate = s.file::StorageState
        data = readfromdevice(sstate.device, ref_id, true)
        s.data = Some{Any}(data)
    end
    return ref_id
end
readfromdevice(::CPURAMDevice, ref_id, ret) = ret ? something(datastore[ref_id].data) : nothing
function deletefromdevice!(::CPURAMDevice, ref_id)
    datastore[ref_id].data = nothing
end

"""
    SerializationFileDevice <: StorageDevice

Stores data in a temporary file, using the `Serialization` stdlib to write and
read data.
"""
struct SerializationFileDevice <: StorageDevice
    fs::FilesystemResource
    dir::String
end
"Construct a `SerializationFileDevice` which stores data in the directory `dir`."
SerializationFileDevice(dir) =
    SerializationFileDevice(FilesystemResource(Sys.iswindows() ? "C:" : "/"), dir)
SerializationFileDevice() =
    SerializationFileDevice(joinpath(tempdir(), ".mempool"))
function writetodevice!(device::SerializationFileDevice, ref_id)
    mkpath(device.dir)
    path = tempname(device.dir; cleanup=false)
    s = datastore[ref_id]
    fref = FileRef(path, s.size)
    open(path, "w") do io
        serialize(io, MMWrap(something(s.data)))
    end
    _writetodevice!(ref_id, device, fref)
    return ref_id
end
function readfromdevice(device::SerializationFileDevice, id, ret)
    if ret
        datastore[id].data !== nothing && return
        fref = storage_handle(id)
        data = unwrap_payload(open(deserialize, fref.file, "r+"))
        datastore[id].data = Some{Any}(data)
        data
    end
end
function deletefromdevice!(device::SerializationFileDevice, id)
    fref = storage_handle(id)
    rm(fref.file; force=true)
    datastore[id].file = nothing
end

struct StorageState
    device::StorageDevice
    handle
end

"""
    LRUAllocator <: StorageDevice

A simple LRU allocator device which manages an `upper` device and a `lower`
device. The `upper` device is be limited to `upper_limit` bytes of storage;
when an allocation exceeds this limit, the least recently accessed data will be
moved to the `lower` device (which is unbounded), and the new allocation will
be moved to the `upper` device.

Consider using an `upper` device of `CPURAMDevice` and a `lower` device of
`SerializationFileDevice` to implement a basic swap-to-disk allocator. Such a
device will be created and used automatically if the environment variable
`JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR` is set to `1` or `true`.
"""
struct LRUAllocator <: StorageDevice
    mem_limit::UInt64
    device::StorageDevice
    device_limit::UInt64
    mem_refs::Vector{Int}
    device_refs::Vector{Int}
end
LRUAllocator(mem_limit, device, device_limit) =
    LRUAllocator(mem_limit, device, device_limit, Int[], Int[])
function writetodevice!(lru::LRUAllocator, ref_id)
    ref_state = datastore[ref_id]
    if ref_state.size > lru.mem_limit
        # Won't fit in memory, can we demote?
        if ref_state.size > lru.device_limit
            # Too bulky, won't fit in device either
            throw(ArgumentError("DRef data for ID $ref_id too large!"))
        end
        # Demote to device immediately
        lru_migrate!(lru, ref_id, false)
        deletefromdevice!(CPURAMDevice(), ref_id)
        @assert ref_state.file !== nothing
        return
    end
    lru_migrate!(lru, ref_id, true)
end
function lru_migrate!(lru::LRUAllocator, ref_id, to_mem)
    ref_size = datastore[ref_id].size
    if to_mem
        # Demoting to device
        from_refs = lru.mem_refs
        from_limit = lru.mem_limit
        from_device = CPURAMDevice()
        to_refs = lru.device_refs
        to_limit = lru.device_limit
        to_device = lru.device
    else
        # Promoting to memory
        from_refs = lru.device_refs
        from_limit = lru.device_limit
        from_device = lru.device
        to_refs = lru.mem_refs
        to_limit = lru.mem_limit
        to_device = CPURAMDevice()
    end
    from_size = sum(map(x->datastore[x].size, from_refs))
    to_size = sum(map(x->datastore[x].size, to_refs))
    idx = to_mem ? lastindex(from_refs) : firstindex(from_refs)
    # TODO: Plan migrations to determine feasibility
    while ref_size + from_size > from_limit
        # Demote older/promote newer refs until space is available
        @assert 1 <= idx <= length(from_refs) "Failed to make $ref_size bytes of space available"
        oref = from_refs[idx]
        oref_size = datastore[oref].size
        if oref_size + to_size <= to_limit
            # Destination has space for this ref
            readfromdevice(from_device, oref, true)
            # N.B. We `writetodevice!` before deleting from old device, in case
            # the write fails (so we don't lose data)
            if to_mem
                writetodevice!(to_device, oref)
            end
            push!(to_refs, oref)
            deletefromdevice!(from_device, oref)
            deleteat!(from_refs, idx)
            @assert something(datastore[oref].data, datastore[oref].file) !== nothing
            from_size -= oref_size
            to_size += oref_size
        end
        idx += to_mem ? -1 : 1
    end

    # Space available, perform migration
    readfromdevice(ref_id)
    writetodevice!(from_device, ref_id)
    pushfirst!(from_refs, ref_id)
    if (idx = findfirst(x->x==ref_id, to_refs)) !== nothing
        deletefromdevice!(to_device, ref_id)
        deleteat!(to_refs, idx)
        @assert something(datastore[ref_id].data, datastore[ref_id].file) !== nothing
    end
end
function readfromdevice(lru::LRUAllocator, id, ret)
    if (idx = findfirst(x->x==id, lru.mem_refs)) !== nothing
        deleteat!(lru.mem_refs, idx)
        pushfirst!(lru.mem_refs, id)
        readfromdevice(CPURAMDevice(), id, ret)
    else
        idx = findfirst(x->x==id, lru.device_refs)
        @assert idx !== nothing
        lru_migrate!(lru, id, true)
        device = storage_device(id)
        readfromdevice(device, id, ret)
    end
end
function deletefromdevice!(lru::LRUAllocator, id)
    if (idx = findfirst(x->x==id, lru.mem_refs)) !== nothing
        deletefromdevice!(CPURAMDevice(), lru.mem_refs[idx])
        deleteat!(lru.mem_refs, idx)
    else
        idx = findfirst(x->x==id, lru.device_refs)
        @assert idx !== nothing
        deletefromdevice!(lru.device, lru.device_refs[idx])
        deleteat!(lru.device_refs, idx)
    end
end

function setdevice!(device::StorageDevice, id)
    old_device = device_map[id]
    old_device == device && return
    readfromdevice(device, id, true)
    writetodevice!(device, id)
    deletefromdevice!(old_device, id)
    device_map[id] = device
end
function setdevice!(device::StorageDevice, ref::DRef)
    if ref.owner == myid()
        setdevice!(device, ref.id)
    else
        remotecall_wait(setdevice!, ref.owner, ref.id)
    end
end

const GLOBAL_DEVICE = Ref{StorageDevice}(CPURAMDevice())
const device_map = Dict{Int,StorageDevice}()
