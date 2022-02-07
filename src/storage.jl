include("fsinfo.jl")

"""
    StorageResource

The supertype for all storage resources. Any subtype represents some storage
hardware or modality (RAM, disk, NAS, VRAM, tape, etc.) where data can be
stored, and which has some current usage level and a maximum capacity (both
measures in bytes).

Storage resources are generally unique (although they may alias), and do not
represent a method of storing/loading data to/from the resource; instead, a
`StorageDevice` provides an actual implementation of such operations on one or
more resources.
"""
abstract type StorageResource end
storage_available(::StorageResource) = 0
storage_capacity(::StorageResource) = 0
storage_utilized(s::StorageResource) = storage_capacity(s) - storage_available(s)

"Represents CPU RAM."
struct CPURAMResource <: StorageResource end
if Sys.islinux()
function storage_available(::CPURAMResource)
    open("/proc/meminfo", "r") do io
        # skip first 2 lines
        readline(io)
        readline(io)
        line = readline(io)
        free = match(r"MemAvailable:\s*([0-9]*)\s.*", line).captures[1]
        parse(UInt64, free) * 1024
    end
end
else
# FIXME: Sys.free_memory() includes OS caches
storage_available(::CPURAMResource) = Sys.free_memory()
end
storage_capacity(::CPURAMResource) = Sys.total_memory()

"Represents a filesystem mounted at a given path."
struct FilesystemResource <: StorageResource
    mountpoint::String
end
storage_available(s::FilesystemResource) = disk_stats(s.mountpoint).available
storage_capacity(s::FilesystemResource) = disk_stats(s.mountpoint).capacity

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

# TODO: Docstrings
storage_available(dev::StorageDevice) = sum(res->storage_available(dev, res), storage_resources(dev))
storage_capacity(dev::StorageDevice) = sum(res->storage_capacity(dev, res), storage_resources(dev))
storage_utilized(dev::StorageDevice) = sum(res->storage_utilized(dev, res), storage_resources(dev))

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
storage_device(ref::DRef) =
    (@assert ref.owner == myid(); storage_device(ref.id))

"Returns the size of the data for reference `id`."
storage_size(id::Int) = datastore[id].size

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
    externally_varying(device::StorageDevice) -> Bool

Indicates whether the storage availability or capacity of device `device may
vary according to external forces, such as other unrelated processes or OS
behavior.

When `true`, this implies that the ability of `device to store data is
completely arbitrary. Typically this means that calls to storage availability
queries can return different results, even if no storage calls have been made
on `device.

When `false`, it may be reasonable to assume that exact accounting of storage
availability is possible, although it is not guaranteed. There are also no
guarantees that allocations will not trigger forced OS memory reclamation (such
as by the Linux OOM killer).
"""
externally_varying(::StorageDevice) = true

"""
    CPURAMDevice <: StorageDevice

Stores data in memory. This is the default device.
"""
struct CPURAMDevice <: StorageDevice end
storage_resources(dev::CPURAMDevice) = Set{StorageResource}(CPURAMResource())
storage_capacity(::CPURAMDevice, res::CPURAMResource) = storage_capacity(res)
storage_capacity(::CPURAMDevice) = storage_capacity(CPURAMResource())
storage_available(::CPURAMDevice, res::CPURAMResource) = storage_available(res)
storage_available(::CPURAMDevice) = storage_available(CPURAMResource())
storage_utilized(::CPURAMDevice, res::CPURAMResource) = storage_utilized(res)
storage_utilized(::CPURAMDevice) = storage_utilized(CPURAMResource())
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
storage_resources(dev::SerializationFileDevice) = Set{StorageResource}([dev.fs])
function storage_capacity(dev::SerializationFileDevice, res::FilesystemResource)
    @assert res == dev.fs
    storage_capacity(res)
end
storage_capacity(dev::SerializationFileDevice) = storage_capacity(dev.fs)
function storage_available(dev::SerializationFileDevice, res::FilesystemResource)
    @assert res == dev.fs
    storage_available(res)
end
storage_available(dev::SerializationFileDevice) = storage_available(dev.fs)
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
storage_resources(lru::LRUAllocator) =
    Set{StorageResource}([CPURAMResource(), lru.device])
function storage_capacity(lru::LRUAllocator, res::StorageResource)
    if res isa CPURAMResource
        return lru.mem_limit
    elseif res in storage_resources(lru.device)
        return lru.device_limit
    else
        throw(ArgumentError("$res not contained in $lru"))
    end
end
storage_capacity(lru::LRUAllocator) = lru.mem_limit + lru.device_limit
storage_available(lru::LRUAllocator, res::StorageResource) =
    storage_capacity(lru, res) - storage_utilized(lru, res)
storage_available(lru::LRUAllocator) =
    storage_capacity(lru) - storage_utilized(lru)
function storage_utilized(lru::LRUAllocator, res::StorageResource)
    if res isa CPURAMResource
        return sum(map(storage_size, lru.mem_refs))
    elseif res in storage_resources(lru.device)
        return sum(map(storage_size, lru.device_refs))
    else
        throw(ArgumentError("$res not contained in $lru"))
    end
end
storage_utilized(lru::LRUAllocator) =
    sum(map(storage_size, lru.mem_refs)) +
    sum(map(storage_size, lru.device_refs))
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
externally_varying(::LRUAllocator) = false

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
