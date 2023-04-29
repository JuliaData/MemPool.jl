module MemPool

using Serialization, Sockets, Random
import Serialization: serialize, deserialize
export DRef, FileRef, poolset, poolget, pooldelete,
       movetodisk, copytodisk, savetodisk, mmwrite, mmread, cleanup,
       deletefromdisk, poolref, poolunref
import .Threads: ReentrantLock

## Wrapping-unwrapping of payloads:

struct MMWrap # wrap an object to use custom
                 # memory-mapping/fast serializer
    x::Any
end

# Wrapping is an implementation detail unwrap_payload will be called by poolget
# on every fetched value this will give back the value as it was saved.

unwrap_payload(x) = x
unwrap_payload(x::Tuple) = map(unwrap_payload, x)
unwrap_payload(x::MMWrap) = unwrap_payload(x.x)

function serialize(io::AbstractSerializer, w::MMWrap)
    # ticket to mm land
    mmwrite(io, w.x)
end

function deserialize(io::AbstractSerializer, T::Type{MMWrap})
    MMWrap(mmread(T, io)) # gotta keep that wrapper on
end


# Unwrapping FileRef == reading the file.
# This allows communicating only the file name across processors,
# the receiver process will simply read from file while unwrapping
struct FileRef
    host::IPAddr
    file::String
    size::UInt
    force_pid::Ref{Union{Int, Nothing}}

    function FileRef(file, size; pid=nothing)
        new(host, file, size, Ref{Union{Int, Nothing}}(pid))
    end
end

unwrap_payload(f::FileRef) = unwrap_payload(open(deserialize, f.file, "r+"))

include("io.jl")
include("lock.jl")
include("datastore.jl")

"""
`approx_size(d)`

Returns the size of `d` in bytes used for accounting in MemPool datastore.
"""
function approx_size(@nospecialize(d))
    Base.summarysize(d) # note: this is accurate but expensive
end

function approx_size(d::Union{Base.BitInteger, Float16, Float32, Float64})
    sizeof(d)
end

function approx_size(d::AbstractDict{K,V}) where {K,V}
    N = length(d)
    ksz = approx_size(K, N, keys(d))
    vsz = approx_size(V, N, values(d))
    ksz + vsz + 8*N
end

function approx_size(d::AbstractArray{T}) where T
    isempty(d) && return 0
    isbitstype(T) && return sizeof(d)
    approx_size(T, length(d), d)
end

function approx_size(T, L, d)
    fl = fixedlength(T)
    if fl > 0
        return L * fl
    elseif T === Any
        return L * 64  # approximation (override with a more specific method where exact calculation is needed)
    elseif isempty(d)
        return 0
    else
        return sum(approx_size(x) for x in d)
    end
end

function approx_size(xs::AbstractArray{String})
    # doesn't check for redundant references, but
    # really super fast in comparison to summarysize
    s = 0
    for x in xs
        s += sizeof(x)
    end
    s + 4 * length(xs)
end

function approx_size(s::String)
    sizeof(s)+sizeof(Int) # sizeof(Int) for 64 bit vs 32 bit systems
end

function approx_size(s::Symbol)
    sizeof(s)+sizeof(Int)
end

struct DiskCacheConfig
    toggle::Bool
    membound::Int
    diskpath::AbstractString
    diskdevice::StorageDevice
    diskbound::Int
    allocator_type::AbstractString
    evict_delay::Int
end

function DiskCacheConfig(;
    toggle::Union{Nothing,Bool}=nothing,
    membound::Union{Nothing,Int}=nothing,
    diskpath::Union{Nothing,AbstractString}=nothing,
    diskdevice::Union{Nothing,StorageDevice}=nothing,
    diskbound::Union{Nothing,Int}=nothing,
    allocator_type::Union{Nothing,AbstractString}=nothing,
    evict_delay::Union{Nothing,Int}=nothing,
)
    toggle = something(toggle, tryparse(Bool,get(ENV, "JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR", "0")))
    membound = something(membound, parse(Int, get(ENV, "JULIA_MEMPOOL_EXPERIMENTAL_MEMORY_BOUND", repr(8*(1024^3)))))
    diskpath = something(diskpath, get(ENV, "JULIA_MEMPOOL_EXPERIMENTAL_DISK_CACHE", joinpath(default_dir(), randstring(6))))
    diskdevice = something(diskdevice, SerializationFileDevice(FilesystemResource(), diskpath))
    diskbound = something(diskbound, parse(Int, get(ENV, "JULIA_MEMPOOL_EXPERIMENTAL_DISK_BOUND", repr(32*(1024^3)))))
    allocator_type = something(allocator_type, get(ENV, "JULIA_MEMPOOL_EXPERIMENTAL_ALLOCATOR_KIND", "MRU"))
    evict_delay = something(evict_delay, parse(Int, get(ENV, "JULIA_MEMPOOL_EXIT_EVICT_DELAY", "0")))

    allocator_type ∉ ("LRU", "MRU") && throw(ArgumentError("Unknown allocator kind: $allocator_type. Available types: LRU, MRU"))

    return DiskCacheConfig(
        toggle,
        membound,
        diskpath,
        diskdevice,
        diskbound,
        allocator_type,
        evict_delay,
    )
end

function setup_global_device!(cfg::DiskCacheConfig)
    if !cfg.toggle
        return nothing
    end
    if !isa(GLOBAL_DEVICE[],  CPURAMDevice)
        # This detects if a disk cache was already set up
        @warn(
            "Setting the disk cache config when one is already set will lead to " *
            "unexpected behavior and likely cause issues. Please restart the process" *
            "before changing the disk cache configuration." *
            "If this warning is unexpected you may need to " *
            "clear the `JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR` ENV."
        )
    end

    GLOBAL_DEVICE[] = SimpleRecencyAllocator(
        cfg.membound,
        cfg.diskdevice,
        cfg.diskbound,
        Symbol(cfg.allocator_type),
    )
    return nothing
end

function __init__()
    global session = "sess-" * randstring(6)
    try
        global host = getipaddr()
    catch err
        global host = Sockets.localhost
    end

    diskcache_config = DiskCacheConfig()
    setup_global_device!(diskcache_config)

    # Ensure we cleanup all references
    atexit(exit_hook)
end
function exit_hook()
    exit_flag[] = true
    evict_delay = something(tryparse(Int, get(ENV, "JULIA_MEMPOOL_EXIT_EVICT_DELAY", "0")), 0)::Int
    kill_counter = evict_delay
    empty!(file_to_dref)
    empty!(who_has_read)
    GC.gc()
    yield()
    while kill_counter > 0 && with_lock(()->!isempty(datastore), datastore_lock)
        GC.gc()
        sleep(1)
        kill_counter -= 1
    end
    with_lock(datastore_lock) do
        if length(datastore) > 0
            @debug "Failed to cleanup datastore after $evict_delay seconds\nForcibly evicting all entries"
            for id in collect(keys(datastore))
                state = MemPool.datastore[id]
                device = storage_read(state).root
                if device !== nothing
                    @debug "Evicting ref $id with device $device"
                    try
                        delete_from_device!(device, state, id)
                    catch
                    end
                end
                delete!(MemPool.datastore, id)
            end
        end
    end
    if ispath(default_dir())
        rm(default_dir(); recursive=true)
    end
end
precompile(exit_hook, ())

end # module
