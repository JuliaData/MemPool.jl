module MemPool

using Serialization, Sockets, Random
import Serialization: serialize, deserialize
export DRef, FileRef, poolset, poolget, mmwrite, mmread, cleanup
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
    file::String
    size::UInt
end
function FileRef(file; pid=nothing)
    size = try
        UInt(Base.stat(file).size)
    catch err
        @debug "Failed to query FileRef size of $file"
        UInt(0)
    end
    return FileRef(file, size)
end

unwrap_payload(f::FileRef) = unwrap_payload(open(deserialize, f.file, "r+"))

approx_size(f::FileRef) = f.size

include("io.jl")
include("lock.jl")
include("datastore.jl")

"""
    approx_size(d)

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

function __init__()
    SESSION[] = "sess-" * randstring(6)

    DISKCACHE_CONFIG[] = diskcache_config = DiskCacheConfig()
    setup_global_device!(diskcache_config)

    # Ensure we cleanup all references
    atexit(exit_hook)
end
function exit_hook()
    exit_flag[] = true

    evict_delay = DISKCACHE_CONFIG[].evict_delay
    kill_counter = evict_delay

    function datastore_empty(do_lock=true)
        with_lock(datastore_lock, do_lock) do
            all(ref->storage_read(ref).root isa CPURAMDevice, values(datastore))
        end
    end

    # Wait for datastore objects to naturally expire
    GC.gc()
    yield()
    while kill_counter > 0 && !datastore_empty()
        GC.gc()
        sleep(1)
        kill_counter -= 1
    end

    # Forcibly evict remaining objects
    with_lock(datastore_lock) do
        if !datastore_empty(false)
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
