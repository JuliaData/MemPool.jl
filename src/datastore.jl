## datastore

export poolset, poolget, pooldelete, movetodisk, copytodisk, savetodisk

struct DRef
    owner::Int
    id::Int
end

mutable struct RefState
    size::UInt64
    data::Nullable{Any}
    file::Nullable{String}
    spilltodisk::Bool # if set to false, will be destroyed on eviction
end

isinmemory(x::RefState) = !isnull(x.data)
isondisk(x::RefState) = !isnull(x.file)

const datastore = Dict{Int,RefState}()
const id_counter = Ref(0)

function poolset(x, pid=myid())
    if pid == myid()
        id = id_counter[] += 1
        sz = approx_size(x)
        lru_free(sz)
        datastore[id] = RefState(sz,
                                 Nullable{Any}(x),
                                 Nullable{String}(),
                                 true)
        lru_touch(id, sz)
        DRef(myid(), id)
    else
        # use our serialization
        remotecall_fetch(pid, MMWrap(x)) do wx
            # todo: try to evict here before deserializing...
            poolset(unwrap_payload(wx), pid)
        end
    end
end

function poolget(r::FileRef)
    unwrap_payload(r)
end

function poolget(r::DRef)
    if r.owner == myid()
        _getlocal(r.id, false)
    else
        try
            remotecall_fetch(r.owner, r) do r
                MMWrap(_getlocal(r.id, true))
            end
        catch err
            if isa(err, RemoteException)
                if isa(err.captured.ex, KeyError)
                    rethrow(err.captured.ex)
                end
            end
            rethrow(err)
        end
    end |> unwrap_payload
end

function _getlocal(id, remote)
    state = datastore[id]
    if remote
        if isondisk(state)
            return FileRef(get(state.file))
        elseif isinmemory(state)
            lru_touch(id, state.size)
            return get(state.data)
        end
    else
        # local
        if isinmemory(state)
            lru_touch(id, state.size)
            return get(state.data)
        elseif isondisk(state)
            # TODO: get memory mapped size and not count it as
            # much as local process size
            lru_free(state.size)
            lru_touch(id, state.size) # load back to memory
            return unwrap_payload(FileRef(get(state.file)))
        end
    end
    error("ref id $id not found in memory or on disk!")
end

function pooldelete(r::DRef)
    if r.owner != myid()
        return remotecall_fetch(pooldelete, r.owner, r)
    end

    state = datastore[r.id]
    if isondisk(state)
        rm(get(state.file))
    end
    # release
    state.data = Nullable{Any}()
    delete!(lru_order, r.id)
    delete!(datastore, r.id)
    nothing
end

default_path(r::DRef) = ".mempool/$session-$(r.owner)/$(r.id)"

function movetodisk(r::DRef, path=default_path(r), keepinmemory=false) 
    if r.owner != myid()
        return remotecall_fetch(movetodisk, r.owner, r, path)
    end

    mkpath(dirname(path))
    if isfile(path)
        warn("$(r) is already on disk, skipping.")
        return FileRef(path)
    end
    open(path, "w") do io
        serialize(io, MMWrap(poolget(r)))
    end
    s = datastore[r.id]
    s.file = Nullable(path)
    if !keepinmemory
        s.data = Nullable{Any}()
        delete!(lru_order, r.id)
    end

    return FileRef(path)
end

copytodisk(r::DRef, path=default_path(r)) = movetodisk(r, true)

"""
Allow users to specifically save something to disk.
This does not free the data from memory, nor does it affect
size accounting.
"""
function savetodisk(r::DRef, path)
    if r.owner == myid()
        open(path, "w") do io
            serialize(io, MMWrap(poolget(r)))
        end
        return FileRef(path)
    else
        return remotecall_fetch(savetodisk, r.owner, r, path)
    end
end

### LRU

using DataStructures

const max_memsize = Ref{UInt}(2^30)
const lru_order = OrderedDict{Int, UInt}()

# marks the ref as most recently used
function lru_touch(id::Int, sz = datastore[id].size)
    if haskey(lru_order, id)
        delete!(lru_order, id)
        lru_order[id] = sz
    else
        lru_order[id] = sz
    end
end

# returns ref ids that can be evicted to keep
# space below max_memsize
function lru_evictable(required=0)
    total_size = UInt(sum(values(lru_order)) + required)
    if total_size <= max_memsize[]
        return Int[] # nothing to evict
    end

    size_to_evict = total_size - max_memsize[]
    memsum = UInt(0)
    evictable = Int[]

    for (k, v) in lru_order
        if total_size - memsum <= max_memsize[]
            break
        end
        memsum += v
        push!(evictable, k)
    end
    return evictable
end

function lru_free(sz)
    list = lru_evictable(sz)
    for id in list
        state = datastore[id]
        ref = DRef(myid(), id)
        if state.spilltodisk
            movetodisk(ref)
        else
            pooldelete(ref)
        end
    end
end
