## datastore

struct DRef
    owner::Int
    id::Int
    size::UInt
end

mutable struct RefState
    size::UInt64
    data::Nullable{Any}
    file::Nullable{String}
    destroyonevict::Bool # if true, will be removed from memory
end

isinmemory(x::RefState) = !isnull(x.data)
isondisk(x::RefState) = !isnull(x.file)

const datastore = Dict{Int,RefState}()
const id_counter = Ref(0)

function poolset(x, pid=myid(); size=approx_size(x), destroyonevict=false, file=nothing)
    if pid == myid()
        id = id_counter[] += 1
        lru_free(size)
        datastore[id] = RefState(size,
                                 Nullable{Any}(x),
                                 Nullable{String}(file),
                                 destroyonevict)
        lru_touch(id, size)
        DRef(myid(), id, size)
    else
        # use our serialization
        remotecall_fetch(pid, MMWrap(x)) do wx
            # todo: try to evict here before deserializing...
            poolset(unwrap_payload(wx), pid)
        end
    end
end

function forwardkeyerror(f)
    try
        f()
    catch err
        if isa(err, RemoteException) && isa(err.captured.ex, KeyError)
            rethrow(err.captured.ex)
        end
        rethrow(err)
    end
end

const file_to_dref = Dict{String, DRef}()
const who_has_read = Dict{String, Vector{DRef}}() # updated only on master process
const enable_who_has_read = Ref(true)

is_my_ip(ip) = getipaddr() == IPv4(ip)
function get_worker_at(ip)
    for wrkr in Base.Distributed.PGRP.workers
        (IPv4(wrkr.bind_addr) == IPv4(ip)) && (return wrkr.id)
    end
    error("no worker at $ip")
end


function poolget(r::FileRef)
    # since loading a file is expensive, and often
    # requested in quick succession
    # we add the data to the pool
    if haskey(file_to_dref, r.file)
        ref = file_to_dref[r.file]
        if ref.owner == myid()
            return poolget(ref)
        end
    end

    if is_my_ip(r.host)
        x = unwrap_payload(open(deserialize, r.file, "r+"))
    else
        x = remotecall_fetch(get_worker_at(ip), r.file) do file
            unwrap_payload(open(deserialize, file, "r+"))
        end
    end
    dref = poolset(x, file=r.file, size=r.size)
    file_to_dref[r.file] = dref
    if enable_who_has_read[]
        remotecall_fetch(1, dref) do dr
            who_has = MemPool.who_has_read
            if !haskey(who_has, r.file)
                who_has[r.file] = DRef[]
            end
            push!(who_has[r.file], dr)
            nothing
        end
    end
    return x
end

function poolget(r::DRef)
    if r.owner == myid()
        _getlocal(r.id, false)
    else
        forwardkeyerror() do
            remotecall_fetch(r.owner, r) do r
                MMWrap(_getlocal(r.id, true))
            end
        end
    end |> unwrap_payload
end

function _getlocal(id, remote)
    state = datastore[id]
    if remote
        if isondisk(state)
            return FileRef(get(state.file), state.size)
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
            data = unwrap_payload(open(deserialize, get(state.file)))
            state.data = Nullable{Any}(data)
            lru_touch(id, state.size) # load back to memory
            return data
        end
    end
    error("ref id $id not found in memory or on disk!")
end

function datastore_delete(id)
    state = datastore[id]
    if isondisk(state)
        f = get(state.file)
        isfile(f) && rm(f; force=true)
    end
    # release
    state.data = Nullable{Any}()
    delete!(lru_order, id)
    delete!(datastore, id)
    # TODO: maybe cleanup from the who_has_read list?
    nothing
end

function cleanup()
    map(datastore_delete, collect(keys(datastore)))
    d = default_dir(myid())
    isdir(d) && rm(d; recursive=true)
    nothing
end

function pooldelete(r::DRef)
    if r.owner != myid()
        return remotecall_fetch(pooldelete, r.owner, r)
    end
    datastore_delete(r.id)
end

function pooldelete(r::FileRef)
    isfile(r.file) && rm(r.file)
    (r.file in keys(file_to_dref)) && delete!(file_to_dref, r.file)
    # TODO: maybe cleanup from the who_has_read list?
    nothing
end

function destroyonevict(r::DRef, flag::Bool=true)
    if r.owner == myid()
        datastore[r.id].destroyonevict = flag
    else
        forwardkeyerror() do
            remotecall_fetch(destroyonevict, r.owner, r, flag)
        end
    end
end


default_dir(p) = joinpath(".mempool", "$session-$p")
default_path(r::DRef) = joinpath(default_dir(r.owner), string(r.id))

function movetodisk(r::DRef, path=default_path(r), keepinmemory=false) 
    if r.owner != myid()
        return remotecall_fetch(movetodisk, r.owner, r, path)
    end

    mkpath(dirname(path))
    if isfile(path)
        return FileRef(path, r.size)
    end
    open(path, "w") do io
        serialize(io, MMWrap(poolget(r)))
    end
    s = datastore[r.id]
    s.file = Nullable(path)
    fref = FileRef(path, r.size)
    if !keepinmemory
        s.data = Nullable{Any}()
        delete!(lru_order, r.id)
    end

    return fref
end

copytodisk(r::DRef, path=default_path(r)) = movetodisk(r, path, true)

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
        return FileRef(path, r.size)
    else
        return remotecall_fetch(savetodisk, r.owner, r, path)
    end
end

function deletefromdisk(r::DRef, path)
    if r.owner == myid()
        return rm(path)
    else
        return remotecall_fetch(deletefromdisk, r.owner, r, path)
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

const spilltodisk = Ref(false)

function lru_free(sz)
    list = lru_evictable(sz)
    for id in list
        state = datastore[id]
        ref = DRef(myid(), id, state.size)
        if state.destroyonevict
            pooldelete(ref)
        else
            !spilltodisk[] && continue
            movetodisk(ref)
        end
    end
end
