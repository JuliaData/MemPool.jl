## datastore
using Distributed

mutable struct DRef
    owner::Int
    id::Int
    size::UInt
    function DRef(owner, id, size)
        d = new(owner, id, size)
        poolref(d)
        finalizer(poolunref, d)
        d
    end
end
Base.:(==)(d1::DRef, d2::DRef) = (d1.owner == d2.owner) && (d1.id == d2.id)
Base.hash(d::DRef, h::UInt) = hash(d.id, hash(d.owner, h))

const DRefID = Tuple{Int,Int}

function Serialization.serialize(io::AbstractSerializer, d::DRef)
    Serialization.serialize_cycle_header(io, d)
    serialize(io, d.owner)
    serialize(io, d.id)
    serialize(io, d.size)

    _pooltransfer_send(io, d)
end
function _pooltransfer_send(io::Distributed.ClusterSerializer, d::DRef)
    pid = Distributed.worker_id_from_socket(io.io)
    if pid != -1
        pooltransfer_send(d, pid)
        return
    end
    pid = Distributed.worker_id_from_socket(io)
    if pid != -1
        pooltransfer_send(d, pid)
        return
    end
    @warn "Couldn't determine destination for DRef serialization\nRefcounting will be broken"
end
function _pooltransfer_send(io::AbstractSerializer, d::DRef)
    # We assume that we're not making copies of the serialized DRef
    # N.B. This is not guaranteed to be correct
    poolref(d)
end
function Serialization.deserialize(io::AbstractSerializer, dt::Type{DRef})
    # Construct the object
    nf = fieldcount(dt)
    d = ccall(:jl_new_struct_uninit, Any, (Any,), dt)
    Serialization.deserialize_cycle(io, d)
    for i in 1:nf
        tag = Int32(read(io.io, UInt8)::UInt8)
        if tag != Serialization.UNDEFREF_TAG
            ccall(:jl_set_nth_field, Cvoid, (Any, Csize_t, Any), d, i-1, Serialization.handle_deserialize(io, tag))
        end
    end

    _pooltransfer_recv(io, d)
    return d
end
function _pooltransfer_recv(io::Distributed.ClusterSerializer, d)
    # Add a new reference manually, and unref on finalization
    poolref(d, true)
    finalizer(poolunref, d)
end
function _pooltransfer_recv(io::AbstractSerializer, d)
    # N.B. This is not guaranteed to be correct
    poolref(d)
    finalizer(poolunref, d)
    # Matches the poolref during serialization
    poolunref(d)
end

# Ensure we call the DRef ctor
Base.copy(d::DRef) = deepcopy(d)
Base.deepcopy(d::DRef) = DRef(d.owner, d.id, d.size)

include("storage.jl")

mutable struct RefState
    size::UInt64
    data::Union{Some{Any}, Nothing}
    file::Union{StorageState, Nothing}
    destroyonevict::Bool # if true, will be removed from memory
end
function readfromdevice(d::DRef)
    if d.owner == myid()
        readfromdevice(d.id)
    else
        remotecall_fetch(readfromdevice, d.owner, d.id)
    end
end

using .Threads

const datastore = Dict{Int,RefState}()
const datastore_lock = Ref{Union{Nothing, NonReentrantLock}}(nothing)
const id_counter = Ref{Union{Nothing, Atomic{Int}}}(nothing)
const datastore_counter = Dict{DRefID, Atomic{Int}}() # TODO: We don't need owner
const local_datastore_counter = Dict{DRefID, Atomic{Int}}()
const send_datastore_counter = Dict{DRefID, Dict{Int,Atomic{Int}}}()
const recv_datastore_counter = Dict{DRefID, Dict{Int,Atomic{Int}}}()
const exit_flag = Ref{Bool}(false)

function canfree(id)
    owner = myid()
    datastore_counter[(owner, id)][] != 0 && return false
    if haskey(recv_datastore_counter, (owner, id))
        dict = recv_datastore_counter[(owner, id)]
        for pid in keys(dict)
            dict[pid][] != 0 && return false
        end
    end
    return true
end

function clear_counters!(id)
    owner = myid()
    haskey(datastore_counter, (owner, id)) &&
        delete!(datastore_counter, (owner, id))
    haskey(local_datastore_counter, (owner, id)) &&
        delete!(local_datastore_counter, (owner, id))
    haskey(recv_datastore_counter, (owner, id)) &&
        delete!(recv_datastore_counter, (owner, id))
end

# HACK: Force remote GC messages to be executed serially
const SEND_QUEUE = Channel(typemax(Int))
const SEND_TASK = Ref{Task}()
function _enqueue_work(f, args...; gc_context=false)
    if !isassigned(SEND_TASK)
        SEND_TASK[] = @async begin
            while true
                try
                    work, _args = take!(SEND_QUEUE)
                    work(_args...)
                catch err
                    exit_flag[] && continue
                    err isa ProcessExitedException && continue # TODO: Remove proc from counters
                    iob = IOContext(IOBuffer(), :color=>true)
                    println(iob, "Error in enqueued work:")
                    Base.showerror(iob, err)
                    seek(iob, 0)
                    write(stderr, iob)
                end
            end
        end
    end
    if gc_context
        while true
            GC.safepoint()
            if trylock(SEND_QUEUE)
                try
                    put!(SEND_QUEUE, (f, args))
                    break
                finally
                    unlock(SEND_QUEUE)
                end
            end
        end
    else
        put!(SEND_QUEUE, (f, args))
    end
end

function poolref(d::DRef, recv=false)
    with_datastore_lock() do
        ctr = get!(local_datastore_counter, (d.owner,d.id)) do
            # We've never seen this DRef, so tell the owner
            _enqueue_work(remotecall_wait, poolref_owner, d.owner, d.id)
            Atomic{Int}(0)
        end
        # We've received this DRef via transfer
        if recv
            _enqueue_work(remotecall_wait, pooltransfer_recv, d.owner, d.id, myid())
        end
        # Update the local refcount
        atomic_add!(ctr, 1)
    end
end
"Called on owner when a worker first holds a reference to DRef with ID `id`."
function poolref_owner(id::Int)
    with_datastore_lock() do
        ctr = get!(datastore_counter, (myid(),id)) do
            Atomic{Int}(0)
        end
        atomic_add!(ctr, 1)
        if canfree(id)
            clear_counters!(id)
            datastore_delete(id)
        end
    end
end
function poolunref(d::DRef, to_pid=0)
    @assert haskey(local_datastore_counter, (d.owner,d.id)) "poolunref called before any poolref $(myid()): ($(d.owner),$(d.id))"
    @safe_lock_spin datastore_lock[] begin
        ctr = local_datastore_counter[(d.owner,d.id)]
        atomic_sub!(ctr, 1)
        if ctr[] == 0
            delete!(local_datastore_counter, (d.owner,d.id))
            transfers = get(send_datastore_counter, (d.owner,d.id), Dict{Int,Atomic{Int}}())
            delete!(send_datastore_counter, (d.owner,d.id))
            # Tell the owner we hold no more references
            _enqueue_work(remotecall_wait, poolunref_owner, d.owner, d.id, transfers; gc_context=true)
        end
    end
end
"Called on owner when a worker no longer holds any references to DRef with ID `id`."
function poolunref_owner(id::Int, transfers::Dict{Int,Atomic{Int}})
    with_datastore_lock() do
        @assert haskey(datastore_counter, (myid(),id)) "poolunref_owner called before any poolref_owner"
        ctr = datastore_counter[(myid(),id)]
        atomic_sub!(ctr, 1)
        rctr = get!(recv_datastore_counter, (myid(),id)) do
            Dict{Int,Atomic{Int}}()
        end
        for pid in keys(transfers)
            recv_ctr = get!(rctr, pid, Atomic{Int}(0))
            atomic_add!(recv_ctr, transfers[pid][])
        end
        if canfree(id)
            clear_counters!(id)
            datastore_delete(id)
        end
    end
end
function pooltransfer_send(d::DRef, to_pid::Int)
    with_datastore_lock() do
        dict = get!(send_datastore_counter, (d.owner,d.id)) do
            Dict{Int,Atomic{Int}}()
        end
        ctr = get!(dict, to_pid) do
            Atomic{Int}(0)
        end
        atomic_add!(ctr, 1)
    end
end
function pooltransfer_recv(id::Int, to_pid::Int)
    with_datastore_lock() do
        dict = get!(recv_datastore_counter, (myid(),id)) do
            Dict{Int,Atomic{Int}}()
        end
        ctr = get!(dict, to_pid) do
            Atomic{Int}(0)
        end
        atomic_sub!(ctr, 1)
        if canfree(id)
            clear_counters!(id)
            datastore_delete(id)
        end
    end
end

if VERSION >= v"1.3.0-DEV"
function with_datastore_lock(f)
    @safe_lock datastore_lock[] begin
        f()
    end
end
else
with_datastore_lock(f) = f()
end

isinmemory(x::RefState) = x.data !== nothing
isondisk(x::RefState) = x.file !== nothing

function poolset(@nospecialize(x), pid=myid(); size=approx_size(x), destroyonevict=false, file=nothing, device=GLOBAL_DEVICE[])
    if pid == myid()
        id = atomic_add!(id_counter[], 1)
        with_datastore_lock() do
            datastore[id] = RefState(size,
                                     Some{Any}(x),
                                     nothing,
                                     destroyonevict)
        end
        d = DRef(myid(), id, size)
        if file !== nothing
            rootdir = Sys.iswindows() ? "C:" : "/"
            device = SerializationFileDevice(FilesystemResource(rootdir), file)
            fref = FileRef(file, size)
            with_datastore_lock() do
                datastore[id].file = StorageState(device, fref)
                device_map[id] = device
            end
            return d
        end
        with_datastore_lock() do
            try
                writetodevice!(device, id)
                device_map[id] = device
            catch err
                datastore_delete(id)
                rethrow(err)
            end
        end
        d
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
const enable_random_fref_serve = Ref(true)
const wrkrips = Dict{IPv4,Vector{Int}}() # cached IP-pid map to lookup FileRef locations

is_my_ip(ip::String) = getipaddr() == IPv4(ip)
is_my_ip(ip::IPv4) = getipaddr() == ip

function get_wrkrips()
    d = Dict{IPv4,Vector{Int}}()
    for w in Distributed.PGRP.workers
        if w isa Distributed.Worker
            wip = IPv4(w.config.bind_addr)
        else
            wip = isdefined(w, :bind_addr) ? IPv4(w.bind_addr) : MemPool.host
        end
        if wip in keys(d)
            if enable_random_fref_serve[]
                push!(d[wip], w.id)
            else
                d[wip] = [min(d[wip][1], w.id)]
            end
        else
            d[wip] = [w.id]
        end
    end

    loopback = ip"127.0.0.1"
    if (loopback in keys(d)) && (length(d) > 1)
        # there is a chance that workers on loopback are actually on same ip as master
        realip = remotecall_fetch(getipaddr, first(d[loopback]))
        if realip in keys(d)
            append!(d[realip], d[loopback])
        else
            d[realip] = d[loopback]
        end
        delete!(d, loopback)
    end
    d
end

get_worker_at(ip::String) = get_worker_at(IPv4(ip))
get_worker_at(ip::IPv4) = rand(get_workers_at(ip))

function get_workers_at(ip::IPv4)
    isempty(wrkrips) && merge!(wrkrips, remotecall_fetch(get_wrkrips, 1))
    wrkrips[ip]
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
        x = remotecall_fetch(get_worker_at(r.host), r.file) do file
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
    state = with_datastore_lock(()->datastore[id])
    device = device_map[id]
    if isinmemory(state)
        readfromdevice(device, id, false)
        return something(state.data)
    end
    return readfromdevice(device, id, true)
end

function datastore_delete(id)
    state = haskey(datastore, id) ? datastore[id] : nothing
    (state === nothing) && return
    if haskey(device_map, id)
        device = device_map[id]
        deletefromdevice!(device, id)
        delete!(device_map, id)
    end
    # release
    state.data = nothing
    haskey(datastore, id) && delete!(datastore, id)
    # TODO: maybe cleanup from the who_has_read list?
    nothing
end

function cleanup()
    empty!(file_to_dref)
    empty!(who_has_read)
    ks = with_datastore_lock() do
        collect(keys(datastore))
    end
    map(datastore_delete, ks)
    d = default_dir(myid())
    isdir(d) && rm(d; recursive=true)
    nothing
end

function pooldelete(r::DRef)
    if r.owner != myid()
        return remotecall_fetch(pooldelete, r.owner, r)
    end
    with_datastore_lock() do
        datastore_delete(r.id)
    end
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
        return remotecall_fetch(movetodisk, r.owner, r, path, keepinmemory)
    end

    mkpath(dirname(path))
    if isfile(path)
        return FileRef(path, r.size)
    end
    open(path, "w") do io
        serialize(io, MMWrap(poolget(r)))
    end
    s = with_datastore_lock() do
        datastore[r.id]
    end
    # XXX: is this OK??
    rootdir = Sys.iswindows() ? "C:" : "/"
    device = SerializationFileDevice(FilesystemResource(rootdir), path)
    fref = FileRef(path, r.size)
    s.file = StorageState(device, fref)
    with_datastore_lock() do
        device_map[r.id] = device
    end

    return fref
end
function _writetodevice!(ref_id, device, handle, keepinmemory=true)
    new_ref = StorageState(device, handle)
    s = datastore[ref_id]
    # TODO: Atomic cmpswap (need single field)
    s.file = new_ref
    if !keepinmemory
        s.data = nothing
    end
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
