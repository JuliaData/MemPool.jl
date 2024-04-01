using Distributed
import ConcurrentUtils as CU

mutable struct DRef
    owner::Int
    id::Int
    size::UInt
    function DRef(owner, id, size; doref::Bool=true)
        d = new(owner, id, size)
        doref && poolref(d)
        finalizer(poolunref, d)
        d
    end
end
Base.:(==)(d1::DRef, d2::DRef) = (d1.owner == d2.owner) && (d1.id == d2.id)
Base.hash(d::DRef, h::UInt) = hash(d.id, hash(d.owner, h))

const DRefID = Tuple{Int,Int}

const DEBUG_REFCOUNTING = Ref(false)

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
        pooltransfer_send_local(d, pid)
        return
    end
    pid = Distributed.worker_id_from_socket(io)
    if pid != -1
        pooltransfer_send_local(d, pid)
        return
    end
    @warn "Couldn't determine destination for DRef serialization\nRefcounting will be broken"
end
function _pooltransfer_send(io::AbstractSerializer, d::DRef)
    # We assume that we're not making copies of the serialized DRef
    # N.B. This is not guaranteed to be correct
    warn_dref_serdes()
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
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "<- (", d.owner, ", ", d.id, ") at ", myid(), "\n")
    poolref(d, true)
    finalizer(poolunref, d)
end
function _pooltransfer_recv(io::AbstractSerializer, d)
    # N.B. This is not guaranteed to be correct
    warn_dref_serdes()
    poolref(d)
    finalizer(poolunref, d)
    # Matches the poolref during serialization
    poolunref(d)
end

warn_dref_serdes() = @warn "Performing serialization of DRef with unknown serializer\nThis may fail or produce incorrect results" maxlog=1

# Ensure we call the DRef ctor
Base.copy(d::DRef) = deepcopy(d)
Base.deepcopy(d::DRef) = DRef(d.owner, d.id, d.size)

include("storage.jl")

storage_read(state::RefState) = @atomic :acquire state.storage
"Atomically replaces `state.storage` with the result of `f(state.storage)`."
function storage_rcu!(f, state::RefState)
    while true
        orig_sstate = @atomic :acquire state.storage

        # Generate new state based on old state
        new_sstate = f(orig_sstate)

        if new_sstate === orig_sstate
            throw(ConcurrencyViolationError("Attempted to atomically replace StorageState with itself"))
        end
        succ = (@atomicreplace :acquire_release :acquire state.storage orig_sstate => new_sstate).success
        if succ
            return new_sstate
        end
    end
end

using .Threads

# Global ID counter for generating local DRef IDs
const id_counter = Atomic{Int}(0)
# Lock for access to `datastore`
const datastore_lock = NonReentrantLock()
# Data store, maps local DRef ID to RefState, which holds the reference's values
const datastore = Dict{Int,RefState}()

struct RefCounters
    # Count of # of workers holding at least one reference
    worker_counter::Atomic{Int}
    # Count of # of local references
    local_counter::Atomic{Int}
    # Per other worker, count of # of sent references to other worker
    send_counters::Dict{Int,Int}
    # Per other worker, count of # of received references from other worker
    recv_counters::Dict{Int,Int}
    # Locks for access to send/recv counters
    tx_lock::NonReentrantLock
end
function RefCounters()
    rc = maybepop!(REFCOUNTERS_CACHE)
    if rc === nothing
        rc = RefCounters(Atomic{Int}(0),
                            Atomic{Int}(0),
                            Dict{Int,Int}(),
                            Dict{Int,Int}(),
                            NonReentrantLock())
    else
        Threads.atomic_sub!(REFCOUNTERS_STORED, 1)
        rc = something(rc)
    end
    return rc
end
function refcounters_replace!(rc)
    if REFCOUNTERS_STORED[] < REFCOUNTERS_CACHE_MAX
        rc.worker_counter[] = 0
        rc.local_counter[] = 0
        empty!(rc.send_counters)
        empty!(rc.recv_counters)
        Threads.atomic_add!(REFCOUNTERS_STORED, 1)
        push!(REFCOUNTERS_CACHE, rc)
    end
end
const REFCOUNTERS_CACHE = ConcurrentStack{RefCounters}()
const REFCOUNTERS_STORED = Threads.Atomic{Int}(0)
const REFCOUNTERS_CACHE_MAX = 2^12

Base.show(io::IO, ctrs::RefCounters) = with_lock(ctrs.tx_lock) do
    print(io, string(ctrs))
end
function Base.string(ctrs::RefCounters)
    "RefCounters(workers=" * string(ctrs.worker_counter[]) *
    ", local=" * string(ctrs.local_counter[]) *
    ", send=" * string(sum(values(ctrs.send_counters))) *
    ", recv=" * string(sum(values(ctrs.recv_counters))) * ")"
end

# Lock for access to `datastore_counters`
const datastore_counters_lock = NonReentrantLock()
# Per-DRef counters
const datastore_counters = Dict{DRefID, RefCounters}()

# Flag set when this session is exiting
const exit_flag = Ref{Bool}(false)

"""
Updates `local_counter` by `adj`, and checks if the ref is no longer
present on this worker. If so, all sent references are collected and sent to
the owner.
"""
function update_and_check_local!(ctrs, owner, id, adj)
    if atomic_add!(ctrs.local_counter, adj) == 0 - adj
        transfers = nothing
        @safe_lock_spin ctrs.tx_lock begin
            DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "LL (", owner, ", ", id, ") at ", myid(), " with ", string(ctrs), "\n"; gc_context=true)
            transfers = copy(ctrs.send_counters)
            empty!(ctrs.send_counters)
        end
        if myid() == owner
            # N.B. Immediately update counters to prevent hidden counts in send queue
            poolunref_owner(id, transfers; gc_context=true)
        else
            # Tell the owner we hold no more references
            _enqueue_work(remotecall_wait, poolunref_owner, owner, id, transfers; gc_context=true)
            @safe_lock_spin datastore_counters_lock begin
                delete!(datastore_counters, (owner, id))
            end
        end
    end
end

"""
Updates `worker_counter` by `adj`, and checks if the ref can be freed. If it
can be freed, it is immediately deleted from the datastore.
"""
function update_and_check_owner!(ctrs, id, adj)
    with_lock(ctrs.tx_lock) do
        tx_free = true
        for pid in keys(ctrs.recv_counters)
            if ctrs.recv_counters[pid] == 0
                delete!(ctrs.recv_counters, pid)
            else
                tx_free = false
                break
            end
        end
        if atomic_add!(ctrs.worker_counter, adj) == 0 - adj
            DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "OO (", myid(), ", ", id, ") with ", string(ctrs), "\n"; gc_context=true)
            if tx_free
                datastore_delete(id)
                return true
            end
            return false
        end
    end
end

# HACK: Force remote GC messages to be executed serially
mutable struct SendQueue
    queue::Channel{Any}
    @atomic task::Union{Task,Nothing}
    processing::Bool
end
const SEND_QUEUE = SendQueue(Channel(typemax(Int)), nothing, false)
function _enqueue_work(f, args...; gc_context=false)
    if SEND_QUEUE.task === nothing
        task = Task() do
            while true
                try
                    work, _args = take!(SEND_QUEUE.queue)
                    SEND_QUEUE.processing = true
                    work(_args...)
                    SEND_QUEUE.processing = false
                catch err
                    exit_flag[] && continue
                    err isa ProcessExitedException && continue # TODO: Remove proc from counters
                    iob = IOContext(IOBuffer(), :color=>true)
                    println(iob, "Error in enqueued work:")
                    Base.showerror(iob, err)
                    seek(iob.io, 0)
                    write(stderr, iob)
                end
            end
        end
        if @atomicreplace(SEND_QUEUE.task, nothing => task).success
            schedule(task)
            errormonitor(task)
        end
    end
    if gc_context
        while true
            GC.safepoint()
            if trylock(SEND_QUEUE.queue)
                try
                    put!(SEND_QUEUE.queue, (f, args))
                    break
                finally
                    unlock(SEND_QUEUE.queue)
                end
            end
        end
    else
        put!(SEND_QUEUE.queue, (f, args))
    end
end

function poolref(d::DRef, recv=false)
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "^^ (", d.owner, ", ", d.id, ") at ", myid(), "\n")
    ctrs = with_lock(datastore_counters_lock) do
        # This might be a new DRef
        get!(RefCounters, datastore_counters, (d.owner, d.id))
    end
    # Update the local refcount
    if atomic_add!(ctrs.local_counter, 1) == 0
        # We've never seen this DRef, so tell the owner
        DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "!! (", d.owner, ", ", d.id, ") at ", myid(), "\n")
        if myid() == d.owner
            poolref_owner(d.id, ctrs)
        else
            _enqueue_work(remotecall_wait, poolref_owner, d.owner, d.id)
        end
    end
    # We've received this DRef via transfer
    if recv
        if myid() == d.owner
            pooltransfer_recv_owner(d.id, myid())
        else
            _enqueue_work(remotecall_wait, pooltransfer_recv_owner, d.owner, d.id, myid())
        end
    end
end
"Called on owner when a worker first holds a reference to DRef with ID `id`."
function poolref_owner(id::Int, ctrs=nothing)
    free = false
    if ctrs === nothing
        ctrs = with_lock(()->datastore_counters[(myid(), id)],
                         datastore_counters_lock)
    end
    update_and_check_owner!(ctrs, id, 1)
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "== (", myid(), ", ", id, ")\n")
end
function poolunref(d::DRef)
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "vv (", d.owner, ", ", d.id, ") at ", myid(), "\n"; gc_context=true)
    # N.B. Runs in a finalizer context, so yielding is disallowed
    ctrs = @safe_lock_spin datastore_counters_lock begin
        @assert haskey(datastore_counters, (d.owner,d.id)) "poolunref called before any poolref (on $(myid())): ($(d.owner),$(d.id))"
        datastore_counters[(d.owner, d.id)]
    end
    update_and_check_local!(ctrs, d.owner, d.id, -1)
end
"Called on owner when a worker no longer holds any references to DRef with ID `id`."
function poolunref_owner(id::Int, transfers::Dict{Int,Int}; gc_context=false)
    xfers = sum(map(sum, values(transfers)))
    ctrs = if gc_context
        @safe_lock_spin datastore_counters_lock begin
            @assert haskey(datastore_counters, (myid(),id)) "poolunref_owner called before any poolref_owner: ($(myid()), $id)"
            datastore_counters[(myid(), id)]
        end
    else
        with_lock(datastore_counters_lock) do
            @assert haskey(datastore_counters, (myid(),id)) "poolunref_owner called before any poolref_owner: ($(myid()), $id)"
            datastore_counters[(myid(), id)]
        end
    end
    if gc_context
        @safe_lock_spin ctrs.tx_lock begin
            for pid in keys(transfers)
                old = get(ctrs.recv_counters, pid, 0)
                ctrs.recv_counters[pid] = old + transfers[pid]
            end
        end
    else
        with_lock(ctrs.tx_lock) do
            for pid in keys(transfers)
                old = get(ctrs.recv_counters, pid, 0)
                ctrs.recv_counters[pid] = old + transfers[pid]
            end
        end
    end
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "@@ (", myid(), ", ", id, ") with xfers ", xfers, " and ", string(ctrs), "\n"; gc_context)
    update_and_check_owner!(ctrs, id, -1)
end
function pooltransfer_send_local(d::DRef, to_pid::Int)
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "-> (", d.owner, ", ", d.id, ") to ", to_pid, "\n")
    ctrs = with_lock(()->datastore_counters[(d.owner, d.id)],
                     datastore_counters_lock)
    with_lock(ctrs.tx_lock) do
        prev = get(ctrs.send_counters, to_pid, 0)
        ctrs.send_counters[to_pid] = prev + 1
    end
end
function pooltransfer_recv_owner(id::Int, to_pid::Int)
    ctrs = with_lock(()->datastore_counters[(myid(), id)],
                     datastore_counters_lock)
    with_lock(ctrs.tx_lock) do
        prev = get(ctrs.recv_counters, to_pid, 0)
        ctrs.recv_counters[to_pid] = prev - 1
    end
    update_and_check_owner!(ctrs, id, 0)
end

isinmemory(state::RefState) = storage_read(state).data !== nothing
isondisk(state::RefState) =
    any(l->l.handle !== nothing, storage_read(state).leaves)
isinmemory(id::Int) =
    isinmemory(with_lock(()->datastore[id], datastore_lock))
isondisk(id::Int) =
    isondisk(with_lock(()->datastore[id], datastore_lock))
isinmemory(x::DRef) = isinmemory(x.id)
isondisk(x::DRef) = isondisk(x.id)

const MEM_RESERVED = Ref{UInt}(512 * (1024^2)) # Reserve 512MB of RAM for OS
const MEM_RESERVE_LOCK = Threads.ReentrantLock()

"""
When called, ensures that at least `MEM_RESERVED[] + size` bytes are available
to the OS. If there is not enough memory available, then a variety of calls to
the GC are performed to free up memory until either the reservation limit is
satisfied, or `max_sweeps` number of cycles have elapsed.
"""
function ensure_memory_reserved(size::Integer=0; max_sweeps::Integer=5)
    sat_sub(x::T, y::T) where T = x < y ? zero(T) : x-y

    # Check whether the OS is running tight on memory
    sweep_ctr = 0
    while true
        with(QUERY_MEM_OVERRIDE => true) do
            Int(storage_available(CPURAMResource())) - size < MEM_RESERVED[]
        end || break

        # We need more memory! Let's encourage the GC to clear some memory...
        sweep_start = time_ns()
        mem_used = with(QUERY_MEM_OVERRIDE => true) do
            storage_utilized(CPURAMResource())
        end
        if sweep_ctr == 0
            @debug "Not enough memory to continue! Sweeping up unused memory..."
            GC.gc(false)
        elseif sweep_ctr == 1
            GC.gc(true)
        else
            @everywhere GC.gc(true)
        end

        # Let finalizers run
        yield()

        # Wait for send queue to clear
        while SEND_QUEUE.processing
            yield()
        end

        with(QUERY_MEM_OVERRIDE => true) do
            mem_freed = sat_sub(mem_used, storage_utilized(CPURAMResource()))
            @debug "Freed $(Base.format_bytes(mem_freed)) bytes, available: $(Base.format_bytes(storage_available(CPURAMResource())))"
        end

        sweep_ctr += 1
        if sweep_ctr == max_sweeps
            @debug "Made too many sweeps, bailing out..."
            break
        end
    end
    if sweep_ctr > 0
        @debug "Swept for $sweep_ctr cycles"
    end
end

function poolset(@nospecialize(x), pid=myid(); size=approx_size(x),
                 retain=false, restore=false,
                 device=GLOBAL_DEVICE[], leaf_device=initial_leaf_device(device),
                 tag=nothing, leaf_tag=Tag(),
                 destructor=nothing)
    if pid == myid()
        if !restore
            @lock MEM_RESERVE_LOCK ensure_memory_reserved(size)
        end

        id = atomic_add!(id_counter, 1)
        sstate = if !restore
            StorageState(Some{Any}(x),
                         Vector{StorageLeaf}(),
                         device)
        else
            @assert !isa(leaf_device, CPURAMDevice) "Cannot use `CPURAMDevice()` as leaf device when `restore=true`"
            StorageState(nothing,
                         [StorageLeaf(leaf_device, Some{Any}(x), retain)],
                         device)
        end
        notify(sstate)
        state = RefState(sstate,
                         size;
                         tag,
                         leaf_tag,
                         destructor)
        rc = RefCounters()
        Threads.atomic_add!(rc.local_counter, 1)
        Threads.atomic_add!(rc.worker_counter, 1)
        with_lock(datastore_counters_lock) do
            datastore_counters[(pid, id)] = rc
        end
        with_lock(datastore_lock) do
            datastore[id] = state
        end
        DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "++ (", myid(), ", ", id, ") [", x, "]\n")
        d = DRef(myid(), id, size; doref=false)
        if !(restore && device === leaf_device)
            try
                write_to_device!(device, state, id)
            catch
                # Revert the root switch to ensure we don't try to delete
                notify(storage_rcu!(state) do sstate
                    StorageState(sstate; root=CPURAMDevice())
                end)
                rethrow()
            end
        end
        if retain
            retain_on_device!(device, state, id, true; all=true)
        end
        return d
    else
        # use our serialization
        remotecall_fetch(pid, MMWrap(x)) do wx
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

function poolget(ref::DRef)
    DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "?? (", ref.owner, ", ", ref.id, ") at ", myid(), "\n")
    original_ref = ref

    # Check global redirect cache
    ref = CU.lock_read(REDIRECT_CACHE_LOCK) do
        get(REDIRECT_CACHE, ref, ref)
    end

    # Fetch the value (or a RedirectTo) from the owner
    @label fetch
    value = if ref.owner == myid()
        _getlocal(ref.id, false)
    else
        forwardkeyerror() do
            remotecall_fetch(ref.owner, ref) do ref
                MMWrap(_getlocal(ref.id, true))
            end
        end
    end |> unwrap_payload

    if value isa RedirectTo
        # Redirected to a new ref, update the cache and try again
        @lock REDIRECT_CACHE_LOCK begin
            REDIRECT_CACHE[ref] = value.ref
            REDIRECT_CACHE[original_ref] = value.ref
        end
        ref = value.ref
        @goto fetch
    end

    return something(value)
end

function _getlocal(id, remote)
    state = with_lock(()->datastore[id], datastore_lock)
    CU.lock_read(state.lock) do
        if state.redirect !== nothing
            return RedirectTo(state.redirect)
        end
        return Some{Any}(read_from_device(state, id, true))
    end
end

function datastore_delete(id)
    @safe_lock_spin datastore_counters_lock begin
        DEBUG_REFCOUNTING[] && _enqueue_work(Core.print, "-- (", myid(), ", ", id, ") with ", string(datastore_counters[(myid(), id)]), "\n"; gc_context=true)
        ctrs = datastore_counters[(myid(), id)]
        refcounters_replace!(ctrs)
        delete!(datastore_counters, (myid(), id))
    end
    state = @safe_lock_spin datastore_lock begin
        haskey(datastore, id) ? datastore[id] : nothing
    end
    (state === nothing) && return
    errormonitor(Threads.@spawn begin
        device = storage_read(state).root
        if device !== nothing
            delete_from_device!(device, state, id)
        end
    end)
    @safe_lock_spin datastore_lock begin
        haskey(datastore, id) && delete!(datastore, id)
    end
    dtor = state.destructor
    if dtor !== nothing
        dtor()
    end
    return
end

## DRef migration/redirection

"""
    migrate!(ref::DRef, to::Integer) -> DRef

Migrate the data referenced by `ref` to another worker `to`, returning the new
`DRef` which references the new data. The existing `ref` is still usable, and
any accesses via `poolget` will seamlessly redirect to the new `DRef`, but the
data is no longer stored on the same worker as `ref`.
"""
function migrate!(ref::DRef, to::Integer)
    @assert ref.owner != to "Cannot migrate a DRef within the same worker"
    if ref.owner != myid()
        return remotecall_fetch(migrate!, ref.owner, ref, to)
    end
    state = with_lock(()->datastore[ref.id], datastore_lock)

    # Lock the ref against further accesses
    # FIXME: Below is racey w.r.t data mutation
    local new_ref
    @lock state.lock begin
        # Read the current value of the ref
        data = read_from_device(state, ref, true)

        # Create new ref to redirect to
        new_ref = remotecall_fetch(poolset, to, data)

        # Set the redirect to our new ref
        state.redirect = new_ref

        # Delete the local data
        delete_from_device!(state, ref)
    end

    return new_ref
end

struct RedirectTo
    ref::DRef
end

const REDIRECT_CACHE = WeakKeyDict{DRef,DRef}()
const REDIRECT_CACHE_LOCK = CU.ReadWriteLock()

## Default data directory

const SESSION = Ref{String}()

default_dir(p) = joinpath(homedir(), ".mempool", "$(SESSION[])-$p")
default_dir() = default_dir(myid())
default_path(r::DRef) = joinpath(default_dir(r.owner), string(r.id))

## Disk caching configuration

"""
    DiskCacheConfig

Helper struct that stores the config for the disk caching setup.
Handles either direct input or uses the ENVs from the original implementation.
Latest applied config can be found in the `DISKCACHE_CONFIG[]`.
"""
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
    toggle = something(toggle, parse(Bool, get(ENV, "JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR", "0")))
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

"""
    setup_global_device!(cfg::DiskCacheConfig)

Sets up the `GLOBAL_DEVICE` with a `SimpleRecencyAllocator` according to the provided `cfg`.
The latest applied config can be found in `DISKCACHE_CONFIG[]`.
"""
function setup_global_device!(cfg::DiskCacheConfig)
    if !cfg.toggle
        return nothing
    end
    if !isa(GLOBAL_DEVICE[],  CPURAMDevice)
        # This detects if a disk cache was already set up
        @warn(
            "Setting the disk cache config when one is already set will lead to " *
            "unexpected behavior and likely cause issues. Please restart the process " *
            "before changing the disk cache configuration. " *
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

    # Set the config to a global ref for future reference on exit or elsewhere
    DISKCACHE_CONFIG[] = cfg

    return nothing
end

# Stores the last applied disk cache config for future reference
const DISKCACHE_CONFIG = Ref{DiskCacheConfig}()
