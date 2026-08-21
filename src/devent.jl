# Distributed event (`DEvent`) and a `Future`-alike (`DFuture`) built on top of
# MemPool's `DRef` machinery.
#
# Motivation: `Distributed.Future`/`RemoteChannel` are unsafe under concurrent
# access from multiple threads within the same process. Every `put!`/`fetch`/
# `wait` mutates process-global tables (`client_refs`, `PGRP.refs`) via
# `lookup_ref`/`del_client`, and `fetch` auto-deletes the backing ref. Under
# heavy multithreaded access (e.g. Dagger's parallel datadeps scheduling) this
# races: a `lookup_ref` that runs after the ref has been deleted silently
# fabricates a fresh, never-fulfilled `RemoteValue`, and the waiter blocks
# forever.
#
# `DEvent`/`DFuture` avoid that entirely:
# - Readiness is signalled by a plain `Base.Event` living on the owner worker,
#   which is fully thread-safe. Same-process waits/notifies (the common case)
#   never touch any Distributed machinery.
# - Lifetime is managed by a backing `DRef`, reusing MemPool's battle-tested
#   distributed refcounting + serialization. The owner-side state is dropped via
#   the `DRef`'s `destructor` once the last reference (local or remote) is gone.
#   That `DRef` is only materialized *lazily*, the first time the `DEvent` is
#   serialized (see "promotion" below): the overwhelmingly common case in
#   practice (e.g. every Dagger task's future) is an event that is created,
#   waited on, and dropped without ever leaving the process, and a full `DRef`
#   lifecycle (`StorageState` + `RefState` + `RefCounters` + finalizer +
#   datastore/registry insertion, plus the eventual deletion path) costs
#   ~10-20 allocations that such an event has no use for. An unpromoted
#   `DEvent` holds its `DEventBox` directly and is reclaimed by plain GC.
# - Cross-worker operations are stateless RPCs (`remotecall_fetch`/
#   `remotecall_wait`) against the owner; there is no write-once ref that gets
#   deleted out from under a concurrent reader.
# - `DFuture`'s local value cache is never serialized: it's dropped (reset to
#   `nothing`) whenever a `DFuture` crosses the wire, so that an
#   already-fetched value isn't needlessly retransmitted to a process that may
#   not even need it.

"Owner-side state backing a `DEvent`/`DFuture`. Never serialized or spilled."
mutable struct DEventBox
    @atomic set::Bool
    @atomic value::Union{Some{Any},Nothing}
    # Lazily created on first blocking wait: a Base.Event costs ~5 allocations
    # (Event + condition + lock chain) and most boxes are set before anyone
    # ever needs to block on them
    @atomic event::Union{Base.Event,Nothing}
end
DEventBox() = DEventBox(false, nothing, nothing)

# Set-before-read pairing with `_devent_box_wait`: the notifier sets `set`
# first, then notifies any installed event; a waiter installs its event, then
# re-checks `set` before blocking, so a racing notify is never missed.
function _devent_box_notify(box::DEventBox)
    @atomic box.set = true
    event = @atomic box.event
    event !== nothing && notify(event)
    return
end
function _devent_box_wait(box::DEventBox)
    (@atomic box.set) && return
    event = @atomic box.event
    if event === nothing
        new_event = Base.Event()
        _, installed = @atomicreplace box.event nothing => new_event
        event = installed ? new_event : (@atomic box.event)::Base.Event
    end
    (@atomic box.set) && return
    wait(event)
    return
end
function _devent_box_put!(box::DEventBox, @nospecialize(v))
    _, ok = @atomicreplace box.value nothing => Some{Any}(v)
    ok || return # write-once: ignore double-puts leniently
    _devent_box_notify(box)
    return
end
function _devent_box_fetch(box::DEventBox)
    _devent_box_wait(box)
    return something(@atomic box.value)
end

# Owner-side registry: backing-`DRef` id => box, for *promoted* events only
# (an unpromoted `DEvent` reaches its box directly, and is never referenced by
# id). Accessed under a `NonReentrantLock` via spin-locking so it is safe to
# touch from the `DRef` destructor, which may run in a GC/finalizer context
# where task switches (and hence a blocking `lock`) are illegal.
const DEVENT_REGISTRY = Dict{Int,DEventBox}()
const DEVENT_REGISTRY_LOCK = NonReentrantLock()
# Tiny sentinel stored in the datastore for each backing `DRef`; we only use the
# ref for its identity + refcounting, not its payload.
const DEVENT_SENTINEL = :__mempool_devent__

_devent_box(id::Int) = @safe_lock_spin DEVENT_REGISTRY_LOCK begin
    get(DEVENT_REGISTRY, id, nothing)
end
function _devent_register!(id::Int, box::DEventBox)
    @safe_lock_spin DEVENT_REGISTRY_LOCK begin
        DEVENT_REGISTRY[id] = box
    end
    return
end
function _devent_delete!(id::Int)
    @safe_lock_spin DEVENT_REGISTRY_LOCK begin
        delete!(DEVENT_REGISTRY, id)
    end
    return
end

"""
    DEvent()
    DEvent(pid::Integer)

A distributed, one-shot event. `notify` sets it (idempotently) and `wait` blocks
until it is set. Safe under concurrent multithreaded access, and serializable to
other workers (all operations are then performed against the owning worker).

Locally-created events hold their state (a [`DEventBox`](@ref)) directly and
allocate no backing `DRef` until the first time they are serialized; see
[`_devent_promote!`](@ref).
"""
mutable struct DEvent
    # Non-`nothing` iff this event was created on this process, in which case
    # it is also the fast path for every operation (no registry lookup, no RPC).
    const box::Union{DEventBox,Nothing}
    # Backing ref, used for lifetime management and for reaching the box from
    # other processes. Created lazily on first serialization for locally-created
    # events; always set for deserialized ones.
    @atomic ref::Union{DRef,Nothing}
end
DEvent() = DEvent(DEventBox(), nothing)
function DEvent(pid::Integer)
    pid == myid() && return DEvent()
    # The remote side builds a local (unpromoted) `DEvent`; serializing it back
    # to us promotes it, so what we get here is a `box === nothing` event whose
    # ref is owned by `pid`.
    return remotecall_fetch(DEvent, pid)
end

# A locally-created event is owned by this process, promoted or not (the ref it
# promotes to is allocated here as well).
function owner(de::DEvent)
    de.box !== nothing && return myid()
    return ((@atomic de.ref)::DRef).owner
end

"""
    _devent_promote!(de::DEvent) -> DRef

Return `de`'s backing `DRef`, creating (and registering) it if this
locally-created event doesn't have one yet. Idempotent and thread-safe: racing
promotions all return the same, single winning ref.
"""
function _devent_promote!(de::DEvent)
    ref = @atomic de.ref
    ref === nothing || return ref
    box = de.box::DEventBox
    idbox = Ref{Int}(0)
    newref = poolset(DEVENT_SENTINEL; destructor = () -> _devent_delete!(idbox[]))
    idbox[] = newref.id
    _devent_register!(newref.id, box)
    prev, won = @atomicreplace de.ref nothing => newref
    won && return newref
    # Lost the race: drop our registration (the winner's is the one everyone
    # will look up) and let `newref` be collected; its destructor's
    # `_devent_delete!` is then a harmless no-op.
    _devent_delete!(newref.id)
    return prev::DRef
end

# Serializing is what forces promotion; only the ref crosses the wire, so the
# receiving process sees a `box === nothing` event that RPCs back to us.
function Serialization.serialize(io::AbstractSerializer, de::DEvent)
    Serialization.serialize_type(io, DEvent)
    serialize(io, _devent_promote!(de))
end
function Serialization.deserialize(io::AbstractSerializer, ::Type{DEvent})
    ref = deserialize(io)::DRef
    return DEvent(nothing, ref)
end

function _devent_notify_local(id::Int)
    box = _devent_box(id)
    box === nothing && return
    _devent_box_notify(box)
    return
end
function Base.notify(de::DEvent)
    box = de.box
    if box !== nothing
        _devent_box_notify(box)
        return de
    end
    ref = (@atomic de.ref)::DRef
    o = ref.owner
    if o == myid()
        _devent_notify_local(ref.id)
    else
        remotecall_wait(_devent_notify_local, o, ref.id)
    end
    return de
end

function _devent_wait_local(id::Int)
    box = _devent_box(id)
    box === nothing && return # already cleaned up => must have fired
    _devent_box_wait(box)
    return
end
function Base.wait(de::DEvent)
    box = de.box
    if box !== nothing
        _devent_box_wait(box)
        return de
    end
    ref = (@atomic de.ref)::DRef
    o = ref.owner
    if o == myid()
        _devent_wait_local(ref.id)
    else
        remotecall_fetch(_devent_wait_local, o, ref.id)
    end
    return de
end

function _devent_isset_local(id::Int)
    box = _devent_box(id)
    box === nothing && return true
    return @atomic box.set
end
function isset(de::DEvent)
    box = de.box
    box !== nothing && return @atomic box.set
    ref = (@atomic de.ref)::DRef
    o = ref.owner
    if o == myid()
        return _devent_isset_local(ref.id)
    else
        return remotecall_fetch(_devent_isset_local, o, ref.id)
    end
end

"""
    DFuture()
    DFuture(pid::Integer)

A write-once, `Future`-like value cell built on `DEvent`. Supports `put!`,
`fetch`, `wait`, and `isready`, and is safe under concurrent multithreaded
access (unlike `Distributed.Future`). Serializable to other workers.
"""
mutable struct DFuture
    event::DEvent
    @atomic cache::Union{Some{Any},Nothing} # local value cache
end
DFuture() = DFuture(DEvent(), nothing)
DFuture(pid::Integer) = DFuture(DEvent(pid), nothing)

# `cache` is deliberately dropped on serialization: it's a local convenience
# copy, and re-sending it would waste bandwidth re-transmitting (possibly
# large) data that the destination may never even `fetch`. The destination
# just re-populates its own cache lazily, via the backing `DEvent`, on its
# first `fetch`.
function Serialization.serialize(io::AbstractSerializer, f::DFuture)
    Serialization.serialize_cycle_header(io, f) && return
    serialize(io, f.event)
end
function Serialization.deserialize(io::AbstractSerializer, ::Type{DFuture})
    f = ccall(:jl_new_struct_uninit, Any, (Any,), DFuture)
    Serialization.deserialize_cycle(io, f)
    event = deserialize(io)
    ccall(:jl_set_nth_field, Cvoid, (Any, Csize_t, Any), f, 0, event)
    ccall(:jl_set_nth_field, Cvoid, (Any, Csize_t, Any), f, 1, nothing)
    return f
end

function _devent_put_local(id::Int, @nospecialize(v))
    box = _devent_box(id)
    box === nothing && return
    _devent_box_put!(box, v)
    return
end
function Base.put!(f::DFuture, @nospecialize(v))
    de = f.event
    box = de.box
    if box !== nothing
        _devent_box_put!(box, v)
        return f
    end
    ref = (@atomic de.ref)::DRef
    o = ref.owner
    if o == myid()
        _devent_put_local(ref.id, v)
    else
        remotecall_wait(_devent_put_local, o, ref.id, v)
    end
    return f
end

function _devent_fetch_local(id::Int)
    box = _devent_box(id)
    box === nothing && error("DFuture value is unavailable (already cleaned up)")
    return _devent_box_fetch(box)
end
function Base.fetch(f::DFuture)
    c = @atomic f.cache
    c !== nothing && return something(c)
    de = f.event
    box = de.box
    v = if box !== nothing
        _devent_box_fetch(box)
    else
        ref = (@atomic de.ref)::DRef
        o = ref.owner
        if o == myid()
            _devent_fetch_local(ref.id)
        else
            remotecall_fetch(_devent_fetch_local, o, ref.id)
        end
    end
    @atomicreplace f.cache nothing => Some{Any}(v)
    return v
end

Base.wait(f::DFuture) = (wait(f.event); f)
Base.isready(f::DFuture) = ((@atomic f.cache) !== nothing) || isset(f.event)
