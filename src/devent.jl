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
    const event::Base.Event # autoreset=false: stays signalled once notified
end
DEventBox() = DEventBox(false, nothing, Base.Event())

# Owner-side registry: backing-`DRef` id => box. Accessed under a
# `NonReentrantLock` via spin-locking so it is safe to touch from the `DRef`
# destructor, which may run in a GC/finalizer context where task switches (and
# hence a blocking `lock`) are illegal.
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
"""
struct DEvent
    ref::DRef
end
function DEvent()
    box = DEventBox()
    idbox = Ref{Int}(0)
    ref = poolset(DEVENT_SENTINEL; destructor = () -> _devent_delete!(idbox[]))
    idbox[] = ref.id
    _devent_register!(ref.id, box)
    return DEvent(ref)
end
function DEvent(pid::Integer)
    pid == myid() && return DEvent()
    return remotecall_fetch(DEvent, pid)
end

owner(de::DEvent) = de.ref.owner

function _devent_notify_local(id::Int)
    box = _devent_box(id)
    box === nothing && return
    @atomic box.set = true
    notify(box.event)
    return
end
function Base.notify(de::DEvent)
    o = owner(de)
    if o == myid()
        _devent_notify_local(de.ref.id)
    else
        remotecall_wait(_devent_notify_local, o, de.ref.id)
    end
    return de
end

function _devent_wait_local(id::Int)
    box = _devent_box(id)
    box === nothing && return # already cleaned up => must have fired
    wait(box.event)
    return
end
function Base.wait(de::DEvent)
    o = owner(de)
    if o == myid()
        _devent_wait_local(de.ref.id)
    else
        remotecall_fetch(_devent_wait_local, o, de.ref.id)
    end
    return de
end

function _devent_isset_local(id::Int)
    box = _devent_box(id)
    box === nothing && return true
    return @atomic box.set
end
function isset(de::DEvent)
    o = owner(de)
    if o == myid()
        return _devent_isset_local(de.ref.id)
    else
        return remotecall_fetch(_devent_isset_local, o, de.ref.id)
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
    _, ok = @atomicreplace box.value nothing => Some{Any}(v)
    ok || return # write-once: ignore double-puts leniently
    @atomic box.set = true
    notify(box.event)
    return
end
function Base.put!(f::DFuture, @nospecialize(v))
    de = f.event
    o = owner(de)
    if o == myid()
        _devent_put_local(de.ref.id, v)
    else
        remotecall_wait(_devent_put_local, o, de.ref.id, v)
    end
    return f
end

function _devent_fetch_local(id::Int)
    box = _devent_box(id)
    box === nothing && error("DFuture value is unavailable (already cleaned up)")
    wait(box.event)
    return something(@atomic box.value)
end
function Base.fetch(f::DFuture)
    c = @atomic f.cache
    c !== nothing && return something(c)
    de = f.event
    o = owner(de)
    v = if o == myid()
        _devent_fetch_local(de.ref.id)
    else
        remotecall_fetch(_devent_fetch_local, o, de.ref.id)
    end
    @atomicreplace f.cache nothing => Some{Any}(v)
    return v
end

Base.wait(f::DFuture) = (wait(f.event); f)
Base.isready(f::DFuture) = ((@atomic f.cache) !== nothing) || isset(f.event)
