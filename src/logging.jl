# MemPool logging shim
#
# MemPool emits TimespanLogging events (SimpleRecencyAllocator migrations, raw
# device IO, poolget/poolset, storage RCU updates, GC-on-reserve, ...) into the
# *same* `MultiEventLog` that Dagger configures in `Dagger.enable_logging!`. This
# lets a single `fetch_logs!` call return a unified Dagger+MemPool timeline, so
# out-of-core bottlenecks (e.g. time spent blocked on disk writes vs. compute)
# are directly visible.
#
# We deliberately do not create a Dagger `Context`: MemPool sits *below* Dagger
# in the dependency graph and must not depend on it. Instead we route through a
# tiny process-local context whose `log_sink` is a settable `Ref`. Dagger sets it
# to its `MultiEventLog` when logging is enabled and back to `NoOpLog` when
# disabled. When the sink is a `NoOpLog` (the default), `timespan_start`/`finish`
# short-circuit on their first line, so logging-disabled overhead is a single
# pointer load + type check.

import TimespanLogging
import TimespanLogging: NoOpLog, timespan_start, timespan_finish

# The shared log sink. Defaults to a no-op; Dagger points this at its
# `MultiEventLog` (see `Dagger.enable_logging!`). Atomic so cross-thread
# enable/disable is visible without locking on the hot path.
const LOG_SINK = Ref{Any}(NoOpLog())

# Fine-grained, very high-frequency events (e.g. every `storage_rcu!` CAS) are
# gated behind this flag so they don't distort measurements or balloon the log
# unless explicitly requested. Dagger's `enable_logging!(mempool_fine=true)`
# flips it on.
const LOG_FINE = Ref{Bool}(false)

"A process-local logging context for MemPool events (carries only the sink)."
struct MemPoolLogContext end
const MPCTX = MemPoolLogContext()

TimespanLogging.log_sink(::MemPoolLogContext) = LOG_SINK[]
# Never profile MemPool events (profiling is opt-in for Dagger `:compute` only).
TimespanLogging.profile(::MemPoolLogContext, category, id, tl) = false

"""
    set_log_sink!(sink)

Point MemPool's event logging at `sink` (a TimespanLogging sink, typically the
`MultiEventLog` shared with Dagger). Pass a `NoOpLog` to disable. Returns the
previous sink.
"""
function set_log_sink!(sink)
    old = LOG_SINK[]
    LOG_SINK[] = sink
    return old
end

logging_enabled() = !(LOG_SINK[] isa NoOpLog)

# Monotonic nonce so that concurrent events sharing a category+ref still get a
# unique `id` (start/finish pairing in TimespanLogging keys on `(category, id)`).
const _LOG_NONCE = Threads.Atomic{UInt64}(0)
@inline next_log_id() = Threads.atomic_add!(_LOG_NONCE, UInt64(1))

# Thin wrappers so call sites read `MemPool.timespan_start(:cat, id, tl)`. These
# forward to TimespanLogging with MemPool's context; the first line of
# `timespan_start`/`finish` is a `NoOpLog` check, so the disabled path is cheap.
@inline mp_timespan_start(category::Symbol, @nospecialize(id), @nospecialize(tl)) =
    timespan_start(MPCTX, category, id, tl)
@inline mp_timespan_finish(category::Symbol, @nospecialize(id), @nospecialize(tl)) =
    timespan_finish(MPCTX, category, id, tl)

"""
    @mplog category id timeline expr

Wrap `expr` in a `timespan_start`/`timespan_finish` pair for MemPool event
`category` with identity `id` and payload `timeline`, returning `expr`'s value.
The start/finish calls themselves short-circuit when logging is disabled; the
surrounding `logging_enabled()` guard additionally avoids constructing `id`/`tl`
(which may allocate NamedTuples) on the hot path when logging is off.
"""
macro mplog(category, id, timeline, expr)
    quote
        if logging_enabled()
            local _cat = $(esc(category))
            local _id = $(esc(id))
            local _tl = $(esc(timeline))
            mp_timespan_start(_cat, _id, _tl)
            try
                $(esc(expr))
            finally
                mp_timespan_finish(_cat, _id, _tl)
            end
        else
            $(esc(expr))
        end
    end
end
