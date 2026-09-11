# MemPool logging shim
#
# Emits TimespanLogging events into the same sink Dagger configures in
# `enable_logging!`. MemPool sits below Dagger and must not depend on it:
# a process-local context carries only the sink. When the sink is `NoOpLog`
# (the default), `@logstart`/`@logfinish` skip id construction.

import TimespanLogging
import TimespanLogging: NoOpLog, @logstart, @logfinish, @logcategory

const LOG_SINK = Ref{Any}(NoOpLog())
const LOG_FINE = Ref{Bool}(false)

struct MemPoolLogContext end
const MPCTX = MemPoolLogContext()

TimespanLogging.log_sink(::MemPoolLogContext) = LOG_SINK[]
TimespanLogging.profile(::MemPoolLogContext, category, id, tl) = false

function set_log_sink!(sink)
    old = LOG_SINK[]
    LOG_SINK[] = sink
    return old
end

function set_log_fine!(fine::Bool)
    LOG_FINE[] = fine
    return fine
end

logging_enabled() = !(LOG_SINK[] isa NoOpLog)

const _LOG_NONCE = Threads.Atomic{UInt64}(0)
@inline next_log_id() = Threads.atomic_add!(_LOG_NONCE, UInt64(1))

@logcategory LogPoolGet as=:mempool_poolget id=(ref::Int, owner::Int, u::UInt64) data=Nothing
@logcategory LogPoolSet as=:mempool_poolset id=(size::UInt64, u::UInt64) data=Nothing
@logcategory LogSRAWrite as=:mempool_sra_write id=(ref::Int, size::UInt64, u::UInt64) data=Nothing
@logcategory LogSRARead as=:mempool_sra_read id=(ref::Int, size::UInt64, u::UInt64) data=Nothing
@logcategory LogStorageRcu as=:mempool_storage_rcu id=(u::UInt64,) data=Nothing
@logcategory LogMemReserveGC as=:mempool_mem_reserve_gc id=(reserve::UInt64, u::UInt64) data=Nothing

"""
    @mplog Category id expr

Record start/finish around `expr` when MemPool's sink is not `NoOpLog`.
`id` is only evaluated when logging is on.
"""
macro mplog(cat, id, expr)
    quote
        if $(TimespanLogging).logging_enabled(MPCTX, $(esc(cat)))
            local _id = $(esc(id))
            $(TimespanLogging)._emit($(esc(cat)), 0x00, _id, nothing)
            try
                $(esc(expr))
            finally
                $(TimespanLogging)._emit($(esc(cat)), 0x01, _id, nothing)
            end
        else
            $(esc(expr))
        end
    end
end
