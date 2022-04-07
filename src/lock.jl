# Copied from CUDA.jl/src/pool.jl

"""
    NonReentrantLock

Simple non-reentrant lock that errors when trying to reenter on the same task.
"""
struct NonReentrantLock # <: Base.AbstractLock
    rl::ReentrantLock
    NonReentrantLock() = new(ReentrantLock())
end

function Base.lock(nrl::NonReentrantLock)
    @assert !islocked(nrl.rl) || nrl.rl.locked_by !== current_task()
    lock(nrl.rl)
end

function Base.trylock(nrl::NonReentrantLock)
    @assert !islocked(nrl.rl) || nrl.rl.locked_by !== current_task()
    trylock(nrl.rl)
end

Base.unlock(nrl::NonReentrantLock) = unlock(nrl.rl)

if VERSION >= v"1.7.0-DEV"
# as of v1.7 locks in Base disable finalizers
enable_finalizers(on::Bool) = nothing
else
# NonReentrantLock may be taken around code that might call the GC, which might
# reenter through finalizers.  Avoid that by temporarily disabling finalizers
# running concurrently on this thread.
enable_finalizers(on::Bool) = ccall(:jl_gc_enable_finalizers, Cvoid,
                                    (Ptr{Cvoid}, Int32,), Core.getptls(), on)
end

macro safe_lock(l, ex)
    quote
        temp = $(esc(l))
        lock(temp)
        enable_finalizers(false)
        try
            $(esc(ex))
        finally
            unlock(temp)
            enable_finalizers(true)
        end
    end
end

# If we actually want to acquire a lock from a finalizer, we can't cause a task
# switch. As a NonReentrantLock can only be taken by another thread that should
# be running, and not a concurrent task we'd need to switch to, we can safely
# spin.
macro safe_lock_spin(l, ex)
    quote
        temp = $(esc(l))
        while !trylock(temp)
            # we can't yield here
            GC.safepoint()
        end
        enable_finalizers(false) # retains compatibility with non-finalizer callers
        try
            $(esc(ex))
        finally
            unlock(temp)
            enable_finalizers(true)
        end
    end
end

"""
    with_lock(f, lock, cond=true)

Conditionally take lock `lock`, execute `f`, and unlock `lock`. If `!cond`,
then `lock` is not taken or released.
"""
function with_lock(f, lock, cond=true)
    if cond
        @safe_lock lock begin
            f()
        end
    else
        f()
    end
end
