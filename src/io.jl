##### Array{T} #####
using Mmap

struct MMSer{T} end # sentinal type used in deserialize to switch to our
                    # custom memory-mapping deserializers

can_mmap(io::IOStream) = true
can_mmap(io::IO) = false

function padalign(io::IOStream, align=8)
    align = (align + 7) & -8  # make sure alignment is a multiple of 8
    p = position(io)
    if p & (align-1) != 0
        write(io, zeros(UInt8, (align - (p % align))))
    end
end
padalign(io, align=8) = nothing

function seekpadded(io::IOStream, align=8)
    align = (align + 7) & -8
    p = position(io)
    if p & (align-1) != 0
        seek(io, p + (align - (p % align)))
    end
end
seekpadded(io, align=8) = nothing

function deserialize(s::AbstractSerializer, ::Type{MMSer{T}}) where T
    mmread(T, s, can_mmap(s.io))
end

mmwrite(io::IO, xs) = mmwrite(Serializer(io), xs)
mmwrite(io::AbstractSerializer, xs) = serialize(io, xs) # fallback

function mmwrite(io::AbstractSerializer, arr::A) where A<:Union{Array,BitArray}
    T = eltype(A)
    Serialization.serialize_type(io, MMSer{typeof(arr)})
    if T<:Union{} || T==Missing
        serialize(io, size(arr))
        return
    elseif isbitstype(T)
        serialize(io, size(arr))
        padalign(io.io, sizeof(eltype(arr)))
        write(io.io, arr)
        return
    end

    fl = fixedlength(T)
    if fl > 0
        serialize(io, size(arr))
        for x in arr
            fast_write(io.io, x)
        end
    else
        serialize(io, arr)
    end
end

function mmread(::Type{A}, io, mmap) where A<:Union{Array,BitArray}
    T = eltype(A)
    if T<:Union{} || T==Missing
        sz = deserialize(io)
        return Array{T}(undef, sz)
    elseif isbitstype(T)
        sz = deserialize(io)
        seekpadded(io.io, sizeof(T))

        if prod(sz) == 0
            return A(undef, sz...)
        end
        if mmap
            data = Mmap.mmap(io.io, A, sz, position(io.io))
            seek(io.io, position(io.io)+sizeof(data)) # move
            return data
        else
            data = Base.read!(io.io, A(undef, sz...))
            return data
        end
    end

    fl = fixedlength(T)
    if fl > 0
        sz = deserialize(io)
        arr = A(undef, sz...)
        @inbounds for i in eachindex(arr)
            arr[i] = fast_read(io.io, T)::T
        end
        return arr
    else
        return deserialize(io) # slow!!
    end
end

##### Array{String} #####

const UNDEF_LENGTH = typemax(UInt32) # if your string is exaclty 4GB you're out of luck

function mmwrite(io::AbstractSerializer, xs::Array{String})
    Serialization.serialize_type(io, MMSer{typeof(xs)})

    lengths = UInt32[]
    buffer = UInt8[]
    serialize(io, size(xs))
    # todo: write directly to buffer, but also mmap
    ptr = pointer(buffer)
    for i in 1:length(xs)
        if isassigned(xs, i)
            x = xs[i]
            l = sizeof(x)
            lb = length(buffer)
            push!(lengths, l)
            resize!(buffer, lb+l)
            unsafe_copyto!(pointer(buffer)+lb, pointer(x), l)
            ptr += l
        else
            push!(lengths, UNDEF_LENGTH)
        end
    end

    mmwrite(io, buffer)
    mmwrite(io, lengths)
end

function mmread(::Type{Array{String,N}}, io, mmap) where N
    sz = deserialize(io)
    buf = deserialize(io)
    lengths = deserialize(io)

   #@assert length(buf) == sum(filter(x->x>0, lengths))
   #@assert prod(sz) == length(lengths)

    ys = Array{String,N}(undef, (sz...,)) # output
    ptr = pointer(buf)
    @inbounds for i = 1:length(ys)
        l = lengths[i]
        l == UNDEF_LENGTH && continue
        ys[i] = unsafe_string(ptr, l)
        ptr += l
    end
    ys
end


## Optimized fixed length IO
## E.g. this is very good for `StaticArrays.MVector`s

function fixedlength(t::Type, cycles=IdDict())
    if isbitstype(t)
        return sizeof(t)
    elseif isa(t, UnionAll) || isabstracttype(t) || Base.isbitsunion(t)
        return -1
    end

    if haskey(cycles, t)
        return -1
    end
    cycles[t] = nothing
    lens = ntuple(i->fixedlength(fieldtype(t, i), copy(cycles)), fieldcount(t))
    if isempty(lens)
        # e.g. abstract type / array type
        return -1
    elseif any(x -> x < 0, lens)
        return -1
    else
        return sum(lens)
    end
end

fixedlength(::Type{>:Missing}, cycles=nothing) = -1
fixedlength(::Type{<:String}, cycles=nothing) = -1
fixedlength(::Type{Union{}}, cycles=nothing) = -1
fixedlength(::Type{<:Ptr}, cycles=nothing) = -1

function gen_writer(::Type{T}, expr) where T
    @assert fixedlength(T) >= 0 "gen_writer must be called for fixed length eltypes"
    if T<:Tuple && isbitstype(T)
        :(write(io, Ref{$T}($expr)))
    elseif length(T.types) > 0
        :(begin
              $([gen_writer(fieldtype(T, i), :(getfield($expr, $i))) for i=1:fieldcount(T)]...)
          end)
    elseif isbitstype(T) && sizeof(T) == 0
        return :(begin end)
    elseif isbitstype(T)
        return :(write(io, $expr))
    else
        error("Don't know how to serialize $T")
    end
end

function gen_reader(::Type{T}) where T
    @assert fixedlength(T) >= 0 "gen_reader must be called for fixed length eltypes"
    ex = if T<:Tuple
        if isbitstype(T)
            return :(read!(io, Ref{$T}())[])
        else
            exprs = [gen_reader(fieldtype(T, i)) for i=1:fieldcount(T)]
            return :(tuple($(exprs...)))
        end
    elseif length(T.types) > 0
        return :(ccall(:jl_new_struct, Any, (Any,Any...), $T, $([gen_reader(fieldtype(T, i)) for i=1:fieldcount(T)]...)))
    elseif isbitstype(T) && sizeof(T) == 0
        return :(ccall(:jl_new_struct, Any, (Any,Any...), $T))
    elseif isbitstype(T)
        return :($T; read(io, $T))
    else
        error("Don't know how to deserialize $T")
    end
    return :($T; $ex)
end

@generated function fast_write(io, x)
    gen_writer(x, :x)
end

@generated function fast_read(io, ::Type{T}) where T
    gen_reader(T)
end
