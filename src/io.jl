##### Array{T} #####

struct MMSer{T} end # sentinal type used in deserialize to switch to our
                    # custom memory-mapping deserializers

can_mmap(io::IOStream) = true
can_mmap(io::IO) = false

function deserialize{T}(s::AbstractSerializer, ::Type{MMSer{T}})
    mmread(T, s, can_mmap(s.io))
end

mmwrite(io::IO, xs) = mmwrite(SerializationState(io), xs)
mmwrite(io::AbstractSerializer, xs) = serialize(io, xs) # fallback

function mmwrite(io::AbstractSerializer, arr::A) where A<:Union{Array,BitArray}
    T = eltype(A)
    Base.serialize_type(io, MMSer{typeof(arr)})
    if isbits(T)
        serialize(io, size(arr))
        write(io.io, arr)
    elseif T<:Union{} || T<:Nullable{Union{}}
        serialize(io, size(arr))
    else
        serialize(io, arr)
    end
end

function mmread(::Type{A}, io, mmap) where A<:Union{Array,BitArray}
    T = eltype(A)
    if isbits(T)
        sz = deserialize(io)
        if prod(sz) == 0
            return A(sz...)
        end
        if mmap
            data = Mmap.mmap(io.io, A, sz, position(io.io))
            seek(io.io, position(io.io)+sizeof(data)) # move
            return data
        else
            return Base.read!(io.io, A(sz...))
        end
    elseif T<:Union{} || T<:Nullable{Union{}}
        sz = deserialize(io)
        return Array{T}(sz)
    else
        return deserialize(io) # slow!!
    end
end

##### Array{String} #####

function mmwrite(io::AbstractSerializer, xs::Array{String})
    Base.serialize_type(io, MMSer{typeof(xs)})

    lengths = map(x->convert(UInt32, endof(x)), xs)
    buffer = Vector{UInt8}(sum(lengths))
    serialize(io, size(xs))
    # todo: write directly to buffer, but also mmap
    ptr = pointer(buffer)
    for x in xs
        l = endof(x)
        unsafe_copy!(ptr, pointer(x), l)
        ptr += l
    end

    mmwrite(io, buffer)
    mmwrite(io, lengths)
end

function mmread{N}(::Type{Array{String,N}}, io, mmap)
    sz = deserialize(io)
    buf = deserialize(io)
    lengths = deserialize(io)

    @assert length(buf) == sum(lengths)
    @assert prod(sz) == length(lengths)

    ys = Array{String,N}(sz...) # output
    ptr = pointer(buf)
    @inbounds for i = 1:length(ys)
        l = lengths[i]
        ys[i] = unsafe_string(ptr, l)
        ptr += l
    end
    ys
end
