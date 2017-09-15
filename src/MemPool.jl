__precompile__()

module MemPool

import Base: serialize, deserialize
export DRef, FileRef, poolset, poolget, pooldelete, destroyonevict,
       movetodisk, copytodisk, savetodisk, mmwrite, mmread

## Wrapping-unwrapping of payloads:

immutable MMWrap{T} # wrap an object to use custom
                    # memory-mapping/fast serializer
    x::T
end

# Wrapping is an implementation detail unwrap_payload will be called by poolget
# on every fetched value this will give back the value as it was saved.

unwrap_payload(x) = x
unwrap_payload(x::Tuple) = map(unwrap_payload, x)
unwrap_payload(x::MMWrap) = unwrap_payload(x.x)

function serialize(io::AbstractSerializer, w::MMWrap)
    # ticket to mm land
    mmwrite(io, w.x)
end

function deserialize{T}(io::AbstractSerializer, ::Type{MMWrap{T}})
    MMWrap(mmread(T, io)) # gotta keep that wrapper on
end


# Unwrapping FileRef == reading the file.
# This allows communicating only the file name across processors,
# the receiver process will simply read from file while unwrapping
struct FileRef
    file::String
end

unwrap_payload(f::FileRef) = unwrap_payload(open(deserialize, f.file))

"""
`approx_size(d)`

Returns the size of `d` in bytes used for accounting in MemPool datastore.
"""
function approx_size(d)
    Base.summarysize(d) # note: this is accurate but expensive
end

function approx_size{T}(d::Array{T})
    if isbits(T)
        sizeof(d)
    else
        Base.summarysize(d)
    end
end

function approx_size(xs::Array{String})
    # doesn't check for redundant references, but
    # really super fast in comparison to summarysize
    sum(map(sizeof, xs)) + 4 * length(xs)
end

include("io.jl")
include("datastore.jl")

__init__() = global session = "sess-" * randstring(5)

end # module
