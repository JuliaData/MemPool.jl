
function approx_size(d)
    Base.summarysize(d) # note: this is accurate but expensive
end

function approx_size{T}(d::Array{T})
    if isbits(eltype(T))
        sizeof(d)
    else
        Base.summarysize(d)
    end
end

function approx_size(xs::Array{String})
    # doesn't check for redundant references, but
    # really super fast in comparison to summarysize
    sum(map(sizeof, xs))
end
