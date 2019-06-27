include("testenv.jl")

if nprocs() == 1
    addprocs_with_testenv(2)
end

using Serialization, Random

@everywhere using Sockets

using MemPool
using Test

import Sockets: getipaddr

#import MemPool: lru_touch, lru_evictable, mmwrite, mmread

function roundtrip(x, eq=(==), io=IOBuffer())
    mmwrite(Serializer(io), x)
    y = deserialize(seekstart(io))
    try
        @assert eq(y, x)
        @test eq(y, x)
    catch err
        println("Expected:     ", x)
        println("Deserialized: ", y)
        rethrow(err)
    end
end

primitive type TestInt160 160 end

@testset "Array" begin
    roundtrip(rand(10))
    roundtrip(BitArray(rand(Bool,10)))
    roundtrip(map(Ref, rand(10)), (x,y)->getindex.(x) == getindex.(y))
    mktemp() do path, f
        roundtrip(Vector{TestInt160}(undef, 10), (x,y)->true, f)
    end

    io = IOBuffer()
    x = Array{Union{}}(undef, 10)
    mmwrite(Serializer(io), x)
    y = deserialize(seekstart(io))
    @test typeof(y) == Array{Union{},1}
    @test length(y) == 10
    @test MemPool.fixedlength(Tuple{String, Int}) == -1
end

using StaticArrays
@testset "StaticArrays" begin
    x = [@MVector(rand(75)) for i=1:100]
    io = IOBuffer()
    mmwrite(Serializer(io), x)
    alloc = @allocated mmwrite(Serializer(seekstart(io)), x)

    @test deserialize(seekstart(io)) == x
    @test MemPool.approx_size(x) == 75*100*8
end

@testset "Array{String}" begin
    roundtrip([randstring(rand(1:10)) for i=1:4])
    sa = Array{String}(undef, 2)
    sa[1] = "foo"
    io = IOBuffer()
    mmwrite(Serializer(io), sa)
    sa2 = deserialize(seekstart(io))
    @test sa2[1] == "foo"
    @test !isassigned(sa2, 2)
end

mutable struct Empty
end
import Base: ==
==(a::Empty, b::Empty) = true

@testset "Array{Empty}" begin
    roundtrip([Empty() for i=1:4])
    roundtrip([(Empty(),) for i=1:4])
end

#=
@testset "lru" begin
    # clean up
    tmpsz = MemPool.max_memsize[]
    MemPool.max_memsize[] = 20
    empty!(MemPool.lru_order)

    @test isempty(lru_evictable(20))
    lru_touch(1, 10)
    @test isempty(lru_evictable(10))
    @test lru_evictable(11) == [1]
    lru_touch(2, 10)
    @test isempty(lru_evictable(0))
    @test lru_evictable(1) == [1]
    @test lru_evictable(10) == [1]
    @test lru_evictable(11) == [1,2]
    lru_touch(1, 10)
    @test lru_evictable(1) == [2]
    @test lru_evictable(10) == [2]
    @test lru_evictable(11) == [2,1]

    MemPool.max_memsize[] = tmpsz
    empty!(MemPool.lru_order)
end
=#

@testset "set-get-delete" begin
    r1 = poolset([1,2])
    r2 = poolset(["abc","def"], 2)
    r3 = poolset([Ref(1),Ref(2)], 2)
    @test poolget(r1) == [1,2]
    @test poolget(r2) == ["abc","def"]
    @test map(getindex, poolget(r3)) == [1,2]
    pooldelete(r1)
    @test_throws KeyError poolget(r1)
    pooldelete(r2)
    pooldelete(r3)
    @test_throws KeyError poolget(r2)
    #@test isempty(MemPool.lru_order)
    #@test fetch(@spawnat 2 isempty(MemPool.lru_order))
    @test isempty(MemPool.datastore)
    #@test fetch(@spawnat 2 isempty(MemPool.datastore))
end

@testset "movetodisk" begin
    ref = poolset([1,2], 2)
    fref = movetodisk(ref)
    @test isa(fref, MemPool.FileRef)
    @test fref.host == getipaddr()
    @test poolget(fref) == poolget(ref)
    f = tempname()
    fref2 = savetodisk(ref, f)
    @test fref2.file == f
    @test poolget(fref) == poolget(fref2)
    pooldelete(ref)
    #@test fetch(@spawnat 2 isempty(MemPool.lru_order))
    @test fetch(@spawnat 2 isempty(MemPool.datastore))

    ref = poolset([1,2], 2)
    fref = movetodisk(ref)
    @test remotecall_fetch(poolget, 2, fref) == poolget(ref)
    @everywhere (using MemPool; MemPool.cleanup())

    @test MemPool.get_worker_at(getipaddr()) in [1,2,3]
    @test remotecall_fetch(()->MemPool.get_worker_at(getipaddr()), 2) in [1,2,3]
    @everywhere MemPool.enable_random_fref_serve[] = false
    @everywhere empty!(MemPool.wrkrips)
    @test MemPool.is_my_ip(getipaddr())
end

inmem(ref, pid=myid()) = remotecall_fetch(id -> MemPool.isinmemory(MemPool.datastore[id]), ref.owner, ref.id)
#=
@testset "lru free" begin
    @everywhere MemPool.max_memsize[] = 8*10
    @everywhere MemPool.spilltodisk[] = true
    r1 = poolset([1,2], 2)
    r2 = poolset([1,2,3], 2)
    r3 = poolset([1,2,3,4,5], 2)

    @test inmem(r1)
    @test inmem(r2)
    @test inmem(r3)

    r4 = poolset([1], 2)
    @test !inmem(r1)
    @test inmem(r2)
    @test inmem(r3)
    poolget(r2) # make this less least recently used

    destroyonevict(r3)
    r5 = poolset([1,2],2)
    @test_throws KeyError poolget(r3)
    @test inmem(r2)

    r6 = poolset([1,2],2)
    @test inmem(r2)
    @test inmem(r4)

    map(poolget, [r1,r2,r4,r5,r6]) == [[1:2;], [1:3;], [1], [1:2;], [1:2;]]
    map(pooldelete, [r1,r2,r4,r5,r6])
    @everywhere MemPool.max_memsize[] = 2e9
end
=#

@testset "who_has_read" begin
    f = tempname()
    ref = poolset([1:5;])
    fref = savetodisk(ref, f)
    r2 = remotecall_fetch(()->MemPool.poolget(fref), 2)
    @test MemPool.who_has_read[f][1].owner == 2
end

@testset "cleanup" begin
    @everywhere MemPool.cleanup()
    d = MemPool.default_dir(myid())
    @test !isdir(d)
end
