if nprocs() == 1
    addprocs(1)
end

using MemPool
using Base.Test

import MemPool: lru_touch, lru_evictable
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

    MemPool.max_memsize[] = tmpsz
    empty!(MemPool.lru_order)
end

@testset "set-get" begin
    @test poolget(poolset([1,2])) == [1,2]
    @test poolget(poolset([1,2], 2)) == [1,2]
end

@testset "movetodisk" begin
    ref = poolset([1,2], 2)
    fref = movetodisk(ref)
    @test isa(fref, MemPool.FileRef)
    @test poolget(fref) == poolget(ref)
    f = tempname()
    fref2 = savetodisk(ref, f)
    @test fref2.file == f
    @test poolget(fref) == poolget(fref2)
end
