include("testenv.jl")

if nprocs() == 1
    addprocs_with_testenv(2)
end

@everywhere ENV["JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR"] = "0"

using Serialization, Random

@everywhere using Sockets

@everywhere using MemPool
import MemPool: CPURAMDevice, SerializationFileDevice, SimpleRecencyAllocator
import MemPool: storage_read
using Test

import Sockets: getipaddr

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

@testset "approx_size" begin
    @testset "approx_size $(typeof(x))" for x in (
        "foo~^å&",
        SubString("aaaaa", 2),
    )
        @test MemPool.approx_size(x) == Base.summarysize(x)
    end
    @testset "approx_size Symbol" begin
        s = Symbol("foo~^å&")
        @test MemPool.approx_size(s) == Base.summarysize(String(s))
    end
end


mutable struct Empty
end
import Base: ==
==(a::Empty, b::Empty) = true

@testset "Array{Empty}" begin
    roundtrip([Empty() for i=1:4])
    roundtrip([(Empty(),) for i=1:4])
end

@testset "Array{Union{Nothing,Vector}}" begin
    roundtrip([nothing, Int[]])
    @test MemPool.fixedlength(Union{Nothing,Vector{Int}}) == -1 # Issue #43.
end

@testset "DRef equality" begin
    d1 = poolset(1)
    d2 = copy(d1)
    @test d1 == d2
    @test hash(d1) == hash(d2)

    d = Dict{DRef, Int}()
    d[d1] = 1
    @test haskey(d, d1)
    @test haskey(d, d2)
    @test d[d2] == 1
end

@testset "Set-Get" begin
    r1 = poolset([1,2])
    r2 = poolset(["abc","def"], 2)
    r3 = poolset([Ref(1),Ref(2)], 2)
    @test poolget(r1) == [1,2]
    @test poolget(r2) == ["abc","def"]
    @test map(getindex, poolget(r3)) == [1,2]
end

@testset "Distributed reference counting" begin
    @testset "Owned locally" begin
        # Owned by us
        r1 = poolset([1,2])
        key1 = (r1.owner, r1.id)
        id1 = r1.id

        # We know about this DRef
        @test haskey(MemPool.datastore_counters, key1)
        # We own it, and are told when others receive it, but it hasn't been passed around yet
        @test MemPool.datastore_counters[key1].worker_counter[] == 1
        @test length(MemPool.datastore_counters[key1].recv_counters) == 0
        # We hold a local reference to it
        @test MemPool.datastore_counters[key1].local_counter[] == 1
        # We haven't sent it to anyone
        @test length(MemPool.datastore_counters[key1].send_counters) == 0

        # They don't know about it
        @test fetch(@spawnat 2 !haskey(MemPool.datastore_counters, key1))

        # Send them a copy
        @everywhere [2] begin
            # They know about this DRef
            @assert haskey(MemPool.datastore_counters, $key1)
            # They don't own it, and aren't told when others receive it
            @assert MemPool.datastore_counters[$key1].worker_counter[] == 0
            @assert length(MemPool.datastore_counters[$key1].recv_counters) == 0
            # They hold a local reference to it
            @assert MemPool.datastore_counters[$key1].local_counter[] == 1
            # They haven't sent it to anyone
            @assert length(MemPool.datastore_counters[$key1].send_counters) == 0
            # Here to ensure r1 is serialized and retained
            const r1_ref = Ref{Any}($r1)
        end

        # We've sent it to them and are tracking that
        @test MemPool.datastore_counters[key1].worker_counter[] == 2

        # Delete their copy
        @everywhere [2] begin
            r1_ref[] = nothing
            GC.gc(); sleep(0.5)
        end
        GC.gc(); sleep(0.5)

        # They don't know about it (anymore)
        @test fetch(@spawnat 2 !haskey(MemPool.datastore_counters, key1))

        # They're done with it and we're aware of that
        @test MemPool.datastore_counters[key1].worker_counter[] == 1
    end

    @testset "Owned remotely" begin
        # Owned by worker 2 ("them")
        r2 = poolset([1,2], 2)
        key2 = (r2.owner, r2.id)
        id2 = r2.id

        # Give us some time to tell them we received r2
        @everywhere GC.gc()
        sleep(1)

        # We know about this DRef
        @test haskey(MemPool.datastore_counters, key2)
        # We don't own it, and aren't told when others receive it
        @test MemPool.datastore_counters[key2].worker_counter[] == 0
        @test length(MemPool.datastore_counters[key2].recv_counters) == 0
        # We hold a local reference to it
        @test MemPool.datastore_counters[key2].local_counter[] == 1
        # We haven't sent it to anyone (yet)
        @test length(MemPool.datastore_counters[key2].send_counters) == 0

        @everywhere [2] begin
            # They know about this DRef
            @assert haskey(MemPool.datastore_counters, $key2)
            # They own it, and are told when others receive it (and we have received it, but they're already aware of that)
            @assert MemPool.datastore_counters[$key2].worker_counter[] >= 1
            @assert length(MemPool.datastore_counters[$key2].recv_counters) == 0
            # They don't hold a local reference to it
            @assert MemPool.datastore_counters[$key2].local_counter[] == 0
            # They haven't sent it to anyone (recently)
            @assert length(MemPool.datastore_counters[$key2].send_counters) == 0
        end

        # Send them a copy
        @everywhere [2] begin
            const r2_ref = Ref{Any}($r2)
            # They hold a local reference to it
            @assert MemPool.datastore_counters[$key2].local_counter[] == 1
            # They know about our and their copies
            @assert MemPool.datastore_counters[$key2].worker_counter[] == 2
        end

        # Delete our copy
        r2 = nothing
        @everywhere GC.gc()
        sleep(0.5)
        # We don't know about this DRef (anymore)
        @test_broken !haskey(MemPool.datastore_counters, key2)

        @test_skip "They only see their copy"
        #=
        @everywhere [2] begin
            # They only see their copy
            @assert MemPool.datastore_counters[$key2].worker_counter[] == 1
        end
        =#

        # Delete their copy
        @everywhere [2] begin
            r2_ref[] = nothing
            GC.gc(); sleep(0.5)
        end

        @test_skip "They don't know about this DRef (anymore)"
        #=
        @everywhere [2] begin
            # They don't know about this DRef (anymore)
            @assert !haskey(MemPool.datastore_counters, $key2)
        end
        =#
    end
end

@testset "Destructors" begin
    ref_del = Ref(false)
    # @eval because testsets retain values in weird ways
    x = @eval Ref{Any}(poolset(123; destructor=()->(@assert !$ref_del[]; $ref_del[]=true;)))
    @test !ref_del[]
    x[] = nothing
    GC.gc(); yield()
    @test ref_del[]
end

@testset "StorageState" begin
    sstate1 = MemPool.StorageState(nothing,
                                   MemPool.StorageLeaf[],
                                   CPURAMDevice())
    @test sstate1 isa MemPool.StorageState
    @test sstate1.ready isa Base.Event
    @test !sstate1.ready.set
    notify(sstate1)
    @test sstate1.ready.set
    @test sstate1.root isa CPURAMDevice
    @test length(sstate1.leaves) == 0
    @test sstate1.data === nothing
    @test sstate1 === sstate1

    sstate2 = MemPool.StorageState(sstate1; data=Some{Any}(123))
    @test sstate2 !== sstate1
    @test sstate2.ready !== sstate1.ready
    @test !sstate2.ready.set
    notify(sstate2)
    @test sstate2.root isa CPURAMDevice
    @test length(sstate2.leaves) == 0
    @test sstate2.data isa Some{Any}
    @test something(sstate2.data) == 123

    x1 = poolset([1,2,3])
    @test MemPool.isinmemory(x1)
    state = MemPool.datastore[x1.id]
    sstate = MemPool.storage_read(state)
    @test sstate === MemPool.storage_read(state)
    wait(sstate)
    @test sstate.ready.set
    @test length(sstate.leaves) == 0
    @test sstate.root isa CPURAMDevice
    @test sstate.data isa Some{Any}
    @test something(sstate.data) == [1,2,3]

    sdevice = MemPool.SerializationFileDevice(joinpath(MemPool.default_dir(), randstring(6)))
    MemPool.set_device!(sdevice, x1)
    @test MemPool.isondisk(x1)
    @test MemPool.datastore[x1.id] === state
    new_sstate = MemPool.storage_read(state)
    @test sstate !== new_sstate
    wait(new_sstate)
    @test new_sstate.ready.set
    @test new_sstate.root === sdevice
    @test new_sstate.data isa Some{Any}
    @test length(new_sstate.leaves) == 1
    leaf = first(new_sstate.leaves)
    @test leaf.device === sdevice
    @test leaf.handle isa Some{Any}
    @test something(leaf.handle) isa FileRef
end

@testset "RefState" begin
    sstate1 = MemPool.StorageState(Some{Any}(123),
                                   MemPool.StorageLeaf[],
                                   CPURAMDevice(),
                                   Base.Event())
    notify(sstate1)
    state = MemPool.RefState(sstate1, 64, "abc", MemPool.Tag(SerializationFileDevice=>123), nothing)
    @test state.size == 64
    @test MemPool.storage_size(state) == 64
    @test_throws ArgumentError state.storage
    @test_throws ArgumentError state.tag
    @test_throws ArgumentError state.leaf_tag
    @test MemPool.tag_read(state, sstate1, CPURAMDevice()) == "abc"
    @test MemPool.tag_read(state, sstate1, SerializationFileDevice()) == 123
    sstate2 = MemPool.storage_read(state)
    @test sstate2 isa MemPool.StorageState
    @test sstate2 === sstate1

    @test_throws ArgumentError (state.storage = sstate1)
    sstate3 = MemPool.storage_rcu!(state) do old_sstate
        @test old_sstate === sstate1
        # N.B. Not semantically correct to have CPURAMDevice as leaf
        leaf = MemPool.StorageLeaf(CPURAMDevice(), Some{Any}(456))
        MemPool.StorageState(old_sstate; leaves=[leaf])
    end
    @test !sstate3.ready.set
    notify(sstate3)
    @test sstate3 !== sstate1
    @test sstate3.data isa Some{Any}
    @test something(sstate3.data) == 123
    @test length(sstate3.leaves) == 1
    leaf = first(sstate3.leaves)
    @test leaf.device isa CPURAMDevice
    @test leaf.handle isa Some{Any}
    @test something(leaf.handle) == 456

    @test_throws ConcurrencyViolationError MemPool.storage_rcu!(old_sstate->old_sstate, state)
end

@testset "Tag" begin
    tag = MemPool.Tag()
    @test tag[SerializationFileDevice] == nothing

    tag = MemPool.Tag(SerializationFileDevice=>123)
    @test tag[SerializationFileDevice] == 123
    @test tag[CPURAMDevice] === nothing

    tag = MemPool.Tag(SerializationFileDevice=>123, CPURAMDevice=>456)
    @test tag[SerializationFileDevice] == 123
    @test tag[CPURAMDevice] == 456
    @test tag[SimpleRecencyAllocator] === nothing
end

@testset "CPURAMDevice" begin
    # Delete -> read throws
    x = poolset(123)
    MemPool.delete_from_device!(CPURAMDevice(), x)
    @test_throws Exception poolget(x)
end

@testset "SerializationFileDevice" begin
    x1 = poolset([1,2,3])
    state = MemPool.datastore[x1.id]
    data = something(storage_read(state).data)

    dirpath = mktempdir()
    sdevice = MemPool.SerializationFileDevice(dirpath)
    MemPool.set_device!(sdevice, x1.id)
    sstate = MemPool.storage_read(state)
    leaf = first(sstate.leaves)
    @test sstate.root === leaf.device === sdevice
    @test leaf.handle !== nothing
    @test something(leaf.handle) isa FileRef
    path = something(leaf.handle).file
    @test isdir(dirpath)
    @test isfile(path)
    @test normpath(joinpath(dirpath, basename(path))) == normpath(path)
    @test poolget(x1) == data
    # Retains the FileRef after read to memory
    @test first(storage_read(state).leaves).handle !== nothing

    MemPool.delete_from_device!(CPURAMDevice(), state, x1.id)
    @test storage_read(state).data === nothing
    @test poolget(x1) == data
    @test storage_read(state).data !== nothing
    MemPool.delete_from_device!(sdevice, state, x1)
    @test length(storage_read(state).leaves) == 0

    # With retention, data is retained
    MemPool.set_device!(sdevice, state, x1)
    MemPool.retain_on_device!(sdevice, state, x1, true)
    sstate = storage_read(state)
    @test only(sstate.leaves).retain
    path = something(only(sstate.leaves).handle).file
    MemPool.delete_from_device!(sdevice, state, x1)
    @test length(storage_read(state).leaves) == 0
    GC.gc(); sleep(1)
    # File is retained
    @test isfile(path)
    # Retention cannot be changed after deletion
    @test_throws ArgumentError MemPool.retain_on_device!(sdevice, state, x1, false)
    # Memory is retained
    @test storage_read(state).data !== nothing
    @test poolget(x1) == data

    # Without retention, data is lost
    MemPool.set_device!(sdevice, state, x1)
    MemPool.retain_on_device!(sdevice, state, x1, false)
    sstate = storage_read(state)
    @test !only(sstate.leaves).retain
    path = something(only(sstate.leaves).handle).file
    MemPool.delete_from_device!(sdevice, state, x1)
    @test length(storage_read(state).leaves) == 0
    GC.gc(); sleep(1)
    # File is not retained
    @test !isfile(path)
    # Retention cannot be changed after deletion
    @test_throws ArgumentError MemPool.retain_on_device!(sdevice, state, x1, true)
    # Memory is retained
    @test storage_read(state).data !== nothing
    @test poolget(x1) == data

    @testset "Serialization Filters" begin
        struct BitOpSerializer{O,I} <: IO
            io::IO
            value::UInt8
            out_op::O
            in_op::I
        end
        BitOpSerializer(io::IO, value::UInt8, op) = BitOpSerializer(io, value, op, op)
        Base.write(io::BitOpSerializer, x::UInt8) = write(io.io, io.out_op(x, io.value))
        Base.read(io::BitOpSerializer, ::Type{UInt8}) = io.in_op(read(io.io, UInt8), io.value)
        Base.close(io::BitOpSerializer) = close(io.io)
        Base.eof(io::BitOpSerializer) = eof(io.io)

        # Symmetric filtering
        sdevice2 = SerializationFileDevice()
        push!(sdevice2.filters, (io->BitOpSerializer(io, 0x42, ⊻))=>(io->BitOpSerializer(io, 0x42, ⊻)))
        x1 = poolset(UInt8(123); device=sdevice2)
        MemPool.delete_from_device!(CPURAMDevice(), x1)
        path = something(only(storage_read(MemPool.datastore[x1.id]).leaves).handle).file
        # Filter is applied on-disk
        iob = IOBuffer(); serialize(iob, UInt8(123)); seek(iob, 0)
        @test read(path, UInt8) == first(take!(iob)) ⊻ 0x42
        # Filter is undone on read
        @test poolget(x1) == UInt8(123)

        # Asymmetric filtering
        sdevice2 = SerializationFileDevice()
        push!(sdevice2.filters, (io->BitOpSerializer(io, 0x42, +, -))=>(io->BitOpSerializer(io, 0x42, +, -)))
        x1 = poolset(UInt8(123); device=sdevice2)
        MemPool.delete_from_device!(CPURAMDevice(), x1)
        path = something(only(storage_read(MemPool.datastore[x1.id]).leaves).handle).file
        # Filter is applied on-disk before serialization
        iob = IOBuffer(); serialize(iob, UInt8(123)); seek(iob, 0)
        @test read(path, UInt8) == first(take!(iob)) + 0x42
        # Filter is undone on read
        @test poolget(x1) == UInt8(123)

        # Chained filtering
        sdevice2 = SerializationFileDevice()
        push!(sdevice2.filters, (io->BitOpSerializer(io, 0x3, +, -))=>(io->BitOpSerializer(io, 0x3, +, -)))
        push!(sdevice2.filters, (io->BitOpSerializer(io, 0x5, ⊻))=>(io->BitOpSerializer(io, 0x5, ⊻)))
        x1 = poolset(UInt8(123); device=sdevice2)
        MemPool.delete_from_device!(CPURAMDevice(), x1)
        path = something(only(storage_read(MemPool.datastore[x1.id]).leaves).handle).file
        # Filter is applied on-disk before serialization
        iob = IOBuffer(); serialize(iob, UInt8(123)); seek(iob, 0)
        value = first(take!(iob))
        @test read(path, UInt8) == (value + 0x3) ⊻ 0x5
        @test read(path, UInt8) != (value ⊻ 0x5) + 0x3
        # Filter is undone on read
        @test poolget(x1) == UInt8(123)
    end

    @testset "Custom File Name" begin
        tag = "myfile.bin"
        ref = poolset(123; device=sdevice, tag)
        @test isfile(joinpath(dirpath, tag))
        ref = nothing; GC.gc(); sleep(0.5)
        @test !isfile(joinpath(dirpath, tag))
    end
end

sra_upper_pos(sra, ref) = findfirst(x->x==ref.id, sra.mem_refs)
sra_lower_pos(sra, ref) = findfirst(x->x==ref.id, sra.device_refs)
sra_inmem_pos(sra, ref, idx) =
    MemPool.isinmemory(ref) &&
    !MemPool.isondisk(ref) &&
    sra_upper_pos(sra, ref) == idx &&
    sra_lower_pos(sra, ref) === nothing
sra_ondisk_pos(sra, ref, idx) =
    !MemPool.isinmemory(ref) &&
    MemPool.isondisk(ref) &&
    sra_upper_pos(sra, ref) === nothing &&
    sra_lower_pos(sra, ref) == idx
@testset "SimpleRecencyAllocator" begin
    sdevice = SerializationFileDevice()

    # Garbage policy throws on creation
    @test_throws ArgumentError SimpleRecencyAllocator(1, sdevice, 1, :blah)

    # Memory and disk limits must be positive and non-zero
    @test_throws ArgumentError SimpleRecencyAllocator(0, sdevice, 1, :LRU)
    @test_throws ArgumentError SimpleRecencyAllocator(1, sdevice, 0, :LRU)
    @test_throws ArgumentError SimpleRecencyAllocator(0, sdevice, -1, :LRU)
    @test_throws ArgumentError SimpleRecencyAllocator(-1, sdevice, 0, :LRU)

    for sra in [MemPool.SimpleRecencyAllocator(8*10, sdevice, 8*10_000, :LRU),
                MemPool.SimpleRecencyAllocator(8*10, sdevice, 8*10_000, :MRU)]
        r1 = poolset([1,2]; device=sra)
        r2 = poolset([1,2,3]; device=sra)
        r3 = poolset([1,2,3,4,5]; device=sra)

        @test sra_inmem_pos(sra, r3, 1)
        @test sra_inmem_pos(sra, r2, 2)
        @test sra_inmem_pos(sra, r1, 3)
        for ref in [r1, r2, r3]
            @test haskey(sra.ref_cache, ref.id)
            @test sra.ref_cache[ref.id] === MemPool.datastore[ref.id]
        end

        # Add a ref that causes an eviction
        r4 = poolset([1,2]; device=sra)
        @test sra_inmem_pos(sra, r4, 1)
        if sra.policy == :LRU
            @test sra_inmem_pos(sra, r3, 2)
            @test sra_inmem_pos(sra, r2, 3)
            @test sra_ondisk_pos(sra, r1, 1)
        else
            @test sra_inmem_pos(sra, r2, 2)
            @test sra_inmem_pos(sra, r1, 3)
            @test sra_ondisk_pos(sra, r3, 1)
        end

        # Make an in-memory ref the most recently used
        @test poolget(r2) == [1,2,3]
        @test sra_inmem_pos(sra, r2, 1)
        if sra.policy == :LRU
            @test sra_inmem_pos(sra, r4, 2)
            @test sra_inmem_pos(sra, r3, 3)
            @test sra_ondisk_pos(sra, r1, 1)
        else
            @test sra_inmem_pos(sra, r4, 2)
            @test sra_inmem_pos(sra, r1, 3)
            @test sra_ondisk_pos(sra, r3, 1)
        end

        # Make an on-disk ref the most recently used
        if sra.policy == :LRU
            @test poolget(r1) == [1,2]
            @test sra_inmem_pos(sra, r1, 1)
            @test sra_inmem_pos(sra, r2, 2)
            @test sra_inmem_pos(sra, r4, 3)
            @test sra_ondisk_pos(sra, r3, 1)
        else
            @test poolget(r3) == [1,2,3,4,5]
            @test sra_inmem_pos(sra, r3, 1)
            @test sra_inmem_pos(sra, r4, 2)
            @test sra_inmem_pos(sra, r1, 3)
            @test sra_ondisk_pos(sra, r2, 1)
        end

        # Delete a ref that was in memory
        prev_mem_refs = copy(sra.mem_refs)
        prev_device_refs = copy(sra.device_refs)
        local del_id
        # FIXME: Somehow refs get retained?
        if sra.policy == :LRU
            del_id = r1.id
            #r1 = nothing
            MemPool.delete_from_device!(sra, r1)
        else
            del_id = r3.id
            #r3 = nothing
            MemPool.delete_from_device!(sra, r3)
        end
        @test_broken !haskey(MemPool.datastore, del_id)
        @test !in(del_id, sra.mem_refs)
        @test !in(del_id, sra.device_refs)
        @test !haskey(sra.ref_cache, del_id)
        @test sra.mem_refs == filter(id->id != del_id, prev_mem_refs)
        @test sra.device_refs == prev_device_refs

        # Delete a ref that was on disk
        prev_mem_refs = copy(sra.mem_refs)
        prev_device_refs = copy(sra.device_refs)
        if sra.policy == :LRU
            del_id = r3.id
            #r3 = nothing
            MemPool.delete_from_device!(sra, r3)
        else
            del_id = r2.id
            #r2 = nothing
            MemPool.delete_from_device!(sra, r2)
        end
        @test_broken !haskey(MemPool.datastore, del_id)
        @test !in(del_id, sra.mem_refs)
        @test !in(del_id, sra.device_refs)
        @test !haskey(sra.ref_cache, del_id)
        @test sra.mem_refs == prev_mem_refs
        @test sra.device_refs == filter(id->id != del_id, prev_device_refs)

        # Try to add a bulky object that doesn't fit in memory
        prev_mem_refs = copy(sra.mem_refs)
        prev_device_refs = copy(sra.device_refs)
        r7 = poolset(collect(1:11))
        @test_throws ArgumentError MemPool.set_device!(sra, r7)
        @test sra.mem_refs == prev_mem_refs
        @test sra.device_refs == prev_device_refs
        @test !haskey(sra.ref_cache, r7.id)

        # Add a bulky object that doesn't fit in memory or on disk
        prev_mem_refs = sra.mem_refs
        prev_device_refs = sra.device_refs
        r8 = poolset(collect(1:10_001))
        @test_throws ArgumentError MemPool.set_device!(sra, r8)
        @test sra.mem_refs == prev_mem_refs
        @test sra.device_refs == prev_device_refs
        @test !haskey(sra.ref_cache, r8.id)
    end

    # Whole-device retention applies to all objects
    dirname = mktempdir()
    sdevice2 = SerializationFileDevice(dirname)
    sra = MemPool.SimpleRecencyAllocator(8*10, sdevice2, 8*10_000, :LRU)
    refs = [poolset(123; device=sra) for i in 1:8]
    @test isempty(sra.device_refs)
    MemPool.retain_on_device!(sra, true)

    # Retention is immediate
    @test !isempty(sra.device_refs)
    @test length(readdir(dirname)) == 8

    # Files still exist after refs expire
    refs = nothing; GC.gc(); sleep(0.5)
    @test isempty(sra.mem_refs)
    @test isempty(sra.device_refs)
    @test length(readdir(dirname)) == 8

    # Counters are properly cleared (https://github.com/JuliaParallel/DTables.jl/issues/60)
    sra = MemPool.SimpleRecencyAllocator(8*10, sdevice2, 8*10_000, :LRU)
    function generate()
        poolset(collect(1:10); device=sra)
        poolset(collect(1:10); device=sra)
        return
    end
    generate()
    @test sra.mem_size[] > 0
    @test sra.device_size[] > 0
    for _ in 1:3
        GC.gc()
        yield()
    end
    @test sra.mem_size[] == 0
    @test sra.device_size[] == 0
end

@testset "Mountpoints and Disk Stats" begin
    mounts = MemPool.mountpoints()
    @test mounts isa Vector{String}
    @test length(mounts) > 0
    if Sys.iswindows()
        @test "C:" in mounts
    else
        @test "/" in mounts
    end
    for mount in mounts
        if ispath(mount)
            stats = MemPool.disk_stats(mount)
            @test stats.available > 0
            @test stats.capacity > 0
            @test stats.available < stats.capacity
        end
    end
end

@testset "High-level APIs" begin
    sdevice = SerializationFileDevice()
    devices = [CPURAMDevice(),
               sdevice,
               SimpleRecencyAllocator(8, sdevice, 1024^3, :LRU)]

    # Resource queries work correctly
    for device in devices
        resources = MemPool.storage_resources(device)
        @test length(resources) >= 1
        @test length(unique(resources)) == length(resources)
        for resource in resources
            @test MemPool.storage_capacity(device, resource) > 0
            @test MemPool.storage_available(device, resource) >= 0
            @test MemPool.storage_utilized(device, resource) >= 0

            @test MemPool.externally_varying(device) isa Bool
            if !MemPool.externally_varying(device)
                @test MemPool.storage_capacity(device, resource) ==
                      MemPool.storage_available(device, resource) +
                      MemPool.storage_utilized(device, resource)
            end
        end

        # Wrong resource passed to resource queries throw errors
        wrong_res = MemPool.FilesystemResource("/fake/path")
        @test_throws ArgumentError MemPool.storage_capacity(device, wrong_res)
        @test_throws ArgumentError MemPool.storage_available(device, wrong_res)
        @test_throws ArgumentError MemPool.storage_utilized(device, wrong_res)
    end

    # All APIs accept either DRef, ref ID, or RefState as identifier
    # Either state or device may be passed
    # Redundant set, read, write, retain, delete are allowed
    # Non-read calls return nothing
    # set_device! sets root and leaf
    for device in devices
        x1 = poolset(123)
        state = MemPool.datastore[x1.id]

        # set_device! requires access to ref ID and device to set
        @test MemPool.set_device!(device, x1) ===
              MemPool.set_device!(device, state, x1) ===
              MemPool.set_device!(device, x1.id) ===
              MemPool.set_device!(device, state, x1.id) ===
              nothing

        @test MemPool.isinmemory(x1) ==
              MemPool.isinmemory(x1.id) ==
              MemPool.isinmemory(state)
        @test MemPool.isondisk(x1) ==
              MemPool.isondisk(x1.id) ==
              MemPool.isondisk(state)

        # Root and leaf are set appropriately
        sstate = storage_read(state)
        @test sstate.root === device
        if device isa CPURAMDevice
            @test length(sstate.leaves) == 0
        elseif device isa SerializationFileDevice
            @test length(sstate.leaves) == 1
            @test first(sstate.leaves).device === device
        elseif device isa SimpleRecencyAllocator
            x2 = poolset(456; device) # to push x1 to disk
            @test first(storage_read(state).leaves).device !== device
        end

        @test MemPool.read_from_device(state, x1, true) ==
              MemPool.read_from_device(device, x1, true) ==
              MemPool.read_from_device(device, state, x1, true) ==
              MemPool.read_from_device(state, x1.id, true) ==
              MemPool.read_from_device(device, x1.id, true) ==
              MemPool.read_from_device(device, state, x1.id, true)

        # When ret == false, nothing is returned
        @test MemPool.read_from_device(state, x1, false) ===
              MemPool.read_from_device(device, x1, false) ===
              MemPool.read_from_device(device, state, x1, false) ===
              MemPool.read_from_device(state, x1.id, false) ===
              MemPool.read_from_device(device, x1.id, false) ===
              MemPool.read_from_device(device, state, x1.id, false) ===
              nothing

        @test MemPool.write_to_device!(state, x1) ===
              MemPool.write_to_device!(device, x1) ===
              MemPool.write_to_device!(device, state, x1) ===
              MemPool.write_to_device!(state, x1.id) ===
              MemPool.write_to_device!(device, x1.id) ===
              MemPool.write_to_device!(device, state, x1.id) ===
              nothing

        for mode in [true, false]
            @test MemPool.retain_on_device!(state, x1, mode; all=true) ===
                  MemPool.retain_on_device!(device, x1, mode; all=true) ===
                  MemPool.retain_on_device!(device, state, x1, mode; all=true) ===
                  MemPool.retain_on_device!(state, x1.id, mode; all=true) ===
                  MemPool.retain_on_device!(device, x1.id, mode; all=true) ===
                  MemPool.retain_on_device!(device, state, x1.id, mode; all=true) ===
                  nothing
        end

        @test MemPool.delete_from_device!(state, x1) ===
              MemPool.delete_from_device!(device, x1) ===
              MemPool.delete_from_device!(device, state, x1) ===
              MemPool.delete_from_device!(state, x1.id) ===
              MemPool.delete_from_device!(device, x1.id) ===
              MemPool.delete_from_device!(device, state, x1.id) ===
              nothing

        # Delete clears all leaf devices
        @test length(storage_read(state).leaves) == 0
    end

    # Garbage ref IDs passed to APIs which don't take a RefState always throw
    for device in devices
        @test_throws Exception MemPool.set_device!(device, typemax(Int))
        @test_throws Exception MemPool.read_from_device(device, typemax(Int), true)
        @test_throws Exception MemPool.write_to_device!(device, typemax(Int))
        @test_throws Exception MemPool.delete_from_device!(device, typemax(Int))
    end

    @testset "set_device! failure" begin
        # N.B. That this device doesn't fully conform to semantics
        struct FailingWriteStorageDevice <: MemPool.StorageDevice end
        MemPool.write_to_device!(::FailingWriteStorageDevice, ::MemPool.RefState, ::Any) =
            error("Failed to write")

        # Allocate directly throws and does not have datastore entry
        GC.gc(); sleep(1)
        len = length(MemPool.datastore)
        @test_throws ErrorException poolset(123; device=FailingWriteStorageDevice())
        GC.gc(); sleep(1)
        @test length(MemPool.datastore) == len

        # Allocate, then set, throws and has datastore entry
        x = poolset(123)
        @test_throws ErrorException MemPool.set_device!(FailingWriteStorageDevice(), x)
        @test haskey(MemPool.datastore, x.id)
    end

    # Retention can be set in poolset
    x1 = poolset(123; device=sdevice)
    @test !only(storage_read(MemPool.datastore[x1.id]).leaves).retain
    x1 = poolset(123; device=sdevice, retain=true)
    @test only(storage_read(MemPool.datastore[x1.id]).leaves).retain
    MemPool.retain_on_device!(sdevice, x1, false)
end

#= TODO
Allocate, write non-CPU A, write non-CPU B, handle for A was explicitly deleted
Allocate, chain write and reads such that write starts before, and finishes after, read, ensure ordering is correct
Stress RCU with many readers and one write-delete-read task
=#
