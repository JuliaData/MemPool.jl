# Network transfer abstractions

struct DistributedNetwork end

initialize(net::DistributedNetwork, procs) = nothing
Distributed.remotecall_fetch(net::DistributedNetwork, args...) =
    Distributed.remotecall_fetch(args...)
Distributed.remotecall_fetch(f::Function, net::DistributedNetwork, args...) =
    Distributed.remotecall_fetch(f, args...)
