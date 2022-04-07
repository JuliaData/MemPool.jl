if Sys.isunix()

struct StatFSStruct
    f_bsize::Culong
    f_frsize::Culong
    f_blocks::Culong
    f_bfree::Culong
    f_bavail::Culong
    f_files::Culong
    f_ffree::Culong
    f_favail::Culong
    f_fsid::Culong
    f_flag::Culong
    f_namemax::Culong
    reserved::NTuple{6,Cint}
end

function statvfs(path::String)
    buf = Ref{StatFSStruct}()
    GC.@preserve buf begin
        errno = ccall(:statvfs, Cint, (Cstring, Ptr{StatFSStruct}), path, buf)
        if errno != 0
            Base.systemerror(errno)
        end
        buf[]
    end
end

function disk_stats(path::String)
    vfs = statvfs(path)
    return (
        available = vfs.f_bavail * vfs.f_bsize,
        capacity = vfs.f_blocks * vfs.f_bsize
    )
end

struct MntEntStruct
    mnt_fsname::Cstring
    mnt_dir::Cstring
    mnt_type::Cstring
    mnt_opts::Cstring
    mnt_freq::Cint
    mnt_passno::Cint
end
struct MntEntry
    mnt_fsname::String
    mnt_dir::String
    mnt_type::String
    mnt_opts::String
    mnt_freq::Cint
    mnt_passno::Cint
end

function getmntent(;fstab::String="/etc/fstab")
    bufs = MntEntry[]
    mnt = ccall(:setmntent, Ptr{Cvoid}, (Cstring, Cstring), fstab, "r")
    Base.systemerror("Failed to setmntent at $fstab", UInt64(mnt) == 0)
    while true
        ret = ccall(:getmntent, Ptr{MntEntStruct}, (Ptr{Cvoid},), mnt)
        UInt64(ret) == 0 && break
        buf = unsafe_load(ret)
        push!(bufs, MntEntry(
            deepcopy(unsafe_string(buf.mnt_fsname)),
            deepcopy(unsafe_string(buf.mnt_dir)),
            deepcopy(unsafe_string(buf.mnt_type)),
            deepcopy(unsafe_string(buf.mnt_opts)),
            buf.mnt_freq,
            buf.mnt_passno
        ))
    end
    ccall(:endmntent, Cvoid, (Ptr{Cvoid},), mnt)
    bufs
end
mountpoints() = map(mnt->mnt.mnt_dir, getmntent())

elseif Sys.iswindows()

function disk_stats(path::String)
    lpDirectoryName = path
    lpFreeBytesAvailableToCaller = Ref{Int64}(0)
    lpTotalNumberOfBytes = Ref{Int64}(0)
    lpTotalNumberOfFreeBytes = Ref{Int64}(0)

    r = ccall(
        (:GetDiskFreeSpaceExA, "kernel32"), Bool,
        (Cstring, Ref{Int64}, Ref{Int64}, Ref{Int64}),
        lpDirectoryName,
        lpFreeBytesAvailableToCaller,
        lpTotalNumberOfBytes,
        lpTotalNumberOfFreeBytes
    )
    @assert r "Failed to query disk stats of $path"

    return (
        available = lpFreeBytesAvailableToCaller[],
        capacity = lpTotalNumberOfBytes[]
    )
end

function mountpoints()
    mounts = String[]
    path = Libc.malloc(256)
    mnt = ccall((:FindFirstVolumeW, "kernel32"), Ptr{Cvoid},
                (Ptr{Cvoid}, Int), path, 256)
    Base.systemerror("Failed to FindFirstVolumeW at $path", UInt64(mnt) == 0)
    push!(mounts, unsafe_string(reinterpret(Cstring, path)))
    while true
        ret = ccall((:FindNextVolumeW, "kernel32"), Bool,
                    (Ptr{Cvoid}, Ptr{Cvoid}, Int), mnt, path, 256)
        ret || break
        push!(mounts, unsafe_string(reinterpret(Cstring, path)))
    end
    ccall((:FindVolumeClose, "kernel32"), Cvoid, (Ptr{Cvoid},), mnt)
    Libc.free(path)
    mounts
end

else

mountpoints() = error("Not implemented yet")
disk_stats(path::String) = error("Not implemented yet")

end
