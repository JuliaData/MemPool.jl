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

function mountpoints(;fstab::String="/etc/fstab")
    bufs = MntEntry[]
    mnt = ccall(:setmntent, Ptr{Cvoid}, (Cstring, Cstring), path, "r")
    Base.systemerror("Failed to setmntent at $path", UInt64(mnt) == 0)
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

# TODO: mountpoints

else

# FIXME
error("Not implemented yet")

end
