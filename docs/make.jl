using MemPool
using Documenter
import Documenter.Remotes: GitHub

makedocs(;
modules = [MemPool],
authors = "JuliaParallel and contributors",
repo = GitHub("JuliaParallel", "MemPool.jl"),
sitename = "MemPool.jl",
format = Documenter.HTML(;
prettyurls = get(ENV, "CI", "false") == "true",
canonical = "https://juliaparallel.github.io/MemPool.jl",
assets = String["assets/favicon.ico"],
),
pages = [
"Home" => "index.md",
"API Reference" => "api.md",
],
warnonly = [:missing_docs]
)

deploydocs(;
repo = "github.com/JuliaParallel/MemPool.jl",
devbranch = "main", # Or "master", check your repo's default branch
)