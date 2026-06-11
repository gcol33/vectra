## Feature release

0.7.0 is a feature update to the version currently published on CRAN
(0.6.2, accepted 2026-05-08). It adds streaming consumption and
out-of-core fitting on top of the existing columnar engine, with no
breaking changes to the public verbs or the `.vtr` / `.vec` on-disk
formats.

New user-visible functions:

* `collect_chunked(x, f, .init)` folds a function over a query one
  batch at a time, so a result larger than RAM can be reduced to a
  small summary in a single bounded-memory pass.
* `chunk_feeder(.source)` turns a query into a resettable generator
  following the `data(reset)` protocol `biglm::bigglm()` expects, so a
  generalized linear model can be fitted out-of-core.
* `offload(x)` materializes a query once to a `.vtr` and returns a node
  that streams it back from disk; `offload(x, by = ...)` splits a query
  into disjoint per-key shards in a single streaming pass.
* `group_map()` / `group_modify()` apply a function to each shard of a
  partition.

Both streaming verbs sit on one new C pull interface (`C_node_optimize`,
`C_node_next_batch`); per-batch conversion reuses the existing column
converter, so the chunked and materializing paths share one code path.

The release also carries the fix previously prepared as 0.6.3 (never
submitted): `summarise()` now accepts namespace-qualified aggregation
calls such as `vectra::n()` / `vectra::sum(x)`.

`biglm` is added to Suggests; it is used only in examples and a vignette,
both guarded by `requireNamespace()`.

## Test environments

* local Windows 11, R 4.6.0 (GCC 14.3.0 via Rtools 46) -- 0/0/0
* win-builder, R-devel (x86_64)
* GitHub Actions: macOS, Windows, ubuntu-latest (R-devel, R-release,
  R-oldrel-1)
* GitHub Actions: ASAN/UBSAN job on Linux (gcc -fsanitize=address,undefined)

## R CMD check results

0 errors, 0 warnings, 0 NOTEs related to the package.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
