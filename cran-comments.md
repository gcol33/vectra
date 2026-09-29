## Submission

This is a feature update. The main changes are:

* `tbl_parquet()`, a native Parquet reader implemented in C with no
  dependency on 'arrow'. ZSTD-compressed pages are decoded by the zstd
  decompressor, included in `src/zstd` under its BSD license.
* `vectra_mem()` is now a single memory bound per query plan, shared by the
  sort, join and aggregation nodes, and grouped `summarise()` aggregates in
  hash tables that spill to disk past that bound.
* LZW-compressed GeoTIFF input, trigonometric functions in expressions, and
  fixes to `bind_rows()` type widening and filter pushdown.

NEWS.md lists every change.

## Authors@R

`Authors@R` now lists the authors and copyright holders of the bundled third
party code: Yann Collet and Meta Platforms (zstd and xxHash, `src/zstd`), and
Rich Geldreich, Martin Raiber, RAD Game Tools and Valve Software, and Tenacious
Software (miniz, `src/miniz`). Alistair Moffat and Jyrki Katajainen are listed
as contributors because a routine in miniz credits them by name.

## Test environments

* Local: Windows 11, R 4.6.1, `R CMD check --as-cran` on the built tarball
* win-builder: R-devel
* GitHub Actions: R-CMD-check (macOS, Windows, Ubuntu release/devel/oldrel-1),
  gcc ASAN/UBSAN, clang `-fsanitize=undefined,function`

## R CMD check results

0 errors | 0 warnings | 1 note

The note is from the incoming feasibility check (days since the last update).

## Reverse dependencies

taxify imports vectra. `R CMD check` of taxify 0.5.5 (the CRAN version)
against this release: see below.
