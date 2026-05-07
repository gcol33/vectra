## Release summary

This is a feature release (0.5.1 -> 0.6.1) introducing a new tiled raster
format (`.vec`) with overview pyramids and time cubes, parallel tile decode,
plus GeoTIFF read/write enhancements (tiled, BigTIFF, LZW + Predictor 2,
GeoKey CRS round-trip, `GDAL_METADATA` band-name parser).

## clang 21 / r-devel-linux-x86_64-debian-clang archive fix

vectra 0.5.1 was archived after `block.c` failed to compile on the
r-devel-clang flavor: clang 21's bundled `omp.h` wrapper contains an
unbalanced `#pragma omp end declare variant`, which trips on any TU that
includes the wrapper, even with the include guarded by `#ifdef _OPENMP`
(when `-fopenmp` is on the compile line, `_OPENMP` is defined and the
broken wrapper is pulled in). 0.6.1 fixes this by forward-declaring the
three OpenMP runtime functions vectra uses and skipping the wrapper
entirely. The `#pragma omp ...` directives elsewhere in `src/` are still
recognised, and the runtime symbols resolve at link time via libomp.

## UBSAN nonnull fix

Addresses a UBSAN nonnull-attribute trap that affected `collect()` and the
internal `block_array_gather` path when a `.vtr` batch contained only empty
or `NA` strings: the gather code called `Rf_mkCharLenCE(NULL, 0, CE_UTF8)`
and the dedup-cache called `memcmp(NULL, ...)` even though the length was
zero. Empty strings are now shortcut to `R_BlankString` before either call.

## Test environments

* local Windows 11, R 4.6.0 (GCC 14.3.0 via Rtools 46)
* GitHub Actions: ubuntu-latest, macos-latest, windows-latest, R-devel +
  R-release
* GitHub Actions: ASAN/UBSAN job on r-devel-ubsan-clang
* GitHub Actions: rchk job

## R CMD check results

0 errors, 0 warnings, 0 NOTEs related to the package.
(One local-environment NOTE "unable to verify current time" appears on this
Windows machine; it does not occur on CRAN's builders.)

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
