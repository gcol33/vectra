## Release summary

This is a feature + archive-fix release (0.5.1 -> 0.6.2). Carries the
0.6.0 raster work (tiled .vec format with overview pyramids and time
cubes, parallel tile decode, tiled / BigTIFF / LZW GeoTIFF write,
GeoKey CRS round-trip, GDAL_METADATA band-name parser, UBSAN
empty-string fix) and the 0.6.1 clang 21 omp.h wrapper fix forward.

## Resolution of 0.5.1 archive issues

The 2026-05-06 archive auto-check pointed at three artefacts. All
three are addressed:

1. r-devel-linux-x86_64-debian-clang (the original archive trigger):
   clang 21's bundled omp.h wrapper has an unbalanced
   `#pragma omp end declare variant`, which fails compilation of any
   TU that includes the wrapper, even with the include guarded by
   `#ifdef _OPENMP`. 0.6.1 forward-declared the three OpenMP runtime
   functions vectra uses and stopped including <omp.h>. Pragmas are
   still recognised; libomp symbols resolve at link time.

2. gcc-ASAN heap-buffer-overflow in the LZ decode path
   (`read_rg_tdc_with_fp` in `vtr1_tdc.c`, downstream of
   `tdc_match_copy`'s SIMD wildcopy): the consolidated decode pipeline
   in tdc (`src/api/decode_impl.c`) now always allocates scratch
   buffers with a +16-byte wildcopy slack, so the SIMD overshoot
   stays within the allocation. The `decode_ex.c` variant that was
   missing this slack on 0.5.1 has been folded into the shared
   `driver_decode_block_impl`. The vectra GitHub Actions sanitizer
   workflow now also renders every vignette under ASAN/UBSAN to
   guard against regression on the same code path that BDR's
   gcc-ASAN run catches.

3. rchk PROTECT findings in `src/r_bridge.c`, `src/r_bridge_io.c`,
   `src/vtr1_tdc.c`, and `src/collect.c`: every `Rf_getAttrib` and
   `Rf_mkString` result that crossed an allocating call (`R_alloc`,
   `Rf_warning`, `Rf_setAttrib`, `Rf_asReal`, `Rf_asInteger`,
   `parse_*`) is now `PROTECT`ed with a matching `UNPROTECT`.
   Touches `apply_annotation`, `C_write_vtr`, `C_write_vtr_tdc`,
   `parse_quantize`, and `parse_spatial`.

## DESCRIPTION wording

The 0.6.1 incoming pretest flagged "gridded" as a possibly-misspelled
word. Replaced with "raster".

## Test environments

* local Windows 11, R 4.6.0 (GCC 14.3.0 via Rtools 46) -- 0/0/0
* GitHub Actions: ubuntu-latest, macos-latest, windows-latest,
  R-devel + R-release
* GitHub Actions: ASAN/UBSAN job on r-devel-ubsan-clang, including a
  vignette-render pass under sanitizers
* GitHub Actions: rchk job

## R CMD check results

0 errors, 0 warnings, 0 NOTEs related to the package.
(One local-environment NOTE "unable to verify current time" appears
on this Windows machine; it does not occur on CRAN's builders.)

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
