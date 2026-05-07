## Release summary

This is a feature release (0.5.1 -> 0.6.0) introducing a new tiled raster
format (`.vec`) with overview pyramids and time cubes, parallel tile decode,
plus GeoTIFF read/write enhancements (tiled, BigTIFF, LZW + Predictor 2,
GeoKey CRS round-trip, `GDAL_METADATA` band-name parser).

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
