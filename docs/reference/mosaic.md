# Merge aligned rasters onto a common grid

Combines several `.vec` rasters that share a resolution and cell grid
into one raster spanning their union, resolving overlap with `fun`. The
output is walked one tile-row strip at a time and each input contributes
only the window overlapping the current strip, so neither the inputs nor
the output are held whole in memory.

## Usage

``` r
mosaic(
  rasters,
  fun = c("first", "last", "mean", "sum", "min", "max"),
  band = 1L,
  path = NULL,
  dtype = "f32",
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- rasters:

  A list of `vectra_raster` handles or `.vec` paths sharing resolution
  and grid alignment.

- fun:

  Overlap rule where inputs cover the same cell: `"first"` (the earliest
  input in `rasters`, default), `"last"`, `"mean"`, `"sum"`, `"min"`, or
  `"max"`. Cells covered by no input come back `NA`.

- band:

  Band read from every input (1-based). Default 1.

- path:

  Optional output `.vec` path. When given the result is streamed to disk
  and the opened
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
  handle is returned invisibly; when `NULL` the result is returned as an
  in-memory matrix.

- dtype:

  Storage dtype for `.vec` output (see
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)).
  Default `"f32"`.

- compression:

  Compression effort for `.vec` output. Default `"fast"`.

## Value

When `path` is `NULL`, a numeric matrix on the union grid (row 1
northmost) carrying `gt`, `extent`, and `crs` attributes. When `path` is
given, the written `vectra_raster` handle (invisibly).

## Details

This is the *monoid fold* tier of the spatial toolbox: each output strip
folds the overlapping input windows, bounded memory, no spill. The
inputs must share resolution and lie on a common cell grid;
[`warp()`](https://gillescolling.com/vectra/reference/warp.md) them onto
a shared grid first if they do not. No sf is needed.

## See also

[`warp()`](https://gillescolling.com/vectra/reference/warp.md) to bring
rasters onto a shared grid,
[`rast_calc()`](https://gillescolling.com/vectra/reference/rast_calc.md)
for cellwise combination of already-aligned rasters.

## Examples

``` r
a <- matrix(1, 4, 4); b <- matrix(2, 4, 4)
fa <- tempfile(fileext = ".vec"); fb <- tempfile(fileext = ".vec")
vec_write_raster(a, fa, dtype = "f64", extent = c(0, 0, 4, 4))
vec_write_raster(b, fb, dtype = "f64", extent = c(2, 2, 6, 6))

m <- mosaic(list(fa, fb), fun = "mean")
dim(m)
unlink(c(fa, fb))
```
