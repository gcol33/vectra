# Cellwise calculation over aligned rasters (map algebra)

Evaluates an expression cell by cell across one or more `.vec` rasters
that share a grid, reading every input one tile-row strip at a time so
no whole band is ever resident. Inside `expr` each name in `rasters`
refers to that raster's strip as a numeric vector, so ordinary
vectorised R expresses the calculation: a band index
`(nir - red) / (nir + red)`, a reclassification `cut(dem, breaks)`, a
threshold `ifelse(slope > 30, 1L, 0L)`, or arithmetic across layers. The
result is written one strip at a time to a single-band output.

## Usage

``` r
rast_calc(
  rasters,
  expr,
  band = 1L,
  path = NULL,
  dtype = "f32",
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- rasters:

  A named list of `vectra_raster` handles or `.vec` paths sharing a
  grid. The names are the variables available inside `expr`.

- expr:

  An expression in those names producing one value per cell (or a
  scalar, recycled). Evaluated against each strip with the caller's
  environment as the enclosing scope.

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

When `path` is `NULL`, a numeric matrix (row 1 northmost) carrying `gt`,
`extent`, and `crs` attributes. When `path` is given, the written
`vectra_raster` handle (invisibly).

## Details

This is the *monoid fold* tier of the spatial toolbox: bounded to one
strip per input, a single streaming pass, no spill. The rasters must
share dimensions and geotransform;
[`warp()`](https://gillescolling.com/vectra/reference/warp.md) them onto
a common grid first if they do not. No sf is needed.

## See also

[`warp()`](https://gillescolling.com/vectra/reference/warp.md) to align
rasters onto a shared grid first,
[`focal()`](https://gillescolling.com/vectra/reference/focal.md) for
neighbourhood rather than cellwise calculation.

## Examples

``` r
nir <- matrix(c(40, 50, 60, 70), 2, 2)
red <- matrix(c(10, 20, 30, 40), 2, 2)
fn <- tempfile(fileext = ".vec"); fr <- tempfile(fileext = ".vec")
vec_write_raster(nir, fn, dtype = "f64", extent = c(0, 0, 2, 2))
vec_write_raster(red, fr, dtype = "f64", extent = c(0, 0, 2, 2))

ndvi <- rast_calc(list(nir = fn, red = fr), (nir - red) / (nir + red))
round(ndvi, 3)
unlink(c(fn, fr))
```
