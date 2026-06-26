# Moving-window (focal) statistics over a streamed raster

Applies a moving window to a `.vec` raster, reading the input one
tile-row strip at a time – each strip expanded by the kernel radius (a
halo read) so window neighbours are available without ever holding the
whole grid resident. The per-window statistic is computed in C. When
`path` is given the output is streamed straight back to a new `.vec` one
tile-row at a time, so neither the input nor the output band is ever
fully in memory; this is the raster op that runs out of core where an
in-memory engine needs the whole raster at once.

## Usage

``` r
focal(
  x,
  w = matrix(1, 3, 3),
  fun = c("mean", "sum", "min", "max", "sd", "median"),
  na.rm = TRUE,
  band = 1L,
  path = NULL,
  dtype = "f32",
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- x:

  A `vectra_raster` (from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md))
  or a path to a `.vec` raster.

- w:

  A numeric weight matrix with odd dimensions, or a single positive odd
  integer `k` for a `k x k` window of ones. Default `matrix(1, 3, 3)`.

- fun:

  Window statistic: one of `"sum"`, `"mean"`, `"min"`, `"max"`, `"sd"`,
  `"median"`. Default `"mean"`.

- na.rm:

  Skip nodata cells inside the window (`TRUE`, default) or let them
  propagate `NA` (`FALSE`).

- band:

  Band to read (1-based). Default 1.

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
`extent`, `crs`, and `fun` attributes. When `path` is given, the written
`vectra_raster` handle (invisibly).

## Details

This is the *sort / partition* tier of the spatial toolbox: bounded to
one haloed strip at a time, exploiting tile locality.

The window `w` is a numeric weight matrix with odd dimensions (or a
single odd integer `k`, shorthand for a `k x k` matrix of ones). `NA`
weights mark cells outside the window. For `fun = "sum"`/`"mean"` the
weights scale the values (sum is `sum(w * x)`, mean is
`sum(w * x) / sum(w)`); for the other statistics a finite weight only
marks membership. With `na.rm = TRUE` (the default) nodata cells inside
the window are skipped; with `na.rm = FALSE` any nodata cell – including
a window that runs off the raster edge – makes the result `NA`, matching
the resident behaviour.

## See also

[`terrain()`](https://gillescolling.com/vectra/reference/terrain.md) for
DEM derivatives built on the same strip pass,
[`zonal()`](https://gillescolling.com/vectra/reference/zonal.md) for
per-zone summaries.

## Examples

``` r
m <- matrix(1:36, 6, 6, byrow = TRUE)
f <- tempfile(fileext = ".vec")
vec_write_raster(m, f, dtype = "f64", extent = c(0, 0, 6, 6))

# 3x3 mean smoother; edge cells see off-raster neighbours.
focal(f, w = matrix(1, 3, 3), fun = "mean")
unlink(f)
```
