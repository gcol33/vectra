# Euclidean distance to the nearest feature (proximity)

Computes, for every cell of a `.vec` raster, the straight-line Euclidean
distance to the nearest feature cell, in CRS units. Feature cells are
the non-NA cells by default, or the cells whose value is in `target`.
This is the raster proximity / Euclidean-distance staple, the distance
companion to
[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md).

## Usage

``` r
proximity(
  x,
  target = NULL,
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

- target:

  Optional numeric vector of feature values. When `NULL` (default) every
  non-NA cell is a feature; otherwise a cell is a feature when its value
  is in `target`.

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
`extent`, and `crs` attributes, with distance in CRS units and `NA`
where the raster holds no feature anywhere. When `path` is given, the
written `vectra_raster` handle (invisibly).

## Details

The exact Euclidean distance transform is separable (Felzenszwalb and
Huttenlocher 2012): a one-dimensional lower-envelope-of-parabolas
transform along the rows, then the same transform along the columns,
each linear in the line length and each line independent. vectra runs it
as four streamed passes over tile-row strips, with an out-of-core
transpose between the row pass and the column pass, so the whole grid is
never resident. The row pass scales squared distances by the x
resolution and the column pass by the y resolution, so the result is
exact on anisotropic (non-square) cells. This places proximity on the
sort / partition tier of the spatial toolbox.

Distances are straight-line Euclidean in the raster CRS units.
Cost-distance, which accumulates a per-cell friction along the path, is
a global shortest-path problem and stays resident:
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) the
raster and run a resident solver for that.

## See also

[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
to build a raster from streamed points,
[`mask()`](https://gillescolling.com/vectra/reference/mask.md) to clip a
raster to a polygon layer.

## Examples

``` r
m <- matrix(NA_real_, 12, 12)
m[3, 4] <- 1; m[9, 10] <- 1
f <- tempfile(fileext = ".vec")
vec_write_raster(m, f, dtype = "f64", extent = c(0, 0, 12, 12))

d <- proximity(f)
round(d[1:3, 1:3], 2)
unlink(f)
```
