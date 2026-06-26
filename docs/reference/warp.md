# Resample or reproject a streamed raster onto a target grid

Warps a `.vec` raster onto a target grid, walking the *output* one
tile-row strip at a time. For each strip the target pixel-centre
coordinates are built, projected into the source coordinate reference
system when the two CRSs differ (delegated to PROJ via sf), mapped
through the source geotransform to fractional source pixels, and sampled
from the bounded source window those coordinates fall in. The output is
assembled in memory or streamed straight back to a new `.vec`, so the
whole output grid is never resident; the source is read in bounded
windows rather than held whole.

## Usage

``` r
warp(
  x,
  template,
  method = c("near", "bilinear", "cubic"),
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
  or a path to a `.vec` raster to warp.

- template:

  The target grid: a `vectra_raster` / `.vec` path whose grid and CRS
  are borrowed, or a list `list(crs =, extent =, res =, dims =)`. With
  `crs` and `res` but no `extent`, the target extent is the source's
  corners projected into `crs`.

- method:

  Resampling method: `"near"`, `"bilinear"`, or `"cubic"`. Default
  `"near"`.

- band:

  Band to warp (1-based). Default 1.

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

When `path` is `NULL`, a numeric matrix on the target grid (row 1
northmost) carrying `gt`, `extent`, and `crs` attributes. When `path` is
given, the written `vectra_raster` handle (invisibly).

## Details

This is the *sort / partition* tier of the spatial toolbox: each output
strip reads the source window it projects onto. For a mild reprojection
or a plain resample that window is a thin band; a strong reprojection
can make it large, but the output stays streamed throughout.

Sampling follows the GDAL / terra convention (pixel centres at
half-integer coordinates). `"near"` takes the nearest source cell;
`"bilinear"` the 2x2 weighted mean; `"cubic"` the 4x4 cubic convolution
(Catmull-Rom, a = -0.5). A target cell whose sampling kernel reaches
outside the source extent, or touches a nodata cell, comes back `NA`.

Reprojection happens only when both rasters carry a known EPSG code and
the codes differ; otherwise `warp()` resamples within a shared CRS and
needs no sf.

## See also

[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
to build a raster from streamed vector features,
[`focal()`](https://gillescolling.com/vectra/reference/focal.md) for
moving-window statistics.

## Examples

``` r
z <- outer(1:8, 1:8, function(r, c) r + 2 * c)
f <- tempfile(fileext = ".vec")
vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 8, 8))

# Resample onto a finer grid over the same extent.
fine <- warp(f, list(extent = c(0, 0, 8, 8), res = 0.5), method = "bilinear")
dim(fine)
unlink(f)
```
