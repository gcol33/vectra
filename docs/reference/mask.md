# Mask a streamed raster to a polygon layer

Keeps the pixels of a `.vec` raster whose cell centre falls inside a
resident polygon layer and sets the rest to `background`, reading the
raster one tile-row strip at a time so the whole grid is never resident.
It is the raster counterpart of
[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md):
the streamed side is the (large) raster and the small `mask` layer stays
in memory. With `inverse = TRUE` the inside is cleared and the outside
kept.

## Usage

``` r
mask(
  x,
  mask,
  inverse = FALSE,
  band = NULL,
  background = NA_real_,
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

- mask:

  An `sf` or `sfc` polygon layer to clip against. When it carries no CRS
  it inherits the raster's EPSG.

- inverse:

  If `FALSE` (default) keep pixels inside `mask`; if `TRUE` keep the
  pixels outside it.

- band:

  Band(s) to mask (1-based). Default `NULL` masks every band.

- background:

  Value written to cleared pixels. Default `NA_real_`.

- path:

  Optional output `.vec` path. When given the result is streamed to disk
  and the opened
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
  handle is returned invisibly; when `NULL` the result is returned in
  memory (a matrix for one band, a list of matrices for several).

- dtype:

  Storage dtype for `.vec` output (see
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)).
  Default `"f32"`.

- compression:

  Compression effort for `.vec` output. Default `"fast"`.

## Value

When `path` is `NULL`, a numeric matrix (one band) or a list of matrices
(several), each carrying `gt`, `extent`, and `crs` attributes (row 1
northmost). When `path` is given, the written `vectra_raster` handle
(invisibly).

## Details

This is the *monoid fold* tier of the spatial toolbox: bounded to one
strip plus the resident mask, a single streaming pass, no spill. A pixel
is tested against the mask only when its centre falls in the mask
bounding box, so the point-in-polygon work stays proportional to the
overlap rather than the whole grid. Point-in-polygon is delegated to sf
(an optional dependency); topology and CRS handling are sf's.

## See also

[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md)
for the vector analogue,
[`zonal()`](https://gillescolling.com/vectra/reference/zonal.md) for
per-zone summaries over the same pixel-in-polygon assignment.

## Examples

``` r
vals <- matrix(1:100, 10, 10, byrow = TRUE)
f <- tempfile(fileext = ".vec")
vec_write_raster(vals, f, dtype = "f64", extent = c(0, 0, 10, 10))

disc <- sf::st_buffer(sf::st_sfc(sf::st_point(c(5, 5))), 3)
inside <- mask(f, disc)
sum(!is.na(inside))
unlink(f)
```
