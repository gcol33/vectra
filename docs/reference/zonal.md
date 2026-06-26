# Summarise raster values within zones

Reduces a raster to one summary row per zone, streaming the raster one
tile-row strip at a time so the whole grid never has to be resident.
Zones come either from a second raster aligned to the value grid (each
pixel's zone is that raster's value, the
[`terra::zonal`](https://rspatial.github.io/terra/reference/zonal.html)
pattern) or from an sf polygon layer (each pixel is assigned the polygon
its centre falls in). The per-zone running moments (count, sum, sum of
squares, min, max) are folded in memory as strips arrive, so peak memory
is one strip plus the small per-zone table regardless of raster size.
This is the *monoid fold* tier of the spatial toolbox: bounded memory, a
single streaming pass, no spill.

## Usage

``` r
zonal(
  raster,
  zones,
  fun = "mean",
  band = 1L,
  zone_band = 1L,
  zone_field = NULL,
  na.rm = TRUE
)
```

## Arguments

- raster:

  A `vectra_raster` (from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md))
  or a path to a `.vec` raster holding the values to summarise.

- zones:

  The zones to summarise within: a `vectra_raster` / `.vec` path aligned
  to `raster` (zone id per pixel), or an `sf`/`sfc` polygon layer.

- fun:

  One or more of `"mean"`, `"sum"`, `"count"`, `"min"`, `"max"`, `"sd"`.
  Each becomes a column in the result. Default `"mean"`.

- band:

  Band of the value `raster` to summarise (1-based). Default 1.

- zone_band:

  Band of a raster `zones` holding the zone ids. Default 1.

- zone_field:

  For an `sf` `zones` layer, the column giving each polygon's zone id.
  Default `NULL` uses the polygon row index `1:n`.

- na.rm:

  If `TRUE` (default) skip nodata pixels; if `FALSE` let a nodata pixel
  propagate `NA` to its zone's statistics.

## Value

A data.frame with a `zone` column (sorted) followed by one column per
`fun`, one row per zone.

## Details

`sd` is derived from the streamed moments
(`sqrt((sum2 - sum^2 / n) / (n - 1))`), so it needs no second pass. With
`na.rm = TRUE` (the default) nodata pixels are skipped; with
`na.rm = FALSE` any nodata pixel in a zone makes that zone's
`sum`/`mean`/`min`/`max`/`sd` `NA`, matching the resident behaviour.
`count` always reports the number of non-nodata cells in the zone.

Polygon zones assign each pixel centre to a polygon natively on the GEOS
C API (the polygons parsed once into a spatial index, every strip's
centres located in C), so sf is touched only to read the polygons in;
geographic polygons with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html))
keep the sf point-in-polygon path. Raster zones are fully sf-free. The
zone raster must share the value raster's dimensions and geotransform.

## See also

[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
to build a value raster from streamed points,
[`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
to open the inputs.

## Examples

``` r
# A value raster and an aligned 2x2-block zone raster on a 4x4 grid.
vals <- matrix(1:16, 4, 4, byrow = TRUE)
zone <- matrix(c(1, 1, 2, 2, 1, 1, 2, 2,
                 3, 3, 4, 4, 3, 3, 4, 4), 4, 4, byrow = TRUE)
fv <- tempfile(fileext = ".vec"); fz <- tempfile(fileext = ".vec")
vec_write_raster(vals, fv, dtype = "f64", extent = c(0, 0, 4, 4))
vec_write_raster(zone, fz, dtype = "f64", extent = c(0, 0, 4, 4))

zonal(fv, fz, fun = c("mean", "sum", "count"))
unlink(c(fv, fz))
```
