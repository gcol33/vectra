# Vectorise a raster into polygons

Converts a `.vec` raster into polygon features, the inverse of
[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md).
The raster is read one tile-row strip at a time; within each strip
equal-valued cells along a row collapse to a rectangle, and (with
`dissolve = TRUE`) the rectangles are then merged by value through
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
so each distinct value becomes a single polygon spanning the whole
raster. The result is a lazy `vectra_node` carrying a value column and
hex-WKB geometry.

## Usage

``` r
polygonize(
  x,
  band = 1L,
  dissolve = TRUE,
  na_rm = TRUE,
  values = "value",
  crs = NA,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_raster` (from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md))
  or a path to a `.vec` raster.

- band:

  Band to vectorise (1-based). Default 1.

- dissolve:

  If `TRUE` (default) merge cells of equal value into one polygon per
  value; if `FALSE` emit one square polygon per cell.

- na_rm:

  Drop nodata cells (`TRUE`, default) or vectorise them as a value.

- values:

  Name of the output value column. Default `"value"`.

- crs:

  Coordinate reference system recorded on the node. Defaults to the
  raster's EPSG, else unknown.

- flush_rows:

  Rows buffered before a spill flush. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` with the value column and a hex-WKB `geometry` column,
materialise it with
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Details

Extraction is the *monoid fold* tier (one strip at a time); the by-value
dissolve rides the *sort / partition* tier of
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md),
localising each value to a shard before the union. Geometry assembly is
delegated to sf (an optional dependency).

## See also

[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
for the inverse,
[`contours()`](https://gillescolling.com/vectra/reference/contours.md)
for iso-lines,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialise as `sf`.

## Examples

``` r
m <- matrix(c(1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3), 4, 4,
            byrow = TRUE)
f <- tempfile(fileext = ".vec")
vec_write_raster(m, f, dtype = "f64", extent = c(0, 0, 4, 4))

polys <- polygonize(f)
collect_sf(polys)
unlink(f)
```
