# Extract contour iso-lines from a streamed raster

Traces contour lines at one or more levels from a `.vec` raster with
marching squares, reading the raster one tile-row strip at a time (each
strip expanded by one row so a cell straddling the strip boundary is
traced once). Each strip contributes line segments, which are
accumulated into a lazy `vectra_node` carrying a `level` column and
hex-WKB geometry. With `merge = TRUE` the segments of each level are
joined into continuous lines.

## Usage

``` r
contours(x, levels, band = 1L, merge = TRUE, crs = NA, flush_rows = NULL)
```

## Arguments

- x:

  A `vectra_raster` (from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md))
  or a path to a `.vec` raster.

- levels:

  Numeric vector of contour levels to trace.

- band:

  Band to contour (1-based). Default 1.

- merge:

  If `TRUE` (default) join each level's segments into continuous lines
  with
  [`sf::st_line_merge()`](https://r-spatial.github.io/sf/reference/geos_unary.html);
  if `FALSE` return the raw per-cell segments.

- crs:

  Coordinate reference system recorded on the node. Defaults to the
  raster's EPSG, else unknown.

- flush_rows:

  Rows buffered before a spill flush. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` with a `level` column and a hex-WKB `geometry` column,
materialise it with
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Details

Extraction is the *sort / partition* tier of the spatial toolbox:
bounded to one haloed strip at a time. The optional final merge collects
the segment set, which is small relative to the raster, and joins it per
level; this is the small all-to-all step on the output, not on the grid.
Geometry assembly and the merge are delegated to sf (an optional
dependency).

## See also

[`polygonize()`](https://gillescolling.com/vectra/reference/polygonize.md)
for area features,
[`terrain()`](https://gillescolling.com/vectra/reference/terrain.md) for
the DEM derivatives contours often accompany,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialise as `sf`.

## Examples

``` r
z <- outer(1:20, 1:20, function(r, c) r + c)
f <- tempfile(fileext = ".vec")
vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 20, 20))

iso <- contours(f, levels = c(15, 25, 35))
collect_sf(iso)
unlink(f)
```
