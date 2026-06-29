# Locate streamed points along a resident line layer

Streams a large point layer `x` through the engine and, for each point,
finds the nearest line of a small resident `line` layer and where the
point falls along it – linear referencing
([`sf::st_line_project`](https://r-spatial.github.io/sf/reference/st_line_project_point.html)).
Each point gets the identifier of its nearest line, the **measure**
(distance along that line from its start to the point's projection), and
the perpendicular distance to the line. With `snap = TRUE` the point
geometry is moved onto the line at that measure. This is the two-layer
companion to a per-feature
[`sf::st_line_interpolate`](https://r-spatial.github.io/sf/reference/st_line_project_point.html),
which goes the other way (a measure back to a point); the billion-row
point stream never materializes, while `line` (the reference network)
stays resident.

## Usage

``` r
spatial_locate(
  x,
  line,
  geom = "geometry",
  coords = NULL,
  crs = NA,
  y_id = NULL,
  id_col = "line",
  measure_col = "measure",
  dist_col = "distance",
  snap = FALSE,
  out_geom = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- line:

  An `sf` or `sfc` object of the reference lines (the resident layer).

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- coords:

  Optional length-2 character vector naming the x and y coordinate
  columns to assemble point geometry from (e.g. `c("x", "y")`), for
  inputs such as
  [`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
  output. The coordinate columns are retained.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- y_id:

  Optional name of a column in `line` whose value identifies the matched
  line in the output. Default `NULL` uses `line`'s 1-based row index.

- id_col, measure_col, dist_col:

  Names of the output columns holding the matched-line identifier, the
  measure along the line, and the perpendicular distance. Defaults
  `"line"`, `"measure"`, `"distance"`.

- snap:

  If `TRUE`, replace each point's geometry with its projection onto the
  nearest line. Default `FALSE` keeps the original points.

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` of `x`'s rows – geometry included (or snapped onto the
line) – plus the matched-line identifier, the measure, and the
perpendicular distance, backed by temporary `.vtr` spills (removed when
the node is garbage-collected) and carrying the input CRS.

## Details

Nearest line and distance are sf's
[sf::st_nearest_feature](https://r-spatial.github.io/sf/reference/st_nearest_feature.html)
and
[sf::st_distance](https://r-spatial.github.io/sf/reference/geos_measures.html):
planar (CRS units) on projected or unprojected planar data, great-circle
(metres) on geographic coordinates with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html)).
Points arrive either as a hex-WKB geometry column (`geom`) or as two
coordinate columns (`coords`). The sf package is an optional dependency
(Suggests).

## See also

[`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md)
for nearest neighbours with distances,
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
for a nearest-feature attribute join,
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
with `~ sf::st_line_interpolate(line, .x$m)` for the inverse,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
line <- sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(10, 0))),
  sf::st_linestring(rbind(c(0, 5), c(0, 15))))
line <- sf::st_sf(road = c("main", "side"), geometry = line)
pts  <- data.frame(id = 1:2, x = c(3, 1), y = c(1, 9))
f <- tempfile(fileext = ".vtr")
write_vtr(pts, f)

# Each point's position along its nearest road.
tbl(f) |>
  spatial_locate(line, coords = c("x", "y"), y_id = "road") |>
  collect()
unlink(f)
```
