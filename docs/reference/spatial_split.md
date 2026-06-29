# Split a streamed layer by a resident blade, or return its crossing points

Streams a large layer `x` through the engine and cuts each batch's
geometry against a small resident `blade` layer (the QGIS "split with
lines"), one batch at a time. With `extract = "pieces"` (the default)
every feature is divided where the blade crosses it – a polygon into the
faces the blade carves out, a line into the arcs between crossings – and
each piece is emitted as its own row with the source attributes copied;
a feature the blade does not cross passes through as a single piece.
With `extract = "points"` the verb instead returns, per feature, the
points where it meets the blade (the "line intersections" tool),
dropping features that do not cross.

## Usage

``` r
spatial_split(
  x,
  blade,
  extract = c("pieces", "points"),
  geom = "geometry",
  crs = NA,
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

- blade:

  An `sf` or `sfc` object whose geometry cuts the stream (typically
  lines, but any geometry whose boundary can node `x`).

- extract:

  `"pieces"` (default) to emit the split pieces, or `"points"` to emit
  the intersection points of each feature with the blade.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node`: with `extract = "pieces"`, one row per piece carrying
`x`'s attributes; with `extract = "points"`, one row per crossing
feature carrying its intersection points. Backed by temporary `.vtr`
spills (removed when the node is garbage-collected) and carrying the
input CRS.

## Details

The split is built from sf/GEOS noding and polygonization, so it expects
projected or unprojected planar data; geographic coordinates are best
projected first. The blade is dissolved to one geometry once and held
resident while the left stream flows past. Geometry travels through the
engine as hex-encoded WKB in a string column and the CRS is carried on
the returned node; when `blade` carries no CRS it inherits the stream's.
The sf package is an optional dependency (Suggests).

## See also

[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md)
to cut against a mask without dividing into pieces,
[`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
to node two polygon layers into a partition,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
sq <- sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(4, 4), c(0, 4), c(0, 0))))
blade <- sf::st_sfc(sf::st_linestring(rbind(c(2, -1), c(2, 5))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1L, geometry = sf::st_as_binary(sf::st_sfc(sq), hex = TRUE)
), f)

# Split the square into two halves along the blade.
tbl(f) |> spatial_split(blade) |> collect_sf()
unlink(f)
```
