# Trace the centerline (medial axis) of streamed polygons

Approximates the centerline of every polygon in a streamed layer, one
batch at a time – the medial axis a per-feature transform such as a
buffer cannot produce. Each polygon's boundary is densified and its
Voronoi diagram taken; the Voronoi edges that fall inside the polygon
trace the points equidistant from two stretches of boundary, which is
its skeleton, and they are merged into maximal lines. This is the usual
approximation for river or road centerlines from a filled shape; `prune`
drops the short branches that the skeleton grows toward convex corners.
A non-polygon geometry passes through unchanged.

## Usage

``` r
spatial_centerline(
  x,
  density = NULL,
  prune = 0,
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

- density:

  Boundary sampling spacing in CRS units: the polygon outline is
  densified to at most this vertex spacing before the Voronoi diagram is
  built. `NULL` (default) uses one-hundredth of each polygon's
  bounding-box diagonal.

- prune:

  Drop centerline branches shorter than this length (CRS units),
  removing the short spurs the skeleton grows toward convex corners. `0`
  (default) keeps every branch.

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
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` of the centerlines, each carrying its source polygon's
attributes (replicated if the centerline is several lines) and the input
CRS, backed by temporary `.vtr` spills removed when the node is
garbage-collected.

## Details

The centerline is an approximation whose detail is set by `density` (the
boundary sampling spacing): finer sampling traces the axis more closely
at more cost. Geometry travels through the engine as hex-encoded WKB in
a string column and the CRS is carried on the returned node; the Voronoi
construction is sf/GEOS and expects projected or unprojected planar
data. The sf package is an optional dependency (Suggests).

## See also

[`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
with `kind = "pole"` for the single deepest interior point,
[`spatial_simplify()`](https://gillescolling.com/vectra/reference/spatial_simplify.md)
to simplify a coverage,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
road <- sf::st_polygon(list(rbind(
  c(0, 0), c(10, 0), c(10, 2), c(0, 2), c(0, 0))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  geometry = sf::st_as_binary(sf::st_sfc(road), hex = TRUE)
), f)

# The centerline runs down the middle of the strip.
tbl(f) |> spatial_centerline(density = 0.25, prune = 0.5) |> collect_sf()
unlink(f)
```
