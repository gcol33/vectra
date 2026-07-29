# Simplify a polygon coverage without tearing shared edges

Simplifies polygon boundaries while keeping a shared border between two
polygons identical on both sides, so adjacent polygons stay edge-matched
with no slivers or gaps – the topology-preserving simplification that a
per-feature `spatial_map(~ sf::st_simplify(.x))` cannot give, because it
simplifies each polygon's copy of a shared border independently. The
boundaries of each group are unioned so a shared border is one line,
noded into arcs at every junction, each arc simplified once (its
junction endpoints pinned), and the arcs re-polygonized; each resulting
face inherits the attributes of the source polygon containing it. Like
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
it rides the **partition tier**: `x` is spilled once and routed into one
disjoint shard per `by` group in a single bounded pass, and each group
is simplified as an independent coverage. Peak memory is the routing
budget during the pass, then one group's geometry while it is simplified
– partition on a key whose groups fit in memory. With no `by`, the whole
layer is one coverage.

## Usage

``` r
spatial_simplify(
  x,
  tolerance,
  by = NULL,
  geom = "geometry",
  crs = NA,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- tolerance:

  Distance tolerance for the boundary simplification, in CRS units:
  vertices that deviate less than this from the simplified line are
  dropped. Larger values simplify more.

- by:

  Character vector of attribute columns whose groups are each simplified
  as an independent coverage. `NULL` (default) treats the whole layer as
  one coverage.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` of the simplified polygons, each carrying its source
feature's attributes and the input CRS, backed by temporary `.vtr`
spills removed when the node is garbage-collected.

## Details

The simplification is topology-preserving Douglas-Peucker
(`dTolerance = tolerance`) on the noded boundary arcs. Geometry travels
through the engine as hex-encoded WKB in a string column and the CRS is
carried on the returned node; the noding is sf/GEOS and expects
projected or unprojected planar data. The sf package is an optional
dependency (Suggests).

## See also

[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
with `~ sf::st_simplify(.x)` for independent per-feature simplification,
[`spatial_smooth()`](https://gillescolling.com/vectra/reference/spatial_smooth.md)
for Chaikin corner-rounding,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
p1 <- sf::st_polygon(list(rbind(
  c(0, 0), c(1, 0), c(1, 0.5), c(1, 1), c(0, 1), c(0, 0))))
p2 <- sf::st_polygon(list(rbind(
  c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0.5), c(1, 0))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = c("a", "b"),
  geometry = sf::st_as_binary(sf::st_sfc(p1, p2), hex = TRUE)
), f)

# The shared edge is simplified once, so the two polygons stay edge-matched.
tbl(f) |> spatial_simplify(tolerance = 0.6) |> collect_sf()
unlink(f)
```
