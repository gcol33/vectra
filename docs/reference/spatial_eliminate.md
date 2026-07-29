# Merge sliver polygons into a neighbour

Cleans a polygon coverage by absorbing every feature whose area is below
`max_area` into an adjacent feature (the QGIS "Eliminate Selected
Polygons"): the sliver removal a per-feature transform cannot do,
because the target a sliver merges into is one of its neighbours, not
the sliver itself. Each small feature is joined to the neighbour it
shares the longest border with (or the largest-area neighbour, with
`into = "largest_area"`); chains of slivers collapse so a connected run
of small features flows to its single largest member, whose attribute
row survives. A small feature with no neighbour is kept unchanged, so
nothing vanishes. Like
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
it rides the **partition tier**: `x` is spilled once and routed into one
disjoint shard per `by` group in a single bounded pass, and each group
is cleaned as an independent coverage. Peak memory is the routing budget
during the pass, then one group's geometry while its slivers are merged
– partition on a key whose groups fit in memory. With no `by`, the whole
layer is one coverage.

## Usage

``` r
spatial_eliminate(
  x,
  max_area,
  by = NULL,
  into = c("longest_border", "largest_area"),
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

- max_area:

  Area threshold in CRS units squared: a feature smaller than this is a
  sliver and is merged into a neighbour. Larger values absorb more.

- by:

  Character vector of attribute columns whose groups are each cleaned as
  an independent coverage. `NULL` (default) treats the whole layer as
  one coverage.

- into:

  How to pick the neighbour a sliver merges into: `"longest_border"`
  (default) the neighbour sharing the longest boundary, or
  `"largest_area"` the neighbour with the greatest area.

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

A `vectra_node` of the cleaned coverage – one row per surviving feature,
each carrying its (largest member's) attributes and the input CRS,
backed by temporary `.vtr` spills removed when the node is
garbage-collected.

## Details

Adjacency and shared-border length are sf/GEOS
([sf::st_intersects](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[sf::st_boundary](https://r-spatial.github.io/sf/reference/geos_unary.html),
[sf::st_intersection](https://r-spatial.github.io/sf/reference/geos_binary_ops.html))
and expect projected or unprojected planar data; `max_area` is in CRS
units squared. Geometry travels through the engine as hex-encoded WKB in
a string column and the CRS is carried on the returned node. The sf
package is an optional dependency (Suggests).

## See also

[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
to merge geometries by attribute,
[`spatial_simplify()`](https://gillescolling.com/vectra/reference/spatial_simplify.md)
for coverage-preserving simplification,
[`spatial_topology()`](https://gillescolling.com/vectra/reference/spatial_topology.md)
for the shared-edge adjacency,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
big    <- sf::st_polygon(list(rbind(
  c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0))))
sliver <- sf::st_polygon(list(rbind(
  c(10, 0), c(10.3, 0), c(10.3, 10), c(10, 10), c(10, 0))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = c("keep", "sliver"),
  geometry = sf::st_as_binary(sf::st_sfc(big, sliver), hex = TRUE)
), f)

# The thin sliver is absorbed into the square it borders.
tbl(f) |> spatial_eliminate(max_area = 5) |> collect_sf()
unlink(f)
```
