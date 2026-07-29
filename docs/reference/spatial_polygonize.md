# Build polygonal faces from a line network

Forms the polygons enclosed by a set of lines (the QGIS "Polygonize",
GEOS `Polygonize`): the inverse of taking polygon boundaries. The lines
of each group are unioned and noded so every crossing becomes a shared
vertex, then the faces of that planar arrangement are returned, one per
row. A pile of lines that does not close any area yields no faces. Like
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
and
[`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
it rides the **partition tier**: `x` is spilled once and routed into one
disjoint shard per `by` group in a single bounded pass, then each
group's lines are polygonized together. Peak memory is the routing
budget during the pass, then one group's geometry while its faces are
built – partition on a key whose groups fit in memory. With no `by`, the
whole layer yields one set of faces.

## Usage

``` r
spatial_polygonize(
  x,
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

- by:

  Character vector of attribute columns to polygonize within: one set of
  faces per distinct combination of their values. `NULL` (default)
  polygonizes the whole layer at once.

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

A `vectra_node` of one row per face, carrying the `by` columns and the
input CRS, backed by temporary `.vtr` spills removed when the node is
garbage-collected.

## Details

Each face is new geometry built from the whole group, so it carries the
`by` columns only, not the attributes of any single source line.
Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; the noding is
sf/GEOS and expects projected or unprojected planar data. The sf package
is an optional dependency (Suggests).

## See also

[`spatial_split()`](https://gillescolling.com/vectra/reference/spatial_split.md)
to cut existing polygons by a blade,
[`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
for hulls and tessellations,
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
to merge geometries by group,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
grid <- sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(2, 0))),
  sf::st_linestring(rbind(c(0, 1), c(2, 1))),
  sf::st_linestring(rbind(c(0, 2), c(2, 2))),
  sf::st_linestring(rbind(c(0, 0), c(0, 2))),
  sf::st_linestring(rbind(c(1, 0), c(1, 2))),
  sf::st_linestring(rbind(c(2, 0), c(2, 2))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  geometry = sf::st_as_binary(grid, hex = TRUE)
), f)

# The four unit cells enclosed by the grid of lines.
tbl(f) |> spatial_polygonize() |> collect_sf()
unlink(f)
```
