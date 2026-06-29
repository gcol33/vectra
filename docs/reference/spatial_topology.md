# Build the shared-edge topology of a polygon coverage

Decomposes a polygon coverage into the arcs of its planar topology: each
border is returned once, tagged with the polygons on either side. Where
the raw boundaries of adjacent polygons each carry their own copy of a
shared edge, this nodes the unioned boundaries so a shared border is a
single arc carrying the identifiers of both neighbours – the "build
topology" of a GIS, and the adjacency a dissolve or a coverage edit
needs. An internal arc names two faces; an outer arc names one and
leaves the other side `NA`. Like
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
it rides the **partition tier**: `x` is spilled once and routed into one
disjoint shard per `by` group in a single bounded pass, and each group
is treated as an independent coverage. Peak memory is the routing budget
during the pass, then one group's geometry while its arcs are built.

## Usage

``` r
spatial_topology(
  x,
  id = NULL,
  by = NULL,
  geom = "geometry",
  crs = NA,
  face_cols = c("face1", "face2"),
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- id:

  Optional name of a column of `x` whose value identifies each polygon
  in the face columns. `NULL` (default) uses the 1-based feature order
  within the group.

- by:

  Character vector of attribute columns whose groups are each built as
  an independent coverage. `NULL` (default) treats the whole layer as
  one coverage.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- face_cols:

  Length-2 character vector naming the two output columns that hold the
  identifiers of the polygons on either side of each arc. Default
  `c("face1", "face2")`.

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` of one row per arc – the arc geometry plus the two
face-identifier columns (and any `by` columns) – carrying the input CRS
and backed by temporary `.vtr` spills removed when the node is
garbage-collected.

## Details

Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; the noding is
sf/GEOS and expects projected or unprojected planar data. The sf package
is an optional dependency (Suggests).

## See also

[`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md)
to rebuild faces from arcs,
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
to merge geometries by group,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
p1 <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
p2 <- sf::st_polygon(list(rbind(c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = c("a", "b"),
  geometry = sf::st_as_binary(sf::st_sfc(p1, p2), hex = TRUE)
), f)

# The shared edge appears once, tagged with both neighbours.
tbl(f) |> spatial_topology(id = "id") |> collect()
unlink(f)
```
