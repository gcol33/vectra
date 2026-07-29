# Build a set-wise geometry construction, optionally per group

Constructs one geometry (or a tessellation) from a whole set of features
– the constructions a per-feature
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
cannot express because they need every feature in scope at once. Like
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
it rides the **partition tier**: `x` is spilled once and routed into one
disjoint shard per `by` group in a single bounded pass, then each
shard's geometry is combined and the construction built with sf. With no
`by`, the whole layer yields one construction. Peak memory is the
routing budget during the pass, then one group's geometry while it is
built – partition on a key whose groups fit in memory.

## Usage

``` r
spatial_construct(
  x,
  kind = .CONSTRUCT_KINDS,
  by = NULL,
  geom = "geometry",
  crs = NA,
  ratio = 0.3,
  allow_holes = FALSE,
  tolerance = 0,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- kind:

  The construction to build; one of the values above.

- by:

  Character vector of attribute columns to construct within: one
  construction (or tessellation) per distinct combination of their
  values. `NULL` (default) builds a single construction from the whole
  layer.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- ratio:

  For `kind = "concave_hull"`, the concaveness in `[0, 1]` (1 is the
  convex hull). Default `0.3`.

- allow_holes:

  For `kind = "concave_hull"`, whether the hull may contain holes.
  Default `FALSE`.

- tolerance:

  Distance tolerance for the kinds that take one (`"inscribed_circle"`,
  `"pole"`, `"voronoi"`, `"delaunay"`). `0` (default) lets the
  inscribed-circle kinds derive a tolerance from the extent and the
  tessellations use the GEOS default.

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` of the construction – one row per group for the
enclosing kinds, one row per cell for the tessellations – carrying the
`by` columns and the input CRS, backed by temporary `.vtr` spills
removed when the node is garbage-collected.

## Details

`kind` selects the construction:

- `"convex_hull"`:

  the convex hull of the set.

- `"concave_hull"`:

  the concave hull (`ratio`, `allow_holes`).

- `"envelope"`:

  the axis-aligned bounding rectangle.

- `"oriented_box"`:

  the minimum-area rotated bounding rectangle.

- `"enclosing_circle"`:

  the minimum bounding circle.

- `"inscribed_circle"`:

  the maximum inscribed circle (largest circle that fits inside the
  set's union).

- `"pole"`:

  the pole of inaccessibility – the centre of the maximum inscribed
  circle, the point inside the shape farthest from its edges.

- `"voronoi"`:

  the Voronoi tessellation, one polygon per cell.

- `"delaunay"`:

  the Delaunay triangulation, one polygon per triangle.

The enclosing kinds and `pole` emit one feature per group; `voronoi` and
`delaunay` emit one feature per cell, each carrying the group's `by`
values.

Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize. Topology is sf/GEOS throughout (an optional dependency,
Suggests); some constructions need projected coordinates.

## See also

[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
to merge a group into one feature,
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
for per-feature transforms,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize.

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
nc$band <- nc$SID74 > 5
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  band = nc$band,
  geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
), f)

# One convex hull per band.
tbl(f) |>
  spatial_construct("convex_hull", by = "band", crs = sf::st_crs(nc)) |>
  collect_sf()
unlink(f)
```
