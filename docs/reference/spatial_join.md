# Spatial join a streamed query against a resident sf object

Streams a large left side `x` through the engine and joins each batch
against a small right side `y` held resident in memory, using an sf
binary predicate (`st_intersects` by default). This is the spatial
analogue of a hash join with the small side on the build side: the
billion-row left stream never materializes, while `y` (admin polygons,
habitat patches, ...) stays in RAM. The dominant real workload it serves
is tagging huge point sets with the polygon they fall in.

## Usage

``` r
spatial_join(
  x,
  y,
  join = NULL,
  geom = "geometry",
  coords = NULL,
  crs = NA,
  left = TRUE,
  suffix = c(".x", ".y"),
  partition = NULL,
  y_geom = NULL,
  y_coords = NULL,
  out_geom = NULL,
  flush_rows = NULL,
  ...
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- y:

  The right side of the join: an `sf` object held resident (the
  default), or – when `partition` is given – a streamed `vectra_node`.

- join:

  An sf binary predicate function, e.g.
  [sf::st_intersects](https://r-spatial.github.io/sf/reference/geos_binary_pred.html)
  (default),
  [sf::st_within](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [sf::st_contains](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [sf::st_nearest_feature](https://r-spatial.github.io/sf/reference/st_nearest_feature.html).

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

- left:

  If `TRUE` (default) keep every left row (left join); if `FALSE` keep
  only matches (inner join).

- suffix:

  Length-2 character vector disambiguating columns present on both
  sides. Default `c(".x", ".y")`.

- partition:

  Optional
  [`grid()`](https://gillescolling.com/vectra/reference/grid.md)
  specification enabling the two-sided streamed path, in which `y` is
  itself a `vectra_node`. Default `NULL` keeps the resident-`y` path.

- y_geom, y_coords:

  Geometry transport for a streamed `y` under `partition`: the name of
  `y`'s hex-WKB geometry column (`y_geom`, default the left `geom`), or
  a length-2 character vector of `y`'s coordinate columns (`y_coords`).
  Ignored without `partition`.

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

- ...:

  Further arguments passed to
  [`sf::st_join()`](https://r-spatial.github.io/sf/reference/st_join.html).

## Value

A `vectra_node` of the joined stream, backed by temporary `.vtr` spills
and carrying the left CRS.

## Details

For the recognised predicates – the topological ones (intersects,
within, contains, overlaps, covers, covered by, touches, crosses),
equals, within-distance
([sf::st_is_within_distance](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
radius passed as `dist =`), and nearest feature
([sf::st_nearest_feature](https://r-spatial.github.io/sf/reference/st_nearest_feature.html))
– on projected or unprojected planar data, the match runs natively on
the GEOS C API straight off the hex-WKB column: `y` is parsed once into
a spatial index, each batch's matches come back from C, and `y`'s
attributes are attached in R without decoding the left side to sf.
Coordinate-assembled (`coords`) point input runs natively too, building
each point in C (the emitted point geometry is built in C as well).
Geographic coordinates with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html)),
a disjoint join (whose matches are the bounding-box complement an index
cannot prune), and other extra
[`sf::st_join()`](https://r-spatial.github.io/sf/reference/st_join.html)
arguments use sf instead, preserving its semantics.

When both sides are larger than RAM, pass `partition = grid(cellsize)`
and a streamed `vectra_node` as `y`: both inputs are binned to a uniform
spatial grid, then joined one shard at a time. Each left feature is
assigned to the single grid cell of its reference point while each right
feature is replicated to every cell its bounding box overlaps, so a left
row is emitted exactly once and the result equals the resident join.
This is exact for point left geometries (the dominant case – tagging a
huge point set with the polygon it falls in) and finds, for an extended
left feature, the matches whose right bounding box overlaps the left
reference cell; choose a `cellsize` larger than the left features for an
extended-on-extended join. The partition path serves topological
predicates (intersects, within, contains, overlaps, covers, covered by).
It also serves
[sf::st_nearest_feature](https://r-spatial.github.io/sf/reference/st_nearest_feature.html):
because nearest is not local to one cell, each left feature then
searches its own cell and the eight around it, so the true nearest is
found when it lies within one cell of the left reference cell (pick a
`cellsize` at least the largest expected nearest distance). Topology and
CRS handling are sf's; vectra supplies the stream and the grid
partition.

## See also

[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
for per-feature transforms,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`,
[`offload()`](https://gillescolling.com/vectra/reference/offload.md) to
partition both-sides-huge joins.

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)

# A stream of points, stored with x/y coordinate columns.
set.seed(1)
pts <- sf::st_coordinates(sf::st_sample(nc, 200))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)

# Tag each point with the county it falls in, streaming.
tagged <- tbl(f) |>
  spatial_join(nc["NAME"], join = sf::st_intersects,
               coords = c("x", "y"), crs = sf::st_crs(nc))
head(collect(tagged))

# Both sides streamed: bin to a grid and join per shard. Here y is a
# vectra_node rather than a resident sf object.
g <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  NAME = nc$NAME,
  geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
), g)
tagged2 <- tbl(f) |>
  spatial_join(tbl(g), coords = c("x", "y"), crs = sf::st_crs(nc),
               partition = grid(0.5))
head(collect(tagged2))
unlink(c(f, g))
```
