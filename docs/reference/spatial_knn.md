# k nearest neighbours of a streamed layer, with distances

Streams a large left side `x` through the engine and, for each feature,
finds the `k` nearest features of a small resident layer `y`, returning
one row per (left, neighbour) pair with the neighbour's rank,
identifier, and distance. Where
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
with
[sf::st_nearest_feature](https://r-spatial.github.io/sf/reference/st_nearest_feature.html)
attaches only the single nearest match, this returns the top `k` and the
distances themselves – the nearest-`k` query and the building block of a
distance matrix. The billion-row left stream never materializes; `y`
(the candidate neighbours) stays resident.

## Usage

``` r
spatial_knn(
  x,
  y,
  k = 1L,
  geom = "geometry",
  coords = NULL,
  crs = NA,
  y_id = NULL,
  id_col = "neighbor",
  dist_col = "distance",
  rank_col = "rank",
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

- y:

  An `sf` or `sfc` object: the resident candidate-neighbour layer.

- k:

  Number of nearest neighbours to return per left feature (capped at the
  number of `y` features). Default `1`.

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

  Optional name of a column in `y` whose value identifies each neighbour
  in the output. Default `NULL` uses `y`'s 1-based row index.

- id_col, dist_col, rank_col:

  Names of the output columns holding the neighbour identifier, the
  distance, and the 1-based rank (1 = nearest). Defaults `"neighbor"`,
  `"distance"`, `"rank"`.

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

A `vectra_node` of one row per (left, neighbour) pair – `x`'s columns
(geometry included) plus the rank, neighbour identifier, and distance –
backed by temporary `.vtr` spills (removed when the node is garbage-
collected) and carrying the input CRS.

## Details

Distances are sf's
[`sf::st_distance()`](https://r-spatial.github.io/sf/reference/geos_measures.html):
planar (CRS units) on projected or unprojected planar data, great-circle
(metres) on geographic coordinates with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html)).
Each batch forms its left-by-`y` distance matrix, so `y` should be the
small side; when `y` carries no CRS it inherits the stream's. The left
geometry rides through unchanged (replicated once per neighbour). The sf
package is an optional dependency (Suggests).

## See also

[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
for a nearest-feature attribute join,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
towns <- sf::st_centroid(sf::st_geometry(nc))[1:5]
towns <- sf::st_sf(town = nc$NAME[1:5], geometry = towns)

set.seed(1)
pts <- sf::st_coordinates(sf::st_sample(nc, 100))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)

# The two nearest towns to each point, with distances.
tbl(f) |>
  spatial_knn(towns, k = 2, coords = c("x", "y"), crs = sf::st_crs(nc),
              y_id = "town") |>
  collect() |> head()
unlink(f)
```
