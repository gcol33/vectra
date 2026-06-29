# Shortest paths and origin-destination costs over a network

Streams a layer of origins `x` past a resident
[`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
and, for each origin, finds the shortest path to one or more
destinations `to`. Each origin and destination is snapped to its nearest
graph node; the solver (native-C Dijkstra) returns one row per (origin,
destination) pair with the total cost and, by default, the route
geometry. With `geometry = FALSE` only the cost is returned, so a
destination set per origin yields the origin-destination cost matrix in
long form. The billion-origin stream never materialises; the graph stays
resident.

## Usage

``` r
spatial_route(
  x,
  network,
  to,
  to_id = NULL,
  geometry = TRUE,
  cost_col = "cost",
  dest_col = "destination",
  geom = "geometry",
  coords = NULL,
  crs = NA,
  out_geom = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` of origin features (point geometry, or `coords`).

- network:

  A `vectra_network` from
  [`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md).

- to:

  An `sf`/`sfc` of destination points. One destination routes every
  origin to it; several produce one row per (origin, destination).

- to_id:

  Optional column in `to` identifying each destination in the output.
  `NULL` (default) uses the 1-based destination index.

- geometry:

  If `TRUE` (default) each row carries the route line; if `FALSE` only
  the cost column (the cost-matrix form, no geometry).

- cost_col, dest_col:

  Names of the output cost and destination-identifier columns. Defaults
  `"cost"`, `"destination"`.

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

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node`: one row per (origin, destination) carrying `x`'s
attributes, the destination id, the cost, and (when `geometry = TRUE`)
the route geometry. Backed by temporary `.vtr` spills removed when the
node is garbage-collected, and carrying the network CRS.

## Details

An unreachable destination returns an infinite cost and an empty
geometry rather than dropping the row, so a cost matrix stays
rectangular. Snapping is to the nearest node, so place origins and
destinations on or near the network; costs are in the units of the
network's `weight` (or CRS length units when the graph was built from
geometry length). The sf package is an optional dependency (Suggests).

## See also

[`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
to build the graph,
[`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)
for reachability,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize routes as `sf`.

## Examples

``` r
mk <- function(x1, y1, x2, y2)
  sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
streets <- sf::st_sfc(
  mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(0, 0, 0, 1),
  mk(0, 1, 1, 1), mk(1, 0, 1, 1), mk(1, 1, 2, 1), mk(2, 0, 2, 1))
net <- spatial_network(streets)

f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = 1:2, x = c(0, 0), y = c(0, 1)), f)
dest <- sf::st_sfc(sf::st_point(c(2, 1)))

tbl(f) |>
  spatial_route(net, to = dest, coords = c("x", "y")) |>
  collect_sf()
unlink(f)
```
