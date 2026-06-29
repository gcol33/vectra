# Service areas and isochrones over a network

Streams a layer of origins `x` past a resident
[`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
and, for each origin, finds every part of the network reachable within a
cost budget - the service area, or, with several budgets, nested
travel-cost isochrone bands. Each origin is snapped to its nearest graph
node and a budget-bounded Dijkstra (native C) collects the reachable
nodes; the reachable set is returned as the convex hull
(`output = "polygon"`, the generalised service area), the reachable
edges (`"lines"`), or the reachable nodes (`"nodes"`).

## Usage

``` r
spatial_service_area(
  x,
  network,
  cost,
  output = c("polygon", "lines", "nodes"),
  band_col = "band",
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

- cost:

  A cost budget (scalar), or several budgets for nested isochrone bands
  (e.g. `c(5, 10, 15)`).

- output:

  `"polygon"` (default) for the convex hull of the reachable nodes,
  `"lines"` for the reachable edges, or `"nodes"` for the reachable
  nodes as a multipoint.

- band_col:

  Name of the output column holding each row's budget. Default `"band"`.

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

A `vectra_node`: one row per (origin, band) carrying `x`'s attributes,
the band value, and the service-area geometry. Backed by temporary
`.vtr` spills removed when the node is garbage-collected, and carrying
the network CRS.

## Details

A vector `cost` returns one row per (origin, band), each band the area
reachable within that budget, so the rows nest from the smallest budget
out. Costs are in the network's `weight` units. The convex-hull polygon
is a generalisation; use `output = "lines"` for the exact reachable
network. The sf package is an optional dependency (Suggests).

## See also

[`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
to build the graph,
[`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md)
for shortest paths,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
mk <- function(x1, y1, x2, y2)
  sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
streets <- sf::st_sfc(
  mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(0, 0, 0, 1),
  mk(0, 1, 1, 1), mk(1, 0, 1, 1), mk(1, 1, 2, 1), mk(2, 0, 2, 1))
net <- spatial_network(streets)

f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = 1L, x = 0, y = 0), f)

tbl(f) |>
  spatial_service_area(net, cost = c(1, 2), output = "lines",
                       coords = c("x", "y")) |>
  collect_sf()
unlink(f)
```
