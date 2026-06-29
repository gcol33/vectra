# Build a routable network graph from a line layer

Builds the node-edge graph a shortest-path query needs: nodes at line
endpoints, edges weighted by geometry length or a cost column. The graph
is held resident in an external pointer and passed to
[`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md)
and
[`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md),
which stream origin (and destination) batches past it. This is the
network counterpart of a resident `sf` `y` in
[`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md):
the graph is the resident budget (bounded by the network size), while
the query side scales by streaming.

## Usage

``` r
spatial_network(
  lines,
  weight = NULL,
  directed = FALSE,
  direction = NULL,
  weight_to = NULL,
  tolerance = 0,
  geom = "geometry",
  crs = NA
)
```

## Arguments

- lines:

  An `sf` or `sfc` object of (multi)linestrings, or a `vectra_node`
  carrying a hex-WKB line geometry column (it is materialised).

- weight:

  Name of a column in `lines` holding each edge's traversal cost. `NULL`
  (default) uses the geometry length
  ([`sf::st_length()`](https://r-spatial.github.io/sf/reference/geos_measures.html)).

- directed:

  If `TRUE`, edges are one-way (by `direction`, or the digitised
  from-\>to direction). `FALSE` (default) makes every edge two-way.

- direction:

  Name of a column of one-way codes, used when `directed`: `"B"`
  two-way, `"FT"` from-\>to only, `"TF"` to-\>from only, `"N"` closed
  (the `+`/`-`/`0` sign convention is also accepted). `NULL` uses `"FT"`
  for every line, or `"B"` when `weight_to` is given.

- weight_to:

  Name of a column holding the reverse-direction cost on a two-way edge
  of a directed graph. `NULL` reuses `weight`.

- tolerance:

  Endpoints within this distance (CRS units) are snapped to one node.
  `0` (default) joins only exactly-coincident endpoints.

- geom, crs:

  Geometry column name and CRS, as in
  [`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md).
  The CRS defaults to the one `lines` carries.

## Value

A `vectra_network` object: the resident graph (an external pointer plus
the node coordinates, edge table, and source-line geometry needed to
snap queries and rebuild routes). Print it to see the node, edge, and
connected-component counts.

## Details

Endpoints within `tolerance` of each other are snapped to a single node,
so a layer whose touching lines do not share exactly-equal endpoint
coordinates still connects. The input must be (multi)linestrings; a
`MULTILINESTRING` is split into its parts, each becoming one edge with
the source attributes. Lines are treated as already split at their
junctions (the usual road-network convention); two lines that merely
cross in their interiors are not connected unless they share an
endpoint. The graph and the Dijkstra solver are native C; sf (an
optional dependency, Suggests) supplies only the geometry.

## See also

[`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md)
for shortest paths and origin-destination costs,
[`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)
for reachability and isochrones.

## Examples

``` r
# A small grid of streets.
mk <- function(x1, y1, x2, y2)
  sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
streets <- sf::st_sfc(
  mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(0, 0, 0, 1),
  mk(0, 1, 1, 1), mk(1, 0, 1, 1), mk(1, 1, 2, 1), mk(2, 0, 2, 1))
net <- spatial_network(streets)
net
```
