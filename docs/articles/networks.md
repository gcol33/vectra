# Network analysis

A line layer becomes a routable graph when its segments are joined at
their endpoints and weighted by a traversal cost. vectra builds that
graph once and holds it resident in a `vectra_network` object, the
network counterpart of the small resident `sf` layer the streaming verbs
compare against. Origins then stream past the resident graph: each batch
of start points is routed in native C, so a national set of origins
flows past a fixed memory budget while the graph stays in memory.

The graph and the shortest-path solver are pure C, a binary-heap
Dijkstra over a compressed adjacency, with no `igraph` dependency. The
solver parallelises over a batch of origins with OpenMP.

``` r

library(vectra)
library(sf)
```

## Building a network

[`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
takes a line layer and returns a `vectra_network`. Endpoints are snapped
to shared nodes (exactly coincident by default, or within `tolerance`
CRS units), and each edge is weighted by its geometry length unless a
`weight` column names another cost.

The examples use a small grid of unit streets.

``` r

mk <- function(x1, y1, x2, y2)
  st_linestring(rbind(c(x1, y1), c(x2, y2)))

streets <- st_sfc(
  mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(2, 0, 3, 0),   # bottom row
  mk(0, 1, 1, 1), mk(1, 1, 2, 1), mk(2, 1, 3, 1),   # middle row
  mk(0, 2, 1, 2), mk(1, 2, 2, 2), mk(2, 2, 3, 2),   # top row
  mk(0, 0, 0, 1), mk(0, 1, 0, 2),                   # left verticals
  mk(1, 0, 1, 1), mk(1, 1, 1, 2),
  mk(2, 0, 2, 1), mk(2, 1, 2, 2),
  mk(3, 0, 3, 1), mk(3, 1, 3, 2))                   # right verticals

net <- spatial_network(streets)
net
#> <vectra_network>
#>   nodes:      12
#>   edges:      34 (undirected)
#>   components: 1
```

## Shortest routes

[`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md)
streams a layer of origin points past the network and returns the
shortest path from each origin to one or more destinations. With
`geometry = FALSE` it returns only the cost, so a set of destinations
per origin is an origin-destination cost matrix in long form.

``` r

f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = 1L, x = 0, y = 0), f)

dest <- st_sfc(st_point(c(3, 2)))

tbl(f) |>
  spatial_route(net, to = dest, coords = c("x", "y"), geometry = FALSE) |>
  collect()
#>   id x y destination cost
#> 1  1 0 0           1    5
```

The cost is the five unit steps from the bottom-left corner to the
top-right. With `geometry = TRUE` (the default) each row also carries
the route line, ready to materialise with
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md):

``` r

tbl(f) |>
  spatial_route(net, to = dest, coords = c("x", "y")) |>
  collect_sf()
#> Simple feature collection with 1 feature and 5 fields
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 3 ymax: 2
#> CRS:           NA
#>   id x y destination cost                       geometry
#> 1  1 0 0           1    5 LINESTRING (0 0, 1 0, 1 1, ...
```

Several destinations, named through `to_id`, give one row per origin and
destination, the cost matrix in long form:

``` r

g <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = 1:2, x = c(0, 3), y = c(0, 0)), g)

dests <- st_sf(
  name = c("top_left", "top_right"),
  geometry = st_sfc(st_point(c(0, 2)), st_point(c(3, 2))))

tbl(g) |>
  spatial_route(net, to = dests, to_id = "name", geometry = FALSE,
                coords = c("x", "y")) |>
  collect()
#>   id x y destination cost
#> 1  1 0 0    top_left    2
#> 2  1 0 0   top_right    5
#> 3  2 3 0    top_left    5
#> 4  2 3 0   top_right    2
```

Each origin carries its own attributes (`id` here) onto every route row.
Unreachable pairs return an infinite cost rather than dropping the row.

## Service areas and isochrones

[`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)
returns, per origin, the part of the network reachable within a cost
budget. A vector of budgets returns nested travel-cost bands, one row
per origin and band. The `output` argument selects the shape: the
reachable nodes as a multipoint, the reachable edges as lines, or their
convex hull as a service-area polygon.

``` r

tbl(f) |>
  spatial_service_area(net, cost = c(1, 2), output = "nodes",
                       coords = c("x", "y")) |>
  collect_sf()
#> Simple feature collection with 2 features and 4 fields
#> Geometry type: MULTIPOINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 2
#> CRS:           NA
#>   id x y band                       geometry
#> 1  1 0 0    1 MULTIPOINT ((0 0), (1 0), (...
#> 2  1 0 0    2 MULTIPOINT ((0 0), (1 0), (...
```

The first band reaches the start node and its two immediate neighbours;
the second band reaches everything within two steps. Asking for
`output = "polygon"` wraps each band in its hull, the usual drive-time
catchment:

``` r

tbl(f) |>
  spatial_service_area(net, cost = 2, output = "polygon",
                       coords = c("x", "y")) |>
  collect_sf() |>
  st_area()
#> [1] 2
```

## Weighted and directed networks

By default every edge is two-way and weighted by length. A `weight`
column sets a different traversal cost (a travel time, a toll), and
`directed = TRUE` makes edges one-way. The `direction` column then
carries one-way codes (`"B"` two-way, `"FT"` along the digitised
direction, `"TF"` against it, `"N"` closed), and `weight_to` gives the
reverse cost on a two-way edge.

``` r

streets_df <- st_sf(
  cost = runif(length(streets), 1, 3),
  geometry = streets)

wnet <- spatial_network(streets_df, weight = "cost")

tbl(f) |>
  spatial_route(wnet, to = dest, coords = c("x", "y"), geometry = FALSE) |>
  collect()
#>   id x y destination     cost
#> 1  1 0 0           1 7.266044
```

The route now minimises the summed `cost` weight instead of the step
count.

## The streaming model

The graph is the resident budget: it is built once and held in the
`vectra_network` object for the life of the queries against it. The
origin layer is what streams. Each batch of origins is solved against
the resident graph with one Dijkstra per origin, run in parallel across
the batch, so the query side scales by streaming while memory tracks the
graph plus one batch. This is the network analogue of the resident-`y`
pattern the [streaming spatial
verbs](https://gillescolling.com/vectra/articles/streaming-spatial.md)
use for a small locator layer.
