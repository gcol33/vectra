# Coverage and topology

The per-feature verbs in [Streaming spatial
operations](https://gillescolling.com/vectra/articles/streaming-spatial.md)
treat each geometry on its own. A second family of verbs reads a whole
feature set at once: the constructions and topology operations that need
every feature in a group together, not one row at a time. These ride a
shared partition tier. A `by` argument routes the layer into one shard
per group, each shard is processed as an independent coverage, and the
shards stream so the resident budget is one group rather than the whole
layer.

This vignette covers building polygons from lines, cleaning a polygon
coverage, decomposing geometry into its parts, set-wise constructions,
and linear referencing. Every block runs on the optional
[sf](https://r-spatial.github.io/sf/) package.

``` r

library(vectra)
library(sf)
```

## From lines to polygons

[`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md)
builds the polygonal faces enclosed by a line network, the inverse of
taking polygon boundaries. A group’s lines are unioned and noded, then
the faces of that arrangement are returned, one per row.

``` r

grid <- st_sfc(
  st_linestring(rbind(c(0, 0), c(2, 0))),
  st_linestring(rbind(c(0, 1), c(2, 1))),
  st_linestring(rbind(c(0, 2), c(2, 2))),
  st_linestring(rbind(c(0, 0), c(0, 2))),
  st_linestring(rbind(c(1, 0), c(1, 2))),
  st_linestring(rbind(c(2, 0), c(2, 2))))

f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(geometry = st_as_binary(grid, hex = TRUE)), f)

tbl(f) |> spatial_polygonize() |> collect_sf()
#> Simple feature collection with 4 features and 0 fields
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 2
#> CRS:           NA
#>                         geometry
#> 1 POLYGON ((1 0, 0 0, 0 1, 1 ...
#> 2 POLYGON ((2 0, 1 0, 1 1, 2 ...
#> 3 POLYGON ((1 1, 0 1, 0 2, 1 ...
#> 4 POLYGON ((2 1, 1 1, 1 2, 2 ...
```

[`spatial_line_merge()`](https://gillescolling.com/vectra/reference/spatial_line_merge.md)
is the line counterpart of a dissolve: it sews segments that meet end to
end into maximal linestrings, one row per chain. Segments meeting at a
junction of degree greater than two stay separate.

``` r

seg <- st_sfc(
  st_linestring(rbind(c(0, 0), c(1, 0))),
  st_linestring(rbind(c(1, 0), c(2, 0))),
  st_linestring(rbind(c(2, 0), c(3, 0))))

g <- tempfile(fileext = ".vtr")
write_vtr(data.frame(geometry = st_as_binary(seg, hex = TRUE)), g)

tbl(g) |> spatial_line_merge() |> collect_sf()
#> Simple feature collection with 1 feature and 0 fields
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 3 ymax: 0
#> CRS:           NA
#>                         geometry
#> 1 LINESTRING (0 0, 1 0, 2 0, ...
```

## Cleaning a coverage

Real coverages arrive with jittered shared borders, sub-pixel slivers,
and more detail than a map needs. Three verbs clean them while keeping
adjacent polygons edge-matched.

[`spatial_snap_grid()`](https://gillescolling.com/vectra/reference/spatial_snap_grid.md)
rounds coordinates to a regular grid and repairs the result, one batch
at a time. It is the fixed-precision snap-rounding that
[`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
applies internally, exposed as a standalone verb, so a layer can be
pre-noded to a common precision without running a full overlay.

``` r

p <- st_polygon(list(rbind(c(0.04, 0.03), c(1.02, 0.01),
                           c(0.98, 1.03), c(0.01, 0.97), c(0.04, 0.03))))
h <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = 1L, geometry = st_as_binary(st_sfc(p), hex = TRUE)), h)

tbl(h) |> spatial_snap_grid(0.1) |> collect_sf()
#> Simple feature collection with 1 feature and 1 field
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 1 ymax: 1
#> CRS:           NA
#>   id                       geometry
#> 1  1 POLYGON ((0 1, 1 1, 1 0, 0 ...
```

[`spatial_snap()`](https://gillescolling.com/vectra/reference/spatial_snap.md)
instead pulls a streamed layer onto a resident reference layer: vertices
and edges within `tolerance` of `y` are moved onto it, so two layers
that should share a boundary line up exactly.

``` r

ref  <- st_sfc(st_linestring(rbind(c(0, 0), c(10, 0))))
line <- st_linestring(rbind(c(0, 0.2), c(5, 0.1), c(10, 0.2)))

sn <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = 1L, geometry = st_as_binary(st_sfc(line), hex = TRUE)), sn)

tbl(sn) |> spatial_snap(ref, tolerance = 0.5) |> collect_sf()
#> Simple feature collection with 1 feature and 1 field
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 10 ymax: 0.1
#> CRS:           NA
#>   id                      geometry
#> 1  1 LINESTRING (0 0, 5 0.1, 10 0)
```

[`spatial_eliminate()`](https://gillescolling.com/vectra/reference/spatial_eliminate.md)
cleans a polygon coverage by absorbing every feature smaller than
`max_area` into a neighbour, the QGIS “Eliminate”. Each sliver joins the
neighbour it shares the longest border with, or the largest-area
neighbour with `into = "largest_area"`. An area-rooted union-find
collapses chains of slivers so a connected run flows to its single
largest member, whose attributes survive.

``` r

big    <- st_polygon(list(rbind(
  c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0))))
sliver <- st_polygon(list(rbind(
  c(10, 0), c(10.3, 0), c(10.3, 10), c(10, 10), c(10, 0))))

e <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = c("keep", "sliver"),
  geometry = st_as_binary(st_sfc(big, sliver), hex = TRUE)), e)

tbl(e) |> spatial_eliminate(max_area = 5) |> collect_sf()
#> Simple feature collection with 1 feature and 1 field
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 10.3 ymax: 10
#> CRS:           NA
#>     id                       geometry
#> 1 keep POLYGON ((0 0, 0 10, 10 10,...
```

The thin sliver is gone, absorbed into the square it bordered, and the
survivor keeps the `keep` attributes.

[`spatial_simplify()`](https://gillescolling.com/vectra/reference/spatial_simplify.md)
simplifies a polygon coverage without tearing shared edges. Boundaries
are unioned so a shared border is one line, noded into arcs, each arc
simplified once with its junction endpoints pinned, then re-polygonized.
Adjacent polygons stay edge-matched with no slivers. This is the
topology-preserving simplification a per-feature
`spatial_map(~ sf::st_simplify())` cannot give, because that simplifies
each polygon’s copy of a shared border independently.

``` r

p1 <- st_polygon(list(rbind(
  c(0, 0), c(1, 0), c(1, 0.5), c(1, 1), c(0, 1), c(0, 0))))
p2 <- st_polygon(list(rbind(
  c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0.5), c(1, 0))))

s <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = c("a", "b"),
  geometry = st_as_binary(st_sfc(p1, p2), hex = TRUE)), s)

tbl(s) |> spatial_simplify(tolerance = 0.6) |> collect_sf()
#> Simple feature collection with 2 features and 1 field
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 1
#> CRS:           NA
#>   id                       geometry
#> 1  a POLYGON ((1 0, 0 0, 0 1, 1 ...
#> 2  b POLYGON ((1 0, 1 0.5, 1 1, ...
```

## Decomposing geometry

[`spatial_explode()`](https://gillescolling.com/vectra/reference/spatial_explode.md)
splits every multipart geometry into its single-part components, a
`MULTIPOLYGON` into one row per polygon and so on, copying the source
attributes onto each part. An optional `part` argument names a 1-based
part-index column. It is the streaming “multipart to singleparts” tool
and the inverse of
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md).

``` r

mp <- st_multipolygon(list(
  list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))),
  list(rbind(c(2, 2), c(3, 2), c(3, 3), c(2, 3), c(2, 2)))))

m <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1L, geometry = st_as_binary(st_sfc(mp), hex = TRUE)), m)

tbl(m) |> spatial_explode(part = "part_id") |> collect_sf()
#> Simple feature collection with 2 features and 2 fields
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 3 ymax: 3
#> CRS:           NA
#>   id part_id                       geometry
#> 1  1       1 POLYGON ((0 0, 1 0, 1 1, 0 ...
#> 2  1       2 POLYGON ((2 2, 3 2, 3 3, 2 ...
```

[`spatial_topology()`](https://gillescolling.com/vectra/reference/spatial_topology.md)
decomposes a polygon coverage into the arcs of its planar topology. The
unioned boundaries are noded so a shared border becomes one arc, tagged
with the identifiers of the (up to two) polygons on either side: two for
an internal shared edge, one for an outer edge. It is the inverse of
[`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md).

``` r

q1 <- st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
q2 <- st_polygon(list(rbind(c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0))))

tp <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = c("a", "b"),
  geometry = st_as_binary(st_sfc(q1, q2), hex = TRUE)), tp)

tbl(tp) |> spatial_topology(id = "id") |> collect()
#>   face1 face2
#> 1     a  <NA>
#> 2     a     b
#> 3     a  <NA>
#> 4     b  <NA>
#>                                                                                                                                             geometry
#> 1                                                                 01020000000200000000000000000000000000000000000000000000000000f03f0000000000000000
#> 2                                                                 010200000002000000000000000000f03f0000000000000000000000000000f03f000000000000f03f
#> 3                                 010200000003000000000000000000f03f000000000000f03f0000000000000000000000000000f03f00000000000000000000000000000000
#> 4 010200000004000000000000000000f03f0000000000000000000000000000004000000000000000000000000000000040000000000000f03f000000000000f03f000000000000f03f
```

The shared edge between `a` and `b` appears once, with both face columns
filled; each outer edge appears once with the second face `NA`.

[`spatial_centerline()`](https://gillescolling.com/vectra/reference/spatial_centerline.md)
traces the centerline (medial axis) of each polygon from the Voronoi
diagram of its densified boundary: the Voronoi edges that fall inside
the polygon are its skeleton, merged into maximal lines. `density` sets
the boundary sampling and `prune` drops the short spurs the skeleton
grows toward convex corners. It is the usual approximation for a river
or road centerline from a filled shape; non-polygon geometry passes
through unchanged.

``` r

strip <- st_polygon(list(rbind(
  c(0, 0), c(10, 0), c(10, 2), c(0, 2), c(0, 0))))

cl <- tempfile(fileext = ".vtr")
write_vtr(data.frame(geometry = st_as_binary(st_sfc(strip), hex = TRUE)), cl)

tbl(cl) |> spatial_centerline(density = 0.25, prune = 0.5) |> collect_sf()
#> Simple feature collection with 5 features and 0 fields
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0.125 ymin: 0.125 xmax: 9.875 ymax: 1.875
#> CRS:           NA
#>                         geometry
#> 1 LINESTRING (1 1, 0.875 0.87...
#> 2 LINESTRING (0.125 1.875, 0....
#> 3 LINESTRING (1 1, 1.125 1, 1...
#> 4 LINESTRING (9.875 1.875, 9....
#> 5 LINESTRING (9 1, 9.125 0.87...
```

## Set-wise constructions

[`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
builds a geometry construction from a whole feature set, the
constructions a per-feature
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
cannot express. A `kind` argument selects it: `"convex_hull"`,
`"concave_hull"`, `"envelope"`, `"oriented_box"`, `"enclosing_circle"`,
`"inscribed_circle"`, `"pole"` (the pole of inaccessibility, the centre
of the maximum inscribed circle), `"voronoi"`, and `"delaunay"`. A `by`
argument builds one construction per group; the enclosing kinds emit one
feature per group, the tessellations one polygon per cell.

``` r

nc <- st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
nc$band <- nc$SID74 > 5

n <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  band = nc$band,
  geometry = st_as_binary(st_geometry(nc), hex = TRUE)), n)

tbl(n) |>
  spatial_construct("convex_hull", by = "band", crs = st_crs(nc)) |>
  collect_sf()
#> Simple feature collection with 2 features and 1 field
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -84.32385 ymin: 33.88199 xmax: -75.45698 ymax: 36.58965
#> Geodetic CRS:  NAD27
#>    band                       geometry
#> 1 FALSE POLYGON ((-84.32385 34.9890...
#> 2  TRUE POLYGON ((-82.88111 35.6735...
```

A tessellation kind returns one polygon per cell, each carrying the
group’s `by` values:

``` r

xy <- rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0.4, 0.6))
cloud <- st_sfc(lapply(seq_len(nrow(xy)), function(i) st_point(xy[i, ])))

v <- tempfile(fileext = ".vtr")
write_vtr(data.frame(geometry = st_as_binary(cloud, hex = TRUE)), v)

tbl(v) |> spatial_construct("voronoi") |> collect_sf()
#> Simple feature collection with 5 features and 0 fields
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -1 ymin: -1 xmax: 2 ymax: 2
#> CRS:           NA
#>                         geometry
#> 1 POLYGON ((2 2, 2 0.5, 0.9 0...
#> 2 POLYGON ((-1 2, 0.5 2, 0.5 ...
#> 3 POLYGON ((-1 -1, -1 0.5, -0...
#> 4 POLYGON ((0.5 1.1, 0.9 0.5,...
#> 5 POLYGON ((2 -1, 0.5 -1, 0.5...
```

## Linear referencing

[`spatial_locate()`](https://gillescolling.com/vectra/reference/spatial_locate.md)
places streamed points along a resident line layer: each point gets its
nearest line’s identifier, the measure (distance along that line to the
point’s projection), and the perpendicular offset, with an optional
`snap` onto the line. The inverse, a measure back to a point, is
[`sf::st_line_interpolate()`](https://r-spatial.github.io/sf/reference/st_line_project_point.html)
through
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md).

``` r

roads <- st_sf(road = c("main", "side"), geometry = st_sfc(
  st_linestring(rbind(c(0, 0), c(10, 0))),
  st_linestring(rbind(c(0, 5), c(0, 15)))))

pts <- data.frame(id = 1:2, x = c(3, 1), y = c(1, 9))
l <- tempfile(fileext = ".vtr")
write_vtr(pts, l)

tbl(l) |>
  spatial_locate(roads, coords = c("x", "y"), y_id = "road") |>
  collect()
#>   id x y line measure distance                                   geometry
#> 1  1 3 1 main       3        1 01010000000000000000000840000000000000f03f
#> 2  2 1 9 side       4        1 0101000000000000000000f03f0000000000002240
```

Point 1 is one unit off `main` at measure 3; point 2 is one unit off
`side` at measure 4. With `snap = TRUE` each point’s geometry is
replaced by its projection onto the matched line.
