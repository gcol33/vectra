# Geometry functions in expressions

vectra carries geometry through the engine as hex-encoded WKB in an
ordinary string column. The verbs in [Streaming spatial
operations](https://gillescolling.com/vectra/articles/streaming-spatial.md)
wrap whole sf operations around that column. This vignette covers the
other half of the spatial surface: a family of `st_*` functions that
work inside the expression verbs themselves, so a measure, a predicate,
or a geometry transform is just another term in
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`filter()`](https://gillescolling.com/vectra/reference/filter.md), or
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md).

These functions run on the GEOS C library straight off the WKB column,
one row at a time, with no per-batch round-trip through sf.
`filter(st_area(geometry) > 1e6)` prunes the stream in C, and
`mutate(here = st_centroid(geometry))` adds a new WKB geometry column
that any later verb can read. The per-row decode is parallelised with
OpenMP, so a measure over a large layer uses every core.

``` r

library(vectra)
library(sf)
```

## A layer to work on

The examples use the North Carolina counties shipped with sf. Writing
the layer to a `.vtr` is the usual first step: the geometry becomes a
hex-WKB string column (named `geometry` by convention), and the
attributes ride alongside it.

``` r

nc <- st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)

f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  NAME     = nc$NAME,
  BIR74    = nc$BIR74,
  geometry = st_as_binary(st_geometry(nc), hex = TRUE)
), f)

tbl(f)
#> vectra query node
#> Columns (3):
#>   NAME <string>
#>   BIR74 <double>
#>   geometry <string>
```

The counties are stored in longitude and latitude, so every measure
below is planar in those units, the same convention the streaming verbs
follow. Project the layer first if you need metric areas or distances.

## Measures

A measure reads a geometry and returns a number, so it drops into
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) as an
ordinary column.

``` r

tbl(f) |>
  mutate(area   = st_area(geometry),
         perim  = st_perimeter(geometry),
         nverts = st_npoints(geometry)) |>
  select(NAME, area, perim, nverts) |>
  collect() |>
  head()
#>          NAME       area    perim nverts
#> 1        Ashe 0.11428350 1.442087     27
#> 2   Alleghany 0.06139976 1.231197     26
#> 3       Surry 0.14301628 1.630283     28
#> 4   Currituck 0.06977098 2.967975     38
#> 5 Northampton 0.15275930 2.206482     34
#> 6    Hertford 0.09715756 1.669527     22
```

[`st_length()`](https://r-spatial.github.io/sf/reference/geos_measures.html)
returns the boundary length of a polygon (an alias of
[`st_perimeter()`](https://r-spatial.github.io/sf/reference/geos_measures.html))
and the line length of a linestring. `st_ngeometries()` counts the parts
of a multi-geometry. `st_x()` and `st_y()` read the coordinate of a
point and return `NA` for anything that is not a point, which makes them
most useful on a centroid:

``` r

tbl(f) |>
  mutate(centroid = st_centroid(geometry),
         cx = st_x(centroid),
         cy = st_y(centroid)) |>
  select(NAME, cx, cy) |>
  collect() |>
  head()
#>          NAME        cx       cy
#> 1        Ashe -81.49826 36.43140
#> 2   Alleghany -81.12515 36.49101
#> 3       Surry -80.68575 36.41252
#> 4   Currituck -76.02750 36.40728
#> 5 Northampton -77.41056 36.42228
#> 6    Hertford -76.99478 36.36145
```

A geometry-valued function such as
[`st_centroid()`](https://r-spatial.github.io/sf/reference/geos_unary.html)
produces a new WKB column (`centroid` above), and the next term reads it
like any other column. That is the whole mechanism: geometry in,
geometry or a scalar out, all as columns.

## Predicates

A unary predicate tests one geometry:
[`st_is_valid()`](https://r-spatial.github.io/sf/reference/valid.html),
[`st_is_empty()`](https://r-spatial.github.io/sf/reference/geos_query.html),
[`st_is_simple()`](https://r-spatial.github.io/sf/reference/geos_query.html).
A binary predicate tests a topological relation against a second
geometry:
[`st_intersects()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_within()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_contains()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_overlaps()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_touches()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_crosses()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_equals()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_disjoint()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_covers()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
[`st_covered_by()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html).

In [`filter()`](https://gillescolling.com/vectra/reference/filter.md) a
predicate keeps the rows where the relation holds, the
geometry-expression form of select-by-location:

``` r

aoi <- st_as_sfc(st_bbox(c(xmin = -80, ymin = 35, xmax = -78, ymax = 36.5)),
                 crs = st_crs(nc))

tbl(f) |>
  filter(st_intersects(geometry, aoi)) |>
  collect() |>
  nrow()
#> [1] 30
```

The second geometry here is a constant `sf` object. It is parsed once
and shared read-only across every row, so testing a whole stream against
one area of interest stays cheap. A multi-feature object is unioned to a
single geometry first.

In [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
the same call returns a logical column, ready for a later verb:

``` r

tbl(f) |>
  mutate(near_raleigh = st_intersects(geometry, aoi)) |>
  filter(near_raleigh) |>
  select(NAME) |>
  collect() |>
  head()
#>         NAME
#> 1     Warren
#> 2    Caswell
#> 3 Rockingham
#> 4  Granville
#> 5     Person
#> 6      Vance
```

## Distance

[`st_distance()`](https://r-spatial.github.io/sf/reference/geos_measures.html)
returns the shortest planar distance to a second geometry, again a
constant or another column:

``` r

raleigh <- st_sfc(st_point(c(-78.64, 35.78)), crs = st_crs(nc))

tbl(f) |>
  mutate(centroid   = st_centroid(geometry),
         d_raleigh  = st_distance(centroid, raleigh)) |>
  select(NAME, d_raleigh) |>
  arrange(d_raleigh) |>
  collect() |>
  head()
#>        NAME  d_raleigh
#> 1      Wake 0.01352946
#> 2    Durham 0.34428008
#> 3  Johnston 0.38179328
#> 4  Franklin 0.46173171
#> 5   Harnett 0.47354352
#> 6 Granville 0.52039217
```

When the second argument is a geometry column instead of a constant, the
distance is computed row by row between the two columns.

## Aggregating a measure

Because a measure is an ordinary numeric column, it aggregates like one.
A grouped
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
over a measure is a zonal total computed entirely in the stream:

``` r

tbl(f) |>
  mutate(area = st_area(geometry)) |>
  summarise(total_area = sum(area), counties = n()) |>
  collect()
#>   total_area counties
#> 1    12.6278      100
```

## Transforms

A transform returns a geometry, so it builds a new WKB column.
Materialise it with
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md),
which reads the WKB column back into an `sf` object (point it at the
column with `geom =`, and pass the `crs` the layer was stored in).

``` r

hulls <- tbl(f) |>
  mutate(geometry = st_convex_hull(geometry)) |>
  select(NAME, geometry) |>
  collect_sf(crs = st_crs(nc))

hulls
#> Simple feature collection with 100 features and 1 field
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -84.32385 ymin: 33.88199 xmax: -75.45698 ymax: 36.58965
#> Geodetic CRS:  NAD27
#> First 10 features:
#>           NAME                       geometry
#> 1         Ashe POLYGON ((-81.47276 36.2343...
#> 2    Alleghany POLYGON ((-81.23989 36.3653...
#> 3        Surry POLYGON ((-80.87438 36.2338...
#> 4    Currituck POLYGON ((-75.79885 36.0728...
#> 5  Northampton POLYGON ((-77.30948 36.1627...
#> 6     Hertford POLYGON ((-76.98069 36.2302...
#> 7       Camden POLYGON ((-75.98134 36.1697...
#> 8        Gates POLYGON ((-76.68874 36.2945...
#> 9       Warren POLYGON ((-78.00629 36.1959...
#> 10      Stokes POLYGON ((-80.02567 36.2502...
```

The transform set is
[`st_centroid()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
[`st_point_on_surface()`](https://r-spatial.github.io/sf/reference/geos_unary.html)
(a point guaranteed to lie on the geometry),
[`st_boundary()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
`st_envelope()` (the bounding rectangle),
[`st_convex_hull()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
[`st_make_valid()`](https://r-spatial.github.io/sf/reference/valid.html)
(repair an invalid geometry), and two parameterised forms:
`st_buffer(g, dist)` and `st_simplify(g, tol)`. Buffering each county
and reading the areas back:

``` r

tbl(f) |>
  mutate(geometry = st_buffer(geometry, 0.1)) |>
  select(NAME, geometry) |>
  collect_sf(crs = st_crs(nc)) |>
  st_area() |>
  head()
#> Units: [m^2]
#> [1] 2843204836 2123154372 3334381567 3098502995 3953811907 2852194601
```

[`st_geometry_type()`](https://r-spatial.github.io/sf/reference/st_geometry_type.html)
returns the GEOS type name (`"Point"`, `"Polygon"`, `"MultiPolygon"`,
and so on) as a string column.

## The second geometry of a binary op

For
[`st_distance()`](https://r-spatial.github.io/sf/reference/geos_measures.html)
and the binary predicates, the second argument can be:

- another geometry column, compared row by row;

- a constant `sf` or `sfc` object, parsed once and reused across the
  stream (a multi-feature object is unioned to one geometry);

- a hex-WKB string.

## Missing geometry

A missing (`NA`) or unparseable geometry, or an operation with no answer
(the coordinate of a non-point, the distance to a missing geometry),
yields `NA` for that row rather than stopping the query:

``` r

g <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1:4,
  geometry = c(st_as_binary(st_geometry(nc)[1:3], hex = TRUE), NA)
), g)

tbl(g) |>
  mutate(area = st_area(geometry)) |>
  collect()
#>   id
#> 1  1
#> 2  2
#> 3  3
#> 4  4
#>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       geometry
#> 1                                 0106000000010000000103000000010000001b000000000000a0415e54c000000060ff1d4240000000209d6254c000000080e122424000000080f76354c0000000200523424000000020846854c0000000a09b2b4240000000c06d6f54c00000000026324240000000a0b06c54c000000040633c4240000000a0fa6c54c0000000c07942424000000040e16a54c0000000a0794b424000000060195654c0000000a053494240000000203e5654c000000060da44424000000020c95454c000000040c0414240000000800d5454c000000080873d4240000000000a5154c000000060f637424000000060d25054c000000060d833424000000080674f54c0000000c090304240000000605a4f54c000000040c42e424000000060e95054c0000000e01b2d4240000000400e5554c000000040872e4240000000c0205754c000000060342d424000000080675754c000000000662b424000000020aa5654c0000000205d26424000000060845754c000000060ac23424000000040025a54c0000000a07c244240000000a0635a54c0000000a03622424000000020965b54c0000000405f21424000000020fc5c54c0000000c0aa1e4240000000a0415e54c000000060ff1d4240
#> 2                                                                 0106000000010000000103000000010000001a000000000000605a4f54c000000040c42e424000000080674f54c0000000c09030424000000060d25054c000000060d8334240000000000a5154c000000060f6374240000000800d5454c000000080873d424000000020c95454c000000040c0414240000000203e5654c000000060da44424000000060195654c0000000a05349424000000000d23954c0000000e05848424000000040bf3b54c0000000c0c83f424000000040cf3d54c0000000e0cd3b424000000060c73c54c0000000001635424000000080353d54c0000000a0af334240000000c0963e54c0000000a018324240000000e0e63e54c000000040982f4240000000802d4054c000000060ef2e4240000000c0934154c0000000e05c30424000000040bd4254c0000000e08534424000000060644554c0000000a007374240000000e04e4654c0000000003037424000000080404754c00000000020364240000000c0474854c0000000009236424000000080db4854c0000000c074354240000000c0d04954c0000000e05d364240000000a04e4b54c0000000402d354240000000605a4f54c000000040c42e4240
#> 3 0106000000010000000103000000010000001c000000000000c0341d54c0000000200c1f4240000000207d1e54c0000000e09a204240000000405c2254c0000000c0dc20424000000080e12254c0000000806923424000000040772354c0000000a0a323424000000060cc2554c0000000e056224240000000c0f42754c0000000e0f422424000000060b72a54c000000040801f424000000000962c54c0000000e029214240000000a0562e54c0000000a015214240000000e0ff2e54c0000000a0e3214240000000002a3054c0000000e00f214240000000000b3154c0000000e083214240000000c0173254c000000060d11f4240000000e0f53754c0000000e0ef1d424000000040bc3754c0000000408d29424000000020e43854c0000000e05d2d4240000000202c3b54c000000060b62f424000000080353d54c0000000a0af33424000000060c73c54c0000000001635424000000040cf3d54c0000000e0cd3b424000000040bf3b54c0000000c0c83f424000000000d23954c0000000e05848424000000060a43554c0000000c01e484240000000801b2754c0000000805547424000000020dc1b54c0000000a08846424000000020fe1c54c000000040e8204240000000c0341d54c0000000200c1f4240
#> 4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         <NA>
#>         area
#> 1 0.11428350
#> 2 0.06139976
#> 3 0.14301628
#> 4         NA
```

## Where this fits

The `st_*` expressions are the scalar, per-row layer of vectra’s spatial
surface. They cover measures, predicates, and the common single-geometry
transforms at the price of a column term, with no sf object built per
batch. For an arbitrary per-feature transform that has no `st_*` form,
reach for
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md),
which runs any sf-in, sf-out function over each feature. For
constructions that read a whole feature set at once (dissolves,
overlays, hulls of a group, planar topology), the set-wise `spatial_*`
verbs in [Streaming spatial
operations](https://gillescolling.com/vectra/articles/streaming-spatial.md)
and [Coverage and
topology](https://gillescolling.com/vectra/articles/coverage-topology.md)
are the tools.
