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
  keep_geom = TRUE,
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

- keep_geom:

  If `TRUE` (default) the output carries the left geometry column (the
  point geometry built from `coords`, when `coords` is given). `FALSE`
  drops it, for the common "tag, then aggregate the attributes" use
  where the geometry is never read; with `coords` the points are then
  never encoded at all.

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

- ...:

  Further arguments passed to
  [`sf::st_join()`](https://r-spatial.github.io/sf/reference/st_join.html).

## Value

A `vectra_node` of the joined stream carrying the left CRS. On the
native path it is a lazy node that consumes `x` when run; on the sf and
`partition` paths it is backed by temporary `.vtr` spills.

## Details

For the recognised predicates – the topological ones (intersects,
within, contains, overlaps, covers, covered by, touches, crosses),
equals, within-distance
([sf::st_is_within_distance](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
radius passed as `dist =`), and nearest feature
([sf::st_nearest_feature](https://r-spatial.github.io/sf/reference/st_nearest_feature.html))
– on projected or unprojected planar data, the join is a lazy plan node
that runs on the GEOS C API straight off the hex-WKB column: `y` is
parsed once into a spatial index, and each streamed batch is matched and
joined to `y`'s attributes in C, without decoding the left side to sf,
passing it through R, or spilling it to disk. The result streams into
the next verb, so tagging points and then aggregating
(`spatial_join() |> count()`) holds one batch at a time.
Coordinate-assembled (`coords`) point input runs natively too, building
each point in C (the emitted point geometry is built in C as well,
unless `keep_geom = FALSE`). Geographic coordinates with spherical
geometry on
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
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) to
materialize as `sf`,
[`offload()`](https://gillescolling.com/vectra/reference/offload.md) to
partition both-sides-huge joins.

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)

# A stream of points, stored with x/y coordinate columns.
set.seed(1)
pts <- sf::st_coordinates(sf::st_sample(nc, 200))
#> Warning: coordinate ranges not computed along great circles; install package lwgeom to get rid of this warning
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)

# Tag each point with the county it falls in, streaming.
tagged <- tbl(f) |>
  spatial_join(nc["NAME"], join = sf::st_intersects,
               coords = c("x", "y"), crs = sf::st_crs(nc))
head(collect(tagged))
#> Simple feature collection with 6 features and 4 fields
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: -81.96962 ymin: 34.27533 xmax: -76.35794 ymax: 36.53061
#> Geodetic CRS:  NAD27
#>   id         x        y       NAME                   geometry
#> 1  1 -81.96962 35.64837   McDowell POINT (-81.96962 35.64837)
#> 2  2 -81.02428 36.53061  Alleghany POINT (-81.02428 36.53061)
#> 3  3 -79.24443 34.50267    Robeson POINT (-79.24443 34.50267)
#> 4  4 -76.35794 36.12743 Perquimans POINT (-76.35794 36.12743)
#> 5  5 -78.46464 36.48867      Vance POINT (-78.46464 36.48867)
#> 6  6 -78.74558 34.27533   Columbus POINT (-78.74558 34.27533)

# Tag, then count per county: the join streams into count() and the point
# geometry is never built.
tbl(f) |>
  spatial_join(nc["NAME"], coords = c("x", "y"), crs = sf::st_crs(nc),
               keep_geom = FALSE) |>
  count(NAME) |>
  collect()
#>            NAME n
#> 1     Alexander 1
#> 2     Alleghany 2
#> 3         Anson 5
#> 4          Ashe 3
#> 5         Avery 2
#> 6      Beaufort 3
#> 7        Bertie 3
#> 8        Bladen 1
#> 9     Brunswick 4
#> 10     Buncombe 4
#> 11        Burke 3
#> 12     Caldwell 2
#> 13       Camden 1
#> 14     Carteret 2
#> 15      Caswell 1
#> 16      Catawba 1
#> 17    Cleveland 3
#> 18     Columbus 8
#> 19       Craven 2
#> 20   Cumberland 4
#> 21    Currituck 1
#> 22         Dare 1
#> 23     Davidson 2
#> 24       Duplin 4
#> 25    Edgecombe 1
#> 26      Forsyth 2
#> 27       Gaston 1
#> 28       Graham 1
#> 29    Granville 4
#> 30     Guilford 1
#> 31      Halifax 7
#> 32      Harnett 1
#> 33      Haywood 2
#> 34    Henderson 2
#> 35     Hertford 3
#> 36         Hoke 1
#> 37         Hyde 2
#> 38      Iredell 3
#> 39      Jackson 2
#> 40     Johnston 1
#> 41        Jones 2
#> 42       Lenoir 1
#> 43      Lincoln 2
#> 44        Macon 1
#> 45      Madison 3
#> 46     McDowell 4
#> 47  Mecklenburg 4
#> 48     Mitchell 3
#> 49   Montgomery 3
#> 50        Moore 1
#> 51         Nash 3
#> 52  New Hanover 1
#> 53  Northampton 2
#> 54       Onslow 4
#> 55       Orange 1
#> 56      Pamlico 2
#> 57       Pender 2
#> 58   Perquimans 2
#> 59       Person 1
#> 60         Pitt 4
#> 61     Randolph 8
#> 62     Richmond 2
#> 63      Robeson 4
#> 64   Rockingham 3
#> 65        Rowan 2
#> 66   Rutherford 4
#> 67      Sampson 3
#> 68     Scotland 3
#> 69       Stanly 4
#> 70       Stokes 1
#> 71        Surry 2
#> 72        Swain 2
#> 73 Transylvania 3
#> 74        Union 4
#> 75        Vance 1
#> 76         Wake 4
#> 77      Watauga 1
#> 78        Wayne 2
#> 79       Wilkes 2
#> 80       Yadkin 1
#> 81       Yancey 1

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
#> Simple feature collection with 6 features and 4 fields
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: -76.4638 ymin: 35.22927 xmax: -75.62045 ymax: 36.4641
#> Geodetic CRS:  NAD27
#>    id         x        y       NAME                   geometry
#> 1 165 -75.62045 35.22927       Dare POINT (-75.62045 35.22927)
#> 2 119 -75.91835 35.62766       Hyde POINT (-75.91835 35.62766)
#> 3  76 -76.08994 35.62090       Hyde  POINT (-76.08994 35.6209)
#> 4   4 -76.35794 36.12743 Perquimans POINT (-76.35794 36.12743)
#> 5  12 -76.03594 36.46410  Currituck  POINT (-76.03594 36.4641)
#> 6 101 -76.46380 36.33474 Perquimans  POINT (-76.4638 36.33474)
unlink(c(f, g))
```
