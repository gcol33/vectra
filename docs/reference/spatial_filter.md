# Keep streamed rows by their spatial relation to a resident layer

Streams a large left side `x` through the engine and keeps each row
whose geometry satisfies an sf binary predicate against a small resident
layer `y` (select by location). This is the spatial counterpart to a
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md):
rows are filtered, never duplicated, and no columns are added, so the
output carries `x`'s schema unchanged. With `negate = TRUE` it keeps the
rows that do *not* match (select by location, inverted). The billion-row
left stream never materializes; `y` (a study region, habitat patches, a
coastline buffer, ...) stays resident.

## Usage

``` r
spatial_filter(
  x,
  y,
  predicate = NULL,
  negate = FALSE,
  geom = "geometry",
  coords = NULL,
  crs = NA,
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

  An `sf` or `sfc` object: the resident locator layer to test against.

- predicate:

  An sf binary predicate function, e.g.
  [sf::st_intersects](https://r-spatial.github.io/sf/reference/geos_binary_pred.html)
  (default),
  [sf::st_within](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [sf::st_covered_by](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [sf::st_is_within_distance](https://r-spatial.github.io/sf/reference/geos_binary_pred.html).
  A left row is kept when the predicate reports at least one match
  against `y`.

- negate:

  If `TRUE`, keep the rows with no match instead (the inverted
  select-by-location). Default `FALSE`.

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

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

- ...:

  Further arguments passed to `predicate`, e.g. `dist =` for
  [sf::st_is_within_distance](https://r-spatial.github.io/sf/reference/geos_binary_pred.html).

## Value

A `vectra_node` of the kept rows with `x`'s schema, backed by temporary
`.vtr` spills and carrying the input CRS.

## Details

For the recognised predicates – the topological ones (intersects,
within, contains, overlaps, covers, covered by, touches, crosses),
equals, disjoint, and within-distance
([sf::st_is_within_distance](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
whose radius is passed as `dist =`) – on projected or unprojected planar
data, the test runs natively on the GEOS C API straight off the hex-WKB
column: `y` is parsed once into a spatial index and each batch is tested
in C, with no per-batch round-trip through sf. Coordinate-assembled
(`coords`) point input runs natively too, building each point in C
rather than through sf; disjoint is the one exception there (its matches
are the bounding boxes the index prunes away) and keeps the sf loop, as
it does for the join. Geographic coordinates with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html))
and any other predicate use sf instead, preserving its semantics. When
`y` carries no CRS it inherits the stream's so the predicate does not
reject on a mismatch.

## See also

[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
to tag rows with `y`'s attributes,
[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md)
to cut geometry against a mask,
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) for
attribute predicates.

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
region <- nc[nc$NAME %in% c("Ashe", "Alleghany", "Surry"), "NAME"]

set.seed(1)
pts <- sf::st_coordinates(sf::st_sample(nc, 300))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)

# Keep only the points that fall inside the three-county region, streaming.
inside <- tbl(f) |>
  spatial_filter(region, coords = c("x", "y"), crs = sf::st_crs(nc))
nrow(collect(inside))
unlink(f)
```
