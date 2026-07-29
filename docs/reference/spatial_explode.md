# Explode multipart geometries into single-part features

Streams a lazy vectra query and splits every multipart geometry into its
component single-part geometries: a `MULTIPOLYGON` becomes one row per
polygon, a `MULTILINESTRING` one row per linestring, a `MULTIPOINT` one
row per point, and a `GEOMETRYCOLLECTION` one row per member
(recursively). The attributes of the source feature are copied onto each
part. Already single-part geometries pass through unchanged, as does an
empty geometry (kept as one row). This is the streaming counterpart of
the QGIS "multipart to singleparts" tool and of
[`sf::st_cast()`](https://r-spatial.github.io/sf/reference/st_cast.html)
to a single-part type.

## Usage

``` r
spatial_explode(
  x,
  geom = "geometry",
  crs = NA,
  out_geom = NULL,
  part = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- part:

  Optional name of an integer column numbering the parts within each
  source feature, 1-based. Default `NULL` adds no such column.

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` of single-part features, backed by temporary `.vtr`
spills (removed when the node is garbage-collected) and carrying the
input CRS.

## Details

One batch is decoded, exploded, and spilled at a time, so peak memory
tracks one batch and its parts, not the whole layer.

## See also

[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
for per-feature transforms,
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
to merge features the other way,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
mp <- sf::st_multipolygon(list(
  list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))),
  list(rbind(c(2, 2), c(3, 2), c(3, 3), c(2, 3), c(2, 2)))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1L,
  geometry = sf::st_as_binary(sf::st_sfc(mp), hex = TRUE)
), f)

# One row per polygon, attributes copied, parts numbered.
tbl(f) |> spatial_explode(part = "part_id") |> collect_sf()
unlink(f)
```
