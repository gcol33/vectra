# Dissolve geometries by group

Unions the geometries within each `by` group into a single feature (the
GIS "Dissolve" tool), optionally summarising attributes. Unlike the
streamed per-batch verbs, dissolve needs every geometry of a group
together to union them, so it rides the **partition tier**: `x` is
spilled once and routed into one disjoint shard per group in a single
bounded pass, then each shard is read in and unioned with sf. Peak
memory is the routing budget during the pass, then one group's
geometries while it is unioned – partition the input on a key whose
groups fit in memory. With no `by`, the whole layer dissolves into one
feature.

## Usage

``` r
spatial_dissolve(
  x,
  by = NULL,
  ...,
  geom = "geometry",
  crs = NA,
  .fun = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- by:

  Character vector of attribute columns to dissolve within: one output
  feature per distinct combination of their values. `NULL` (default)
  dissolves the entire layer into a single feature.

- ...:

  Further arguments passed to
  [`sf::st_union()`](https://r-spatial.github.io/sf/reference/geos_combine.html)
  (e.g. `is_coverage = TRUE`).

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- .fun:

  Optional named list of attribute summaries. Each element is a function
  taking the group's data.frame and returning a length-1 value; the list
  name becomes the output column (e.g.
  `.fun = list(total = function(d) sum(d$pop))`). Default `NULL` keeps
  only the `by` columns and the dissolved geometry.

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` of one row per group – the `by` columns, any `.fun`
summaries, and the dissolved geometry – backed by temporary `.vtr`
spills removed when the node is garbage-collected, carrying the input
CRS for
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Details

Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize. On projected or unprojected planar data each group is
unioned natively on the GEOS C API straight off the hex-WKB column;
geographic coordinates with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html)),
or any extra
[`sf::st_union()`](https://r-spatial.github.io/sf/reference/geos_combine.html)
arguments (e.g. `is_coverage = TRUE`), union through sf instead. The sf
package is an optional dependency (Suggests).

## See also

[`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
to split overlaps apart rather than merge them,
[`offload()`](https://gillescolling.com/vectra/reference/offload.md) for
the partition tier this rides on,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
nc$band <- nc$SID74 > 5            # an attribute to dissolve within
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  band = nc$band, BIR74 = nc$BIR74,
  geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
), f)

# Merge the counties into two features by `band`, summing births.
merged <- tbl(f) |>
  spatial_dissolve(by = "band", crs = sf::st_crs(nc),
                   .fun = list(births = function(d) sum(d$BIR74)))
collect_sf(merged)
unlink(f)
```
