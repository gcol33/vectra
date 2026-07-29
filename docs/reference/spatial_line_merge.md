# Merge contiguous line segments into maximal lines

Sews the line segments of each group into the longest possible
linestrings
([`sf::st_line_merge`](https://r-spatial.github.io/sf/reference/geos_unary.html),
the line counterpart of a dissolve): segments that meet end to end
become one chain, and each chain is emitted as its own row. Where a
plain union of lines returns a single multilinestring of all the parts,
this joins the parts through their shared endpoints; at a crossing where
more than two segments meet the merge is ambiguous and the segments stay
separate. Like
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
it rides the **partition tier**: `x` is spilled once and routed into one
disjoint shard per `by` group in a single bounded pass, then each
group's segments are merged together. Peak memory is the routing budget
during the pass, then one group's geometry while it is merged. With no
`by`, the whole layer is merged at once.

## Usage

``` r
spatial_line_merge(
  x,
  by = NULL,
  geom = "geometry",
  crs = NA,
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

  Character vector of attribute columns to merge within: one set of
  maximal lines per distinct combination of their values. `NULL`
  (default) merges the whole layer at once.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

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

## Value

A `vectra_node` of one row per maximal merged line, carrying the `by`
columns and the input CRS, backed by temporary `.vtr` spills removed
when the node is garbage-collected.

## Details

Each merged line is new geometry built from the whole group, so it
carries the `by` columns only, not the attributes of any single source
segment. Geometry travels through the engine as hex-encoded WKB in a
string column and the CRS is carried on the returned node. The sf
package is an optional dependency (Suggests).

## See also

[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
to union geometries by group,
[`spatial_explode()`](https://gillescolling.com/vectra/reference/spatial_explode.md)
for the opposite direction (multipart to single part),
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
seg <- sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(1, 0))),
  sf::st_linestring(rbind(c(1, 0), c(2, 0))),
  sf::st_linestring(rbind(c(2, 0), c(3, 0))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  geometry = sf::st_as_binary(seg, hex = TRUE)
), f)

# The three end-to-end segments become one line.
tbl(f) |> spatial_line_merge() |> collect_sf()
unlink(f)
```
