# Smooth streamed line and polygon geometry

Rounds the corners of every line and polygon in a streamed layer by
Chaikin corner-cutting, one batch at a time. Each iteration replaces
every vertex with two points a quarter and three-quarters of the way
along its adjacent edges, so sharp angles become a sequence of short
chamfers that read as a smooth curve; more `iterations` give a smoother
result with more vertices. Open lines keep their endpoints
(`keep_ends`); polygon rings are cut cyclically and shrink slightly, as
Chaikin smoothing does. Point geometry passes through unchanged.

## Usage

``` r
spatial_smooth(
  x,
  iterations = 2L,
  keep_ends = TRUE,
  geom = "geometry",
  crs = NA,
  out_geom = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- iterations:

  Number of corner-cutting passes (a positive integer). Each pass
  roughly doubles the vertex count. Default `2`.

- keep_ends:

  If `TRUE` (default), pin the first and last vertex of an open line so
  it is not shortened at its tips. Ignored for closed rings.

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

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` of the smoothed geometry with `x`'s attributes, backed
by temporary `.vtr` spills (removed when the node is garbage-collected)
and carrying the input CRS.

## Details

The smoothing is computed directly on the coordinates (no GEOS call), so
it is dependency-light; sf is used only to decode and rebuild each
batch. Geometry travels through the engine as hex-encoded WKB in a
string column and the CRS is carried on the returned node; use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize.

## See also

[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
for per-feature transforms such as densifying with
`~ sf::st_segmentize(.x, dfMaxLength)` or sampling points along a line
with `~ sf::st_line_sample(.x, n)`,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
zig <- sf::st_linestring(rbind(c(0, 0), c(1, 1), c(2, 0), c(3, 1), c(4, 0)))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1L, geometry = sf::st_as_binary(sf::st_sfc(zig), hex = TRUE)
), f)

# Smooth the zig-zag with three corner-cutting passes.
tbl(f) |> spatial_smooth(iterations = 3) |> collect_sf()
unlink(f)
```
