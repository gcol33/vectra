# Snap a streamed layer's coordinates to a fixed grid

Rounds every coordinate of a streamed layer to a regular grid of spacing
`size` (in CRS units) and repairs the result, one batch at a time. This
is the fixed-precision snap-rounding the overlay noder
([`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md))
applies internally, exposed as a standalone verb: it merges
near-coincident vertices and removes the slivers that floating-point
coordinates leave between shared boundaries, so a layer can be cleaned
(or pre-noded to a common precision) without running a full overlay.
Snapping is done in C straight off the hex-WKB column; one cleaned
geometry comes back per input feature, so attributes ride through
untouched.

## Usage

``` r
spatial_snap_grid(
  x,
  size,
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

- size:

  Grid spacing in CRS units (a positive number). Coordinates are rounded
  to the nearest multiple; a larger `size` snaps more aggressively.

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
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` of the snapped geometry with `x`'s attributes, backed by
temporary `.vtr` spills (removed when the node is garbage-collected) and
carrying the input CRS.

## Details

Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize. The sf package is an optional dependency (Suggests).

## See also

[`spatial_snap()`](https://gillescolling.com/vectra/reference/spatial_snap.md)
to snap toward another layer instead of a grid,
[`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
whose noding uses the same snap-rounding,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Examples

``` r
p <- sf::st_polygon(list(rbind(c(0.04, 0.03), c(1.02, 0.01),
                               c(0.98, 1.03), c(0.01, 0.97), c(0.04, 0.03))))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1L, geometry = sf::st_as_binary(sf::st_sfc(p), hex = TRUE)
), f)

# Snap the jittered corners back onto a 0.1 grid.
tbl(f) |> spatial_snap_grid(0.1) |> collect_sf()
unlink(f)
```
