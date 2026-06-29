# Snap a streamed layer toward a resident reference layer

Streams a large layer `x` through the engine and snaps each batch's
vertices toward a small resident reference layer `y` when they lie
within `tolerance` (in CRS units), one batch at a time (the QGIS "snap
geometries to layer"). Vertices and edges of `x` closer than `tolerance`
to `y` are pulled onto `y`, which closes the small gaps and overshoots
between two layers that should share a boundary. The reference layer
stays resident while the billion-row left stream flows past; the snap
itself is sf's
[`sf::st_snap()`](https://r-spatial.github.io/sf/reference/geos_binary_ops.html).

## Usage

``` r
spatial_snap(
  x,
  y,
  tolerance,
  geom = "geometry",
  coords = NULL,
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

- y:

  An `sf` or `sfc` object: the resident reference layer to snap toward.

- tolerance:

  Snapping distance in CRS units (a positive number). Vertices and edges
  of `x` within this distance of `y` are moved onto `y`.

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

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` of the snapped geometry with `x`'s attributes, backed by
temporary `.vtr` spills (removed when the node is garbage-collected) and
carrying the input CRS.

## Details

Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize. When `y` carries no CRS it inherits the stream's. The sf
package is an optional dependency (Suggests).

## See also

[`spatial_snap_grid()`](https://gillescolling.com/vectra/reference/spatial_snap_grid.md)
to snap to a grid instead of a layer,
[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md)
for the resident-mask streaming pattern,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Examples

``` r
ref <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(10, 0))))
line <- sf::st_linestring(rbind(c(0, 0.2), c(5, 0.1), c(10, 0.2)))
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  id = 1L, geometry = sf::st_as_binary(sf::st_sfc(line), hex = TRUE)
), f)

# Pull the near-zero vertices down onto the reference line.
tbl(f) |> spatial_snap(ref, tolerance = 0.5) |> collect_sf()
unlink(f)
```
