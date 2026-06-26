# Self-overlay a polygon layer into disjoint pieces (QGIS-style Union)

Splits a polygon layer along all its own overlaps into disjoint pieces
and returns a lazy node with one row per piece per covering polygon:
where `k` polygons overlap, that piece appears `k` times, each row
carrying one source polygon's attributes. This is the union overlay GIS
tools expose as "Union (single layer)", with the overlap retained once
per contributing feature rather than dissolved. Resolve the duplicates
with a grouped
[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
/
[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
– for example earliest designation year wins:
`group_by(piece_id) |> slice_min(year)`.

## Usage

``` r
spatial_overlay(
  x,
  vars = NULL,
  piece = "piece_id",
  geom = "geometry",
  grid = NULL,
  flush_rows = NULL,
  mem_limit = NULL,
  threads = NULL,
  quiet = TRUE
)
```

## Arguments

- x:

  An `sf` object with polygon or multipolygon geometry.

- vars:

  Character vector of attribute columns of `x` to carry onto each piece.
  Default `NULL` keeps them all; name a subset to keep the streamed
  output narrow.

- piece:

  Name of the integer piece-id column added to the output (the key you
  group by to resolve overlaps). Default `"piece_id"`.

- geom:

  Name of the output hex-WKB geometry column. Default `"geometry"`.

- grid:

  Fixed-precision snapping grid size in CRS units. Coordinates are
  snapped to this grid before noding so near-duplicate shared boundaries
  merge into one. `NULL` (the default) derives it from coordinate
  magnitude (`max(abs(st_bbox(x))) * 3e-8`), which suits projected
  layers. Pass a number to override when that default is too coarse for
  fine geometry (or too coarse because an outlier coordinate inflated
  the magnitude), or `0` to disable snapping entirely.

- flush_rows:

  Exploded rows buffered before a spill flush. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

- mem_limit:

  Approximate peak working-set budget in bytes. Components are grouped
  into chunks within this budget and each chunk is overlaid then spilled
  before the next, so memory stays bounded regardless of layer size.
  Raise it for more parallel throughput, lower it for tighter memory.
  Defaults to `getOption("vectra.overlay_mem_limit", 2e9)`.

- threads:

  Number of OpenMP threads for the per-component overlay within a chunk.
  `0` (the default, via `getOption("vectra.overlay_threads", 0)`) uses
  all available cores.

- quiet:

  If `FALSE`, show a text progress bar over the overlay chunks.

## Value

A `vectra_node` over the exploded overlay (one row per piece per
covering polygon), backed by temporary `.vtr` spills removed when the
node is garbage-collected, carrying the CRS of `x` for
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Details

The topology is done once with sf/GEOS and tiled over connected overlap
clusters (disjoint clusters never share a piece, so the tiling is exact
and bounded in memory), then the exploded pieces are streamed to a
`.vtr` and handed back as a lazy node. Geometry rides through the engine
as hex-encoded WKB in a string column; the CRS is carried on the node
for
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

The overlay runs on a fixed-precision model: coordinates are snapped to
a grid derived from their own magnitude so the pieces come out disjoint
and their areas reconstruct the union of the inputs, instead of drifting
by the fraction of a percent that floating-point sliver artefacts on
invalid input otherwise introduce. Inputs are also passed through
[`sf::st_make_valid()`](https://r-spatial.github.io/sf/reference/valid.html).

The input `x` must be a resident `sf` object: building the overlap graph
and intersecting needs the geometries in memory. The exploded result,
which is typically several times larger, is what streams to disk.

## See also

[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
/
[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
to resolve each piece to one winner,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
# Two overlapping squares designated in different years.
sq <- function(a, b) sf::st_polygon(list(rbind(
  c(a, 0), c(b, 0), c(b, 1), c(a, 1), c(a, 0))))
polys <- sf::st_sf(year = c(1990L, 2010L),
                   geometry = sf::st_sfc(sq(0, 2), sq(1, 3)))

# Split into disjoint pieces; earliest year wins where they overlap.
first <- spatial_overlay(polys) |>
  group_by(piece_id) |>
  slice_min(year, n = 1, with_ties = FALSE) |>
  collect_sf()
first
```
