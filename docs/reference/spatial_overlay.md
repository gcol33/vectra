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
  y = NULL,
  vars = NULL,
  vars_y = NULL,
  how = c("intersection", "union", "identity", "symdiff"),
  piece = "piece_id",
  geom = "geometry",
  grid = NULL,
  precision = NULL,
  dedup = TRUE,
  flush_rows = NULL,
  mem_limit = NULL,
  threads = NULL,
  quiet = TRUE,
  layer = NULL,
  query = NULL,
  layer_y = NULL,
  query_y = NULL,
  read_chunk = NULL
)
```

## Arguments

- x:

  An `sf` object with polygon or multipolygon geometry, or a single path
  to a vector file (e.g. a GeoPackage). A path is read in feature
  batches via `layer` / `query`, so the whole layer is never held in
  memory at once – peak memory then tracks the cleaned geometry, not the
  source size, which lets a larger-than-RAM layer overlay on a modest
  machine.

- y:

  Optional second layer to overlay `x` against, in the same forms `x`
  accepts (an `sf` object or a file path read via `layer_y` /
  `query_y`). It must share the CRS of `x`. `NULL` (the default)
  self-unions `x`.

- vars:

  Character vector of attribute columns of `x` to carry onto each piece.
  Default `NULL` keeps them all; name a subset to keep the streamed
  output narrow.

- vars_y:

  Character vector of attribute columns of `y` to carry onto each piece
  (two-layer overlay only). Default `NULL` keeps them all. A name shared
  with an `x` column is disambiguated with a `.x` / `.y` suffix in the
  output.

- how:

  For a two-layer overlay, which pieces to keep: `"intersection"`
  (covered by both layers; the default), `"union"` (every piece of
  either, the absent side's attributes filled with `NA`), `"identity"`
  (all of `x`, split by `y`, with `y`'s attributes where it covers and
  `NA` elsewhere), or `"symdiff"` (pieces in exactly one layer). Ignored
  when `y = NULL`.

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

- precision:

  Fixed-precision grid size, in CRS units, for noding the boundary
  linework. Noding on a fixed grid is deterministic and avoids the
  floating noder's repair-and-retry on dense overlapping linework, which
  is what makes a large dense layer feasible to overlay. It is far finer
  than `grid` so intersection points are not collapsed. `NULL` (the
  default) derives it from coordinate magnitude
  (`max(abs(st_bbox(x))) * 1e-13`); pass a number to override, or `0` to
  node in floating precision.

- dedup:

  Overlay one representative per group of byte-identical cleaned
  geometries and fan the per-record attributes back onto its pieces
  afterwards. Duplicates add no faces, so the result is identical; this
  only removes the redundant noding when many records are stacked over
  one site (common in WDPA-style data). `TRUE` by default; set `FALSE`
  to overlay every record.

- flush_rows:

  Exploded rows buffered before a spill flush. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

- mem_limit:

  Approximate peak working-set budget in bytes, bounding the per-tile
  size (`tile_bytes = mem_limit / (threads * 24)`). It is a throughput
  knob with an interior optimum, not "bigger is faster": too small
  replicates features across many tiles, too large nodes too much
  linework per tile (a superlinear cost), and a budget of tens of GB
  runs slower than the default on a dense layer. Lower it for tighter
  memory. Defaults via `getOption("vectra.overlay_mem_limit", ...)` to a
  value that scales with `threads` to hold the per-tile size near its
  measured optimum.

- threads:

  Number of OpenMP threads for the per-component overlay within a chunk.
  `0` (the default, via `getOption("vectra.overlay_threads", 0)`) uses
  all available cores.

- quiet:

  If `FALSE`, show a text progress bar over the overlay chunks.

- layer:

  When `x` is a file path, the name of the layer to read. Ignored for an
  `sf` `x`. Supply this or `query`.

- query:

  When `x` is a file path, a SQL statement selecting the features to
  overlay (read in batches via `LIMIT`/`OFFSET`); use it instead of
  `layer` for a subset or join. With `query` and no `layer`, pass `grid`
  explicitly, since the layer extent cannot be read from the file
  metadata.

- layer_y, query_y:

  The `layer` / `query` equivalents for a file-path `y`.

- read_chunk:

  Features per read/parse batch. `NULL` (default) sizes it from
  available RAM. Smaller batches lower peak memory; larger ones do fewer
  round trips.

## Value

A `vectra_node` over the exploded overlay, backed by temporary `.vtr`
spills removed when the node is garbage-collected, carrying the CRS of
`x` for
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).
For a self-union it is one row per piece per covering polygon; for a
two-layer overlay one row per piece per covering `x`-record / `y`-record
pair, with the columns of both layers.

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

With a second layer `y`, the same machinery overlays two layers instead
of self-unioning one: both layers are noded together into one planar
partition, and each piece carries the attributes of the `x` record and
the `y` record that cover it. `how` selects which pieces to keep – the
intersection (pieces covered by both), the union (every piece of
either), `x` split by `y` (`"identity"`), or the parts in exactly one
layer (`"symdiff"`). With `y = NULL` (the default) the function
self-unions `x` and `how` is ignored.

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

# Two-layer overlay: intersect the squares with a zone layer, keeping both
# sets of attributes on each overlapping piece.
zones <- sf::st_sf(zone = c("A", "B"),
                   geometry = sf::st_sfc(sq(0, 1.5), sq(1.5, 3)))
inter <- spatial_overlay(polys, zones, how = "intersection") |> collect_sf()
inter
```
