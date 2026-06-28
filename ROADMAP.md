# vectra vector GIS roadmap

Scope: vector geometry verbs a QGIS or `sf`/`terra` user expects, that vectra
does not yet stream. Each entry states what the operation does, why it cannot be
expressed with the current verbs, and a sketch of how it would fit the existing
API.

What already covers a lot, so it is not listed below:

- Per-feature transforms (buffer, centroid, point-on-surface, simplify, boundary,
  `st_cast` polygon/line conversions, reproject via `st_transform`, make-valid)
  all run through `spatial_map(~ sf::fn(.x, ...))`, streamed batch at a time.
- Erase / geometric difference against a mask is `spatial_clip(mask, erase = TRUE)`.
- Dissolve / aggregate-by-attribute is `spatial_dissolve(by =)`.
- Select-by-location is `spatial_filter`; point-in-polygon, nearest-feature, and
  the two-sided grid-partitioned join are `spatial_join`.
- Single-layer self-union overlay is `spatial_overlay`.

The gaps are the operations that need either two layers fused, or a set-wide
construction that a per-feature map cannot produce.

## Tier 1 - two-layer overlay (done, 0.9.2)

`spatial_overlay()` takes an optional second layer `y` and nodes two layers into
one planar partition, carrying the attributes of the covering `x`-record and
`y`-record onto each piece. The self-union stays the default (`y = NULL`).

```r
spatial_overlay(x, y = NULL, vars = NULL, vars_y = NULL,
                how = c("intersection", "union", "identity", "symdiff"),
                ...)
```

- `y = NULL` -> self-union, unchanged.
- `how = "intersection"` -> only the overlapping pieces, attributes from both.
- `how = "union"` -> all pieces of both layers, the absent side filled with `NA`.
- `how = "identity"` -> all of `x`, split by `y`, `y` attributes where covered.
- `how = "symdiff"` -> pieces in exactly one layer (also the symmetric difference).

`vars_y` selects the carried `y` columns; a name shared with `x` is disambiguated
with a `.x` / `.y` suffix. `y` accepts an `sf` object or a file path
(`layer_y` / `query_y`) read in batches. It reuses the existing noding, dedup,
component-tiling, and streaming machinery, so it scales like the self-union.

## Tier 2 - set-wise geometry constructions (done, 0.9.2)

`spatial_construct()` builds one geometry (or a tessellation) from a whole set of
features, the constructions a per-feature `spatial_map` cannot express because
they need every feature in scope at once. A `kind` argument selects it:

- `"convex_hull"` and `"concave_hull"` of a feature set.
- `"voronoi"` tessellation and `"delaunay"` triangulation of a point set.
- minimum bounding geometry: `"envelope"`, `"oriented_box"`,
  `"enclosing_circle"`.
- `"inscribed_circle"` and `"pole"` (the QGIS pole of inaccessibility, the point
  inside the shape farthest from its edges).

It rides the partition tier like `spatial_dissolve`: a `by =` argument routes the
layer into one shard per group and emits one construction per group (one polygon
per cell for the tessellations), with `by = NULL` constructing from the whole
layer. Peak memory is the routing budget, then one group's geometry.

## Tier 2 - snapping and topology cleanup (done, 0.9.2)

- `spatial_snap()` snaps the geometries of a streamed layer toward a resident
  reference layer within a tolerance (vertex and edge snapping), the QGIS "snap
  geometries to layer".
- `spatial_snap_grid()` snaps coordinates to a fixed grid as a standalone verb,
  exposing the fixed-precision snap-rounding the overlay noder uses internally so
  a layer can be cleaned or pre-noded to a common precision without running a
  full overlay.

## Tier 2 - explode and collect as verbs

`spatial_explode()` (done, 0.9.2) streams one row per single-part component of
each multipart geometry, copying the source attributes onto each part, with an
optional `part` index column. The inverse, collect-to-multipart, is the
group-and-combine direction already served by `spatial_dissolve()`.

## Tier 3 - analysis verbs

- Distance matrix / k-nearest with returned distances (done, 0.9.2).
  `spatial_knn()` finds the `k` nearest resident-`y` features for each streamed
  feature, returning one row per (left, neighbour) pair with rank, identifier,
  and distance -- the top-`k` and the distances `spatial_join`'s nearest-feature
  match does not give.
- Split with lines, and line-intersection points between two layers.
- Smooth (Chaikin) for line work (done, 0.9.2). `spatial_smooth()` rounds the
  corners of streamed lines and polygons by Chaikin corner-cutting, computed
  directly on the coordinates (no GEOS call). Densify and points-along are
  per-feature transforms that already run through `spatial_map`
  (`~ sf::st_segmentize(.x, dfMaxLength)` and `~ sf::st_line_sample(.x, n)`), so
  they need no dedicated verb, as buffer and simplify do not.

## Notes

- Every verb keeps the streaming contract: peak memory tracks the result or the
  per-group working set, not the input length.
- Prefer arguments on existing verbs over new sibling functions (the two-layer
  overlay is the model: one `spatial_overlay`, optional `y =` and `how =`).
- CRS is threaded from the input node and never hardcoded, as in the current
  spatial verbs.
