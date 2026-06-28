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

## Tier 2 - set-wise geometry constructions

These build one geometry (or a tessellation) from a whole set of features, so a
per-feature `spatial_map` cannot express them: the construction needs every
feature in scope at once.

- Convex hull and concave hull of a feature set.
- Voronoi tessellation and Delaunay triangulation of a point set.
- Minimum bounding geometry (envelope, oriented box, enclosing circle).
- Pole of inaccessibility (the QGIS "point inside polygon farthest from edges").

These are bounded by the result size, not the input, so the streaming model fits:
accumulate input geometry (or its hull-relevant subset) per group, emit the
construction. A `by =` argument gives per-group hulls/tessellations, matching the
`spatial_dissolve` grouping idiom.

## Tier 2 - snapping and topology cleanup

- Snap geometries of one layer to another within a tolerance (vertex and edge
  snapping), the QGIS "snap geometries to layer".
- Snap-to-grid as a standalone verb. The fixed-precision snap-rounding added for
  overlay noding already does this internally; exposing it lets a user node a
  layer without running a full overlay.

## Tier 2 - explode and collect as verbs

`spatial_explode()` (done, 0.9.2) streams one row per single-part component of
each multipart geometry, copying the source attributes onto each part, with an
optional `part` index column. The inverse, collect-to-multipart, is the
group-and-combine direction already served by `spatial_dissolve()`.

## Tier 3 - analysis verbs

- Distance matrix / k-nearest with returned distances. `spatial_join` finds the
  nearest feature; this returns the distances and the top-k, not just the match.
- Split with lines, and line-intersection points between two layers.
- Points along geometry, densify, and smooth (Chaikin / spline) for line work.

## Notes

- Every verb keeps the streaming contract: peak memory tracks the result or the
  per-group working set, not the input length.
- Prefer arguments on existing verbs over new sibling functions (the two-layer
  overlay is the model: one `spatial_overlay`, optional `y =` and `how =`).
- CRS is threaded from the input node and never hardcoded, as in the current
  spatial verbs.
