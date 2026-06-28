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

## Tier 1 - two-layer overlay

The largest gap. `spatial_overlay()` self-unions one layer. There is no streamed
overlay that fuses two different layers: split the geometry where they meet and
carry attributes from both sides onto each resulting piece. Today `spatial_join`
transfers attributes but leaves geometry uncut, and `spatial_clip` cuts geometry
but drops the mask's attributes. The WDPA self-union demo is the special case
`y = x`.

Approach: an optional `y =` argument on the existing verb rather than a new
function, so the self-union stays the default.

```r
spatial_overlay(x, y = NULL, vars = NULL,
                how = c("intersection", "union", "identity", "symdiff"),
                ...)
```

- `y = NULL` -> current self-union, unchanged.
- `how = "intersection"` -> only the overlapping pieces, attributes from both.
- `how = "union"` -> all pieces of both layers, attributes filled where present.
- `how = "identity"` -> all of `x`, split by `y`, `y` attributes where covered.
- `how = "symdiff"` -> pieces in exactly one layer (the XOR; folds in tier 2).

The noding, dedup, clustering, and streaming machinery already built for the
self-union carry over; the new work is the two-input topology pass and the
two-sided attribute fan-out. `y` should accept a resident `sf` object or a file
path/`vectra_node` so a large second layer is read in batches like `x`.

## Tier 1 - symmetric difference

Pieces present in exactly one of two layers. Standalone in QGIS, but here it is
the `how = "symdiff"` mode of the two-layer overlay above, so it ships with
tier 1 rather than as its own verb.

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

Multipart-to-singlepart (explode) runs inside `spatial_overlay` already but is
not exported. A streamed `spatial_explode()` (one row per part) and its inverse
collect-to-multipart are common enough in a QGIS workflow to expose directly,
and the explode pass already exists internally.

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
