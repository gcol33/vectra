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
- Split with lines, and line-intersection points between two layers (done,
  0.9.2). `spatial_split()` cuts each streamed feature against a resident blade
  layer -- a polygon into the faces the blade carves out, a line into its arcs --
  emitting one piece per row, and with `extract = "points"` returns the
  intersection points of each feature with the blade instead.
- Smooth (Chaikin) for line work (done, 0.9.2). `spatial_smooth()` rounds the
  corners of streamed lines and polygons by Chaikin corner-cutting, computed
  directly on the coordinates (no GEOS call). Densify and points-along are
  per-feature transforms that already run through `spatial_map`
  (`~ sf::st_segmentize(.x, dfMaxLength)` and `~ sf::st_line_sample(.x, n)`), so
  they need no dedicated verb, as buffer and simplify do not.

## Tier 4 - build geometry from a set, and linear referencing (done, 0.9.3-0.9.5)

The operations that need either a whole set of features fused into new geometry,
or a point located against a resident line layer. The three set-wise verbs ride
the partition tier alongside `spatial_dissolve` and `spatial_construct` (shared
`.partition_each` router: spill once, one shard per `by` group, one group in
memory at a time); `spatial_locate` is a resident-`y` streamed verb in the
`spatial_knn` / `spatial_split` family.

- `spatial_polygonize()` builds the polygonal faces enclosed by a line network
  (the QGIS "Polygonize", the inverse of taking boundaries): the group's lines
  are unioned and noded, then the faces of that arrangement are returned, one per
  row. Reuses the same `st_node` / `st_polygonize` path `spatial_split` uses to
  carve polygons.
- `spatial_line_merge()` sews line segments that meet end to end into maximal
  linestrings (`st_line_merge`), the line counterpart of a dissolve; each maximal
  chain is one row, and segments meeting at a junction of degree > 2 stay
  separate.
- `spatial_simplify()` simplifies a polygon **coverage** without tearing shared
  edges: boundaries are unioned so a shared border is one line, noded into arcs,
  each arc simplified once (junction endpoints pinned), and re-polygonized, so
  adjacent polygons stay edge-matched with no slivers. This is the
  topology-preserving simplification a per-feature `spatial_map(~ st_simplify())`
  cannot give, because that simplifies each polygon's copy of a shared border
  independently. Each simplified face inherits its source polygon's attributes.
- `spatial_locate()` locates streamed points along a resident line layer
  (linear referencing, `st_line_project`): each point gets the identifier of its
  nearest line, the measure (distance along that line), and the perpendicular
  offset, with an optional `snap` onto the line. The inverse direction (a measure
  back to a point) is `sf::st_line_interpolate` through `spatial_map`.
- `spatial_centerline()` (0.9.4) traces the medial axis of each streamed polygon
  from the Voronoi diagram of its densified boundary: the Voronoi edges inside
  the polygon are its skeleton, merged into lines, with an optional `prune` for
  the short spurs toward convex corners. Per-feature streamed (one polygon at a
  time), the approximation used for river or road centerlines from a filled
  shape.
- `spatial_topology()` (0.9.4) decomposes a polygon coverage into the arcs of its
  planar topology: the unioned boundaries are noded so a shared border is one
  arc, tagged with the identifiers of the (up to two) polygons on either side --
  two for an internal edge, one for an outer edge. Rides the partition tier; the
  "build topology" of a GIS, and the inverse of `spatial_polygonize`.
- `spatial_eliminate()` (0.9.5) cleans a polygon coverage by absorbing every
  feature smaller than `max_area` into a neighbour (the QGIS "Eliminate"): each
  sliver joins the neighbour it shares the longest border with (or, with `into =
  "largest_area"`, the largest neighbour), and an area-rooted union-find collapses
  chains of slivers so a connected run flows to its single largest member, whose
  attributes survive. A sliver with no neighbour is kept, so nothing vanishes.
  The merge target is one of a sliver's neighbours, not the sliver itself, so a
  per-feature `spatial_map` cannot express it; it rides the partition tier.

## Tier 5 - network analysis (future, separate engine)

Routing and reachability over a line network -- shortest path, service areas, an
origin-destination cost matrix, travel-time isochrones (the QGIS network-analysis
tools, `sfnetworks`/pgRouting). This is the one vector workflow a GIS user
expects that vectra does not address, and it is the next genuine tier rather than
a missing verb: it needs a graph built from the geometry (nodes at line
endpoints, edges weighted by length or a cost column) and a shortest-path solver
over it, not a geometry stream. A streamed design would build the node-edge graph
from a line layer once (the partition tier already nodes and indexes lines), keep
the graph resident, and stream the queries -- one row per origin for a cost
matrix, one polygon per origin for a service area -- so the query side scales
while the graph stays in memory. Out of scope for the current geometry tiers;
recorded here as the deliberate boundary.

## Out of scope (a different package)

These sit on top of geometry but are not geometry operations, and fit a
statistics or solver package better than a columnar geometry engine:

- **Spatial interpolation and surfaces** -- IDW, kriging, kernel-density
  heatmaps. A solver over the whole point set, partly raster output; the raster
  tier covers the output format, not the estimator.
- **Spatial statistics and point-pattern** -- Moran's I, Getis-Ord hot spots,
  Ripley's K. Global estimators over a layer, not streamable element by element.
- **Geocoding, conflation, map-matching** -- need external services or external
  reference data.

## Coverage cleanup beyond eliminate

Two cleanup operations remain, both expressible from the verbs already shipped,
so neither is its own tier:

- **Fill gaps in a coverage** -- the empty slivers between polygons that should
  tile. The gap polygons come from the boundary arcs (`spatial_topology` /
  `spatial_polygonize` of the unioned boundary minus the input), and each gap
  then merges into a neighbour by exactly the `spatial_eliminate` machinery; a
  future `spatial_eliminate(fill_gaps = TRUE)` argument would fold it into the
  same verb rather than a sibling.
- **Delete holes** -- drop the interior rings of individual polygons. This is a
  per-feature transform and runs through `spatial_map(~ ...)` rebuilding each
  polygon from its exterior ring, so it needs no dedicated verb.

## Notes

- Every verb keeps the streaming contract: peak memory tracks the result or the
  per-group working set, not the input length.
- Prefer arguments on existing verbs over new sibling functions (the two-layer
  overlay is the model: one `spatial_overlay`, optional `y =` and `how =`).
- CRS is threaded from the input node and never hardcoded, as in the current
  spatial verbs.
