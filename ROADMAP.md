# vectra roadmap

Two parts:

1. **Vector GIS** (below) -- geometry verbs a QGIS or `sf`/`terra` user expects,
   that vectra does not yet stream.
2. **Beyond spatial** (end of file) -- other data domains the engine could
   support, sorted by architectural fit.

# Vector GIS

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

## Tier 5 - network analysis (done, 0.9.6)

Routing and reachability over a line network -- shortest path, origin-destination
cost matrices, service areas, travel-time isochrones (the QGIS network-analysis
tools, `sfnetworks`/pgRouting). The genuine separate tier rather than a missing
verb: it needs a graph built from the geometry (nodes at line endpoints, edges
weighted by length or a cost column) and a shortest-path solver over it, not a
geometry stream. Shipped as `spatial_network()` + `spatial_route()` +
`spatial_service_area()`, with a native-C binary-heap Dijkstra over a CSR
adjacency (`src/network.c`, no `igraph` dependency). The pieces noted as future
below -- contraction hierarchies, snap-to-edge origins, turn restrictions -- are
not yet built.

It still fits vectra's shape, though, by reusing the **resident-`y` streamed-`x`
family** (`spatial_knn`, `spatial_locate`, `spatial_split`): build the node-edge
graph from a line layer once, keep that graph resident, and stream the queries
past it one batch at a time -- one row per origin for a service area, one row per
(origin, destination) pair for a route or a cost-matrix cell. The graph is the
resident budget (bounded by the network size, like a resident `y`); the query
side scales like every other streamed verb. The graph never has to fit alongside
the queries, and the queries never have to fit alongside each other.

### The resident graph: `spatial_network()`

One new door builds the graph and returns a `vectra_network` object -- the
network counterpart of a resident `sf` `y`. Every query verb takes one.

```r
spatial_network(lines,
                weight    = NULL,          # edge cost column; NULL -> geometry length
                directed  = FALSE,
                direction = NULL,          # column of "B"/"FT"/"TF" (or +/-/0) one-way codes
                weight_to = NULL,          # reverse-direction cost on a directed graph
                tolerance = 0,             # snap endpoints within this distance to one node
                node_id   = NULL,          # carry a stable node identifier if present
                geom = "geometry", crs = NA)
```

Build is one pass, the sort/partition tier:

1. **Node the lines.** Split every line at its true intersections so a crossing
   becomes a shared vertex, reusing the GEOS noding already behind
   `spatial_overlay` / `spatial_split` (`st_node` / `GEOSNode`). Optional, gated
   by an argument: a road graph where bridges must *not* connect to the road
   under them needs raw endpoints, not noded crossings, so `node = FALSE` keeps
   each input line one edge.
2. **Dedup endpoints into node ids.** Snap coincident endpoints within
   `tolerance` to a single node (the existing magnitude-relative snap grid), hash
   the rounded coordinate to an integer node id. This is the same coincidence
   collapse `spatial_eliminate`'s union-find and the overlay deduper already do.
3. **Assemble CSR adjacency.** Compressed sparse row (one `int` offset per node,
   `int` target + `double` weight per directed edge) -- the cache-friendly layout
   Dijkstra wants, the same reason `.vtr` row groups are sized to L2/L3. A
   directed graph emits the forward edge with `weight` and, unless the
   `direction` code forbids it, the reverse with `weight_to %||% weight`.
4. **Index the nodes.** Keep a `GEOSSTRtree` over node coordinates (the overlay
   already builds these) so an off-network origin or destination snaps to its
   nearest node -- or, with `snap = "edge"`, to the nearest point on the nearest
   edge, splitting that edge virtually for the duration of one query.

The object holds the CSR graph, the node coordinates + STRtree, the per-edge
source line id (to rebuild route geometry), and the CRS. Peak memory is the graph,
roughly `(2 doubles + 1 int) * nodes + (1 int + 1 double) * edges` -- megabytes
for a national road network, resident for the life of the queries.

### Query verbs (streamed `x` against the resident graph)

Two query doors, split only where the **return genuinely differs in kind** (a
route is a line, a cost is a number, a service area is a polygon) -- the same test
that kept `spatial_overlay` one verb but `spatial_filter` separate from
`spatial_join`.

```r
# Point-to-point and origin-destination shortest paths.
spatial_route(x, network,
              to       = NULL,        # destination: a column of node ids, an sf layer, or coords
              geometry = TRUE,        # TRUE -> route lines; FALSE -> just the cost table (OD matrix)
              cost_col = "cost",
              ...)

# Reachability within a cost budget: service areas and isochrones.
spatial_service_area(x, network,
                     cost   = NULL,   # scalar budget, or c(5, 10, 15) for nested isochrone bands
                     output = c("polygon", "lines", "nodes"),
                     band_col = "band",
                     ...)
```

- **`spatial_route()`** snaps each streamed origin (and its paired destination)
  to a node, runs the solver, and emits one row per (origin, destination) carrying
  the total cost; with `geometry = TRUE` the row's geometry is the route line,
  rebuilt by walking the predecessor pointers and concatenating the source edges'
  coordinates. `geometry = FALSE` returns only the cost column, so the same verb
  is the **OD cost matrix** when `to` is a destination set per origin (one row per
  cell) -- route and matrix differ by one argument, not by a sibling function,
  exactly the two-layer-overlay model.
- **`spatial_service_area()`** runs one budget-bounded traversal per streamed
  origin and emits, per origin, the reached subnetwork: `output = "nodes"` the
  reachable nodes, `"lines"` the reachable edges, `"polygon"` their hull or buffer
  (the isochrone). A vector `cost` returns nested bands, one row per (origin,
  band), tagged in `band_col` -- travel-time isochrones fall straight out.

Both ride `.spatial_stream` like `spatial_knn`: resident graph, streamed `x`, run
files, a `ConcatNode` finalizer. The streaming contract holds -- peak query memory
is one solver's label array (`O(nodes)`) per worker thread, not the origin count.

### The solver (`src/network.c`, native C, `.Call`)

Per the no-dependency-shortcuts rule, the graph and the solver are native C, not
an `igraph` / `sfnetworks` dependency (a binary heap + Dijkstra over CSR is well
under 200 lines). The geometry side -- noding, snapping, route-line assembly --
goes through the libgeos C API already linked, not sf. sf stays a Suggests, used
in tests as ground truth and for vector I/O only; no `igraph` dependency is added,
keeping the self-contained-tarball property.

- **Dijkstra**, label-setting with a binary heap, one run per origin;
  early-terminate when every requested destination is settled (`spatial_route`)
  or the cost budget is exceeded (`spatial_service_area`).
- **Bidirectional Dijkstra** for a single (origin, destination) pair -- meet in
  the middle, roughly halving the settled set on a point-to-point route.
- **OpenMP across origins.** A batch of origins is embarrassingly parallel (the
  graph is read-only), one `#pragma omp parallel for` over the batch with a
  per-thread label/heap arena, the pattern `grepl`/`levenshtein` already use.
- **Contraction hierarchies** are the standard speedup for many queries on a
  large static graph: a one-time preprocessing pass (added to `spatial_network`
  as `prepare = TRUE`) that makes each query orders of magnitude faster. Default
  plain Dijkstra; CH is the noted future optimization, not the first cut.

Unreachable destinations return `Inf` cost and empty geometry rather than
dropping the row, so an OD matrix stays rectangular; connected-component counts
are reported at build time so a disconnected graph is visible, not silently wrong
(the input-totals sanity-check rule).

### Cost-model framing

This adds one entry to the three-tier vocabulary the docs already use:

- **Resident index, streamed probes.** The graph is built once (a sort/partition
  pass: node, dedup, index) and held resident; queries stream against it. The
  same shape as `spatial_knn` / `spatial_join`'s resident `y` and `spatial_locate`
  -- the resident object is a graph rather than an sf layer. Bounded by
  `max(graph, one query's frontier per thread)`, never by the query count.

### Build order (each phase ships independently, main branch)

1. **`spatial_network()` + CSR build + node STRtree** -- R front door over a C
   builder; recovery test that the graph's node/edge counts and a hand-checked
   adjacency match a fixture.
2. **`spatial_route()` (`geometry = FALSE`)** -- Dijkstra in `src/network.c`,
   cost only. The smallest correct slice; recovery test vs `igraph::distances`.
3. **`spatial_route()` geometry reconstruction** -- predecessor walk + edge
   concatenation; route equals the `sfnetworks` path on a fixture.
4. **`spatial_service_area()`** -- budget-bounded traversal, the three `output`
   modes, nested-band isochrones.
5. **Directed graphs + one-way codes**, snap-to-edge origins, bidirectional
   Dijkstra for single pairs, OpenMP across a batch.
6. **Contraction hierarchies** (`prepare = TRUE`) once a real network shows
   plain Dijkstra is the bottleneck.
7. A `vignettes/network.Rmd` showcase (a real road layer: route, OD matrix,
   isochrones) and a `_pkgdown.yml` reference section.

### Testing standard

Recovery against an established router as ground truth (Suggests-only, tests
only), never a shape smoke test:

- shortest-path cost matches `igraph::distances` cell-for-cell on a fixture graph;
- a reconstructed route equals the `sfnetworks` / `dodgr` path geometry;
- service-area node sets match an independent BFS/Dijkstra to the budget;
- streaming invariance: a multi-batch origin stream gives the identical result to
  one batch (a streamed path must equal the resident path; divergence is a bug).

### Out of scope (a further tier or a different package)

- **Turn restrictions and turn costs** -- need an expanded (edge-based) graph;
  note as a follow-on once the node-based engine lands.
- **Time-dependent / multimodal routing** (timetables, transfers) -- a different
  graph model and out of the static-geometry tier.
- **Map-matching GPS traces to the network** -- depends on external reference data
  and a probabilistic model; already listed under the out-of-scope geocoding/
  conflation group.

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

# Beyond spatial - other domains vectra could support

vectra's spatial support works because geometry fits a specific shape the engine
is good at: an **opaque self-describing blob per cell** (hex-WKB) that gets
**decoded and computed on per-row in C**, dispatched through `expr` (the `st_*`
family), parallelized with OpenMP, and streamed so the working set never has to
fit in RAM. Anything matching that shape is a natural extension. Candidates are
sorted by how cleanly they map onto the engine and how much they would matter to
climate-ecology / bioacoustics work.

## Tier 1 - best architectural fit + own domain

### N-dimensional climate cubes (NetCDF / Zarr / HDF5)

Where larger-than-RAM bites hardest in climate ecology. The `tiff_scan` pattern
(pixels -> rows with x, y, band) already proves the model; NetCDF/Zarr generalize
it to (lon, lat, time, level) -> rows with value columns.

- Slots in as a new `nc_scan` / `zarr_scan` backend next to `csv_scan` /
  `tiff_scan`, plus a `tbl_netcdf()` entry point.
- Hard part: chunk-aligned streaming so one Zarr/NetCDF chunk maps to one row
  group. This maps onto the existing row-group scan with zone-map pruning -- a
  bbox/time-range predicate prunes chunks the same way `==` prunes via the hash
  index.
- Payoff: turns "I can't open this CMIP6 file" into a lazy query.

### Genomic intervals + sequence data

Two sub-fits:

- **Interval overlap joins** (done). `interval_join()` overlaps each `x` row's
  `[start, end]` against every `y` row's range, with an optional equality `by`
  key (a chromosome) and `inner`/`left` modes. Both sides materialize resident,
  then a per-block sweep-line over the endpoints emits each overlapping pair
  once (output-sensitive, not all-pairs). The blocking-partition and
  materialization machinery is shared with the fuzzy join
  (`src/join_partition.c`); the matcher is `src/interval_join.c`.
- **Sequence ops** (`seq_*`: reverse-complement, k-mer, GC content, translate,
  edit-distance to a reference). These map onto `expr` exactly like `st_*` does --
  a self-describing string column (the sequence), decoded and computed per-row,
  OpenMP over rows. `levenshtein` / `dl_dist` are already parallelized; alignment
  is the same kernel shape. VCF/BED/FASTA scan backends feed it.

## Tier 2 - clean fit, broad appeal

### Vector / embedding columns + similarity search (done)

`as_embedding()` packs numeric vectors into a hex float32 blob stored in an
ordinary string column (the hex-WKB geometry precedent, kept ASCII so it
round-trips any codec). `cosine()`, `l2()`, and `dot()` decode the blob inside
the engine, one row per thread (`src/expr_vec.c`), against either a constant
query vector or a second embedding column. Nearest-neighbour search is
`mutate(d = cosine(emb, q)) |> slice_min(d, n = k)`, reusing the existing
`topn` node with no engine change. Restoring the dictionary-defer fast path for
duplicated wide-string columns (the known tdc regression) would also speed bulk
embedding scans.

### Time-series resampling + rolling ops (done)

Dates already ride as `VEC_DOUBLE` + a `Date`/`POSIXct` annotation, so the work
was expression and verb level: `floor_time(t, unit)` truncates an epoch column
to a calendar grid (`src/expr_datetime.c`), `resample(t, every, ...)` composes
`floor_time` + `group_by` + `summarise` for calendar-grid downsampling, and
`roll_sum`/`roll_mean`/`roll_min`/`roll_max`/`roll_n` are time-based trailing
windows on the existing window node (per-group sort then a two-pointer sweep,
monotonic deque for min/max; `src/window.c`). A floored/bucket column collects
as numeric epoch (the project node does not re-attach the date class to a
computed column); reclassing on the way out is the remaining nicety. Gap-filling
is not yet built.

## Tier 3 - possible, weaker fit

### Text corpora / full-text

The string + regex + fuzzy machinery is already rich; tokenization, n-grams,
TF-IDF, and a postings-list index would make vectra a larger-than-RAM corpus
engine. Fits, but less differentiated from existing tools than the tiers above.

### Audio / signal frames

Bioacoustics-relevant (spectrograms, MFCCs from larger-than-RAM audio), but
signal ops are windowed across rows rather than per-row, so they fight the
pull-based model more than geometry does. Doable as a windowed node, not as cheap
as the `st_*` analogy suggests.

## Suggested next steps

- **NetCDF/Zarr scan** -- most acute larger-than-RAM problem in the actual
  workflow, the `tiff_scan` backend already shows the path, and chunk-pruning
  reuses the existing zone-map machinery. The one heavy item left here: it needs
  an external library (netcdf-c / HDF5 or a Zarr reader), which cuts against the
  self-contained-tarball property, so it wants its own design pass.
- **Sequence ops** (`seq_*`: reverse-complement, k-mer, GC content, translate)
  -- the `expr_vec` / `expr_string` per-row decode shape now has two precedents
  to follow.
- Smaller follow-ons to what shipped: re-attach the date class to a resampled
  bucket column, gap-filling for time series, and a fused NN node if the
  distance-column memory ever matters.
