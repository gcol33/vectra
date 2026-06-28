# Topology verbs that build new geometry from a whole set of features in scope:
# polygonize (lines -> faces), line_merge (segments -> maximal lines), and
# coverage-preserving simplify, all on the partition tier shared with dissolve
# and construct; plus linear referencing (locate points along a resident line
# layer), a resident-y streamed verb in the spatial_knn / spatial_split family.

# -- polygonize (build polygonal faces from a line network) -------------------

# One group's worth of lines -> the polygonal faces they enclose. The lines are
# unioned and noded so every crossing becomes a shared vertex, then GEOS forms
# the faces of the resulting arrangement; faces carry only the group's `by`
# values, as the faces are new geometry with no single source feature.
.polygonize_fn <- function(by, geom, crs) function(df) {
  sb    <- .sf_decode_chunk(df, geom, NULL, crs)
  noded <- sf::st_node(sf::st_union(sf::st_geometry(sb)))
  faces <- sf::st_collection_extract(sf::st_polygonize(noded), "POLYGON")
  faces <- faces[!sf::st_is_empty(faces)]
  if (!length(faces)) return(NULL)
  rowdf <- if (is.null(by)) data.frame(matrix(nrow = length(faces), ncol = 0))
           else df[rep(1L, length(faces)), by, drop = FALSE]
  rowdf[[geom]] <- sf::st_as_binary(faces, hex = TRUE)
  rownames(rowdf) <- NULL
  rowdf
}

#' Build polygonal faces from a line network
#'
#' Forms the polygons enclosed by a set of lines (the QGIS "Polygonize", GEOS
#' `Polygonize`): the inverse of taking polygon boundaries. The lines of each
#' group are unioned and noded so every crossing becomes a shared vertex, then
#' the faces of that planar arrangement are returned, one per row. A pile of
#' lines that does not close any area yields no faces. Like [spatial_dissolve()]
#' and [spatial_construct()] it rides the **partition tier**: `x` is spilled once
#' and routed into one disjoint shard per `by` group in a single bounded pass,
#' then each group's lines are polygonized together. Peak memory is the routing
#' budget during the pass, then one group's geometry while its faces are built --
#' partition on a key whose groups fit in memory. With no `by`, the whole layer
#' yields one set of faces.
#'
#' Each face is new geometry built from the whole group, so it carries the `by`
#' columns only, not the attributes of any single source line. Geometry travels
#' through the engine as hex-encoded WKB in a string column and the CRS is
#' carried on the returned node; the noding is \pkg{sf}/GEOS and expects
#' projected or unprojected planar data. The \pkg{sf} package is an optional
#' dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param by Character vector of attribute columns to polygonize within: one set
#'   of faces per distinct combination of their values. `NULL` (default)
#'   polygonizes the whole layer at once.
#'
#' @return A `vectra_node` of one row per face, carrying the `by` columns and the
#'   input CRS, backed by temporary `.vtr` spills removed when the node is
#'   garbage-collected.
#'
#' @seealso [spatial_split()] to cut existing polygons by a blade,
#'   [spatial_construct()] for hulls and tessellations, [spatial_dissolve()] to
#'   merge geometries by group, [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' grid <- sf::st_sfc(
#'   sf::st_linestring(rbind(c(0, 0), c(2, 0))),
#'   sf::st_linestring(rbind(c(0, 1), c(2, 1))),
#'   sf::st_linestring(rbind(c(0, 2), c(2, 2))),
#'   sf::st_linestring(rbind(c(0, 0), c(0, 2))),
#'   sf::st_linestring(rbind(c(1, 0), c(1, 2))),
#'   sf::st_linestring(rbind(c(2, 0), c(2, 2))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   geometry = sf::st_as_binary(grid, hex = TRUE)
#' ), f)
#'
#' # The four unit cells enclosed by the grid of lines.
#' tbl(f) |> spatial_polygonize() |> collect_sf()
#' unlink(f)
#'
#' @export
spatial_polygonize <- function(x, by = NULL, geom = "geometry", crs = NA,
                               flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed line layer to polygonize)")
  crs <- .resolve_crs(x, crs)
  .partition_each(x, by, geom, crs, .polygonize_fn(by, geom, crs), flush_rows)
}

# -- line merge (sew contiguous segments into maximal lines) ------------------

# One group's worth of lines -> the maximal linestrings formed by sewing
# segments that meet end to end. The lines are unioned (which nodes them at
# crossings) and merged; the merged result is exploded so each maximal chain is
# one row, carrying the group's `by` values.
.line_merge_fn <- function(by, geom, crs) function(df) {
  sb   <- .sf_decode_chunk(df, geom, NULL, crs)
  m    <- sf::st_line_merge(sf::st_union(sf::st_geometry(sb)))
  segs <- sf::st_cast(m, "LINESTRING", warn = FALSE)
  segs <- segs[!sf::st_is_empty(segs)]
  if (!length(segs)) return(NULL)
  rowdf <- if (is.null(by)) data.frame(matrix(nrow = length(segs), ncol = 0))
           else df[rep(1L, length(segs)), by, drop = FALSE]
  rowdf[[geom]] <- sf::st_as_binary(segs, hex = TRUE)
  rownames(rowdf) <- NULL
  rowdf
}

#' Merge contiguous line segments into maximal lines
#'
#' Sews the line segments of each group into the longest possible linestrings
#' (`sf::st_line_merge`, the line counterpart of a dissolve): segments that meet
#' end to end become one chain, and each chain is emitted as its own row. Where a
#' plain union of lines returns a single multilinestring of all the parts, this
#' joins the parts through their shared endpoints; at a crossing where more than
#' two segments meet the merge is ambiguous and the segments stay separate. Like
#' [spatial_dissolve()] it rides the **partition tier**: `x` is spilled once and
#' routed into one disjoint shard per `by` group in a single bounded pass, then
#' each group's segments are merged together. Peak memory is the routing budget
#' during the pass, then one group's geometry while it is merged. With no `by`,
#' the whole layer is merged at once.
#'
#' Each merged line is new geometry built from the whole group, so it carries the
#' `by` columns only, not the attributes of any single source segment. Geometry
#' travels through the engine as hex-encoded WKB in a string column and the CRS
#' is carried on the returned node. The \pkg{sf} package is an optional
#' dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param by Character vector of attribute columns to merge within: one set of
#'   maximal lines per distinct combination of their values. `NULL` (default)
#'   merges the whole layer at once.
#'
#' @return A `vectra_node` of one row per maximal merged line, carrying the `by`
#'   columns and the input CRS, backed by temporary `.vtr` spills removed when
#'   the node is garbage-collected.
#'
#' @seealso [spatial_dissolve()] to union geometries by group,
#'   [spatial_explode()] for the opposite direction (multipart to single part),
#'   [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' seg <- sf::st_sfc(
#'   sf::st_linestring(rbind(c(0, 0), c(1, 0))),
#'   sf::st_linestring(rbind(c(1, 0), c(2, 0))),
#'   sf::st_linestring(rbind(c(2, 0), c(3, 0))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   geometry = sf::st_as_binary(seg, hex = TRUE)
#' ), f)
#'
#' # The three end-to-end segments become one line.
#' tbl(f) |> spatial_line_merge() |> collect_sf()
#' unlink(f)
#'
#' @export
spatial_line_merge <- function(x, by = NULL, geom = "geometry", crs = NA,
                               flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed line layer to merge)")
  crs <- .resolve_crs(x, crs)
  .partition_each(x, by, geom, crs, .line_merge_fn(by, geom, crs), flush_rows)
}

# -- coverage-preserving simplification ---------------------------------------

# One group's worth of polygons -> the same polygons with simplified boundaries,
# shared edges kept coincident. The boundaries are unioned (so a border shared by
# two polygons is one line) and noded into arcs at every junction, each arc is
# simplified once with topology-preserving Douglas-Peucker (its junction
# endpoints pinned), and the simplified arcs are re-polygonized into faces. Each
# face is matched back to the source polygon containing its interior point, so it
# inherits that feature's full attribute row -- a face that falls in no source
# polygon (a hole between shapes) is dropped.
.simplify_fn <- function(geom, crs, tolerance) function(df) {
  sb    <- .sf_decode_chunk(df, geom, NULL, crs)
  g     <- sf::st_geometry(sb)
  bdry  <- sf::st_node(sf::st_union(sf::st_boundary(g)))
  bsimp <- sf::st_simplify(bdry, dTolerance = tolerance, preserveTopology = TRUE)
  faces <- sf::st_collection_extract(
             sf::st_polygonize(sf::st_union(bsimp)), "POLYGON")
  faces <- faces[!sf::st_is_empty(faces)]
  if (!length(faces)) return(NULL)
  ip  <- suppressWarnings(sf::st_point_on_surface(faces))
  hit <- sf::st_within(ip, g)
  idx <- vapply(hit, function(h) if (length(h)) h[[1L]] else NA_integer_,
                integer(1))
  keep <- !is.na(idx)
  if (!any(keep)) return(NULL)
  faces <- faces[keep]
  idx   <- idx[keep]
  rowdf <- df[idx, setdiff(names(df), geom), drop = FALSE]
  rowdf[[geom]] <- sf::st_as_binary(faces, hex = TRUE)
  rownames(rowdf) <- NULL
  rowdf
}

#' Simplify a polygon coverage without tearing shared edges
#'
#' Simplifies polygon boundaries while keeping a shared border between two
#' polygons identical on both sides, so adjacent polygons stay edge-matched with
#' no slivers or gaps -- the topology-preserving simplification that a per-feature
#' `spatial_map(~ sf::st_simplify(.x))` cannot give, because it simplifies each
#' polygon's copy of a shared border independently. The boundaries of each group
#' are unioned so a shared border is one line, noded into arcs at every junction,
#' each arc simplified once (its junction endpoints pinned), and the arcs
#' re-polygonized; each resulting face inherits the attributes of the source
#' polygon containing it. Like [spatial_dissolve()] it rides the **partition
#' tier**: `x` is spilled once and routed into one disjoint shard per `by` group
#' in a single bounded pass, and each group is simplified as an independent
#' coverage. Peak memory is the routing budget during the pass, then one group's
#' geometry while it is simplified -- partition on a key whose groups fit in
#' memory. With no `by`, the whole layer is one coverage.
#'
#' The simplification is topology-preserving Douglas-Peucker (`dTolerance =
#' tolerance`) on the noded boundary arcs. Geometry travels through the engine as
#' hex-encoded WKB in a string column and the CRS is carried on the returned
#' node; the noding is \pkg{sf}/GEOS and expects projected or unprojected planar
#' data. The \pkg{sf} package is an optional dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param tolerance Distance tolerance for the boundary simplification, in CRS
#'   units: vertices that deviate less than this from the simplified line are
#'   dropped. Larger values simplify more.
#' @param by Character vector of attribute columns whose groups are each
#'   simplified as an independent coverage. `NULL` (default) treats the whole
#'   layer as one coverage.
#'
#' @return A `vectra_node` of the simplified polygons, each carrying its source
#'   feature's attributes and the input CRS, backed by temporary `.vtr` spills
#'   removed when the node is garbage-collected.
#'
#' @seealso [spatial_map()] with `~ sf::st_simplify(.x)` for independent
#'   per-feature simplification, [spatial_smooth()] for Chaikin corner-rounding,
#'   [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' p1 <- sf::st_polygon(list(rbind(
#'   c(0, 0), c(1, 0), c(1, 0.5), c(1, 1), c(0, 1), c(0, 0))))
#' p2 <- sf::st_polygon(list(rbind(
#'   c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0.5), c(1, 0))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   id = c("a", "b"),
#'   geometry = sf::st_as_binary(sf::st_sfc(p1, p2), hex = TRUE)
#' ), f)
#'
#' # The shared edge is simplified once, so the two polygons stay edge-matched.
#' tbl(f) |> spatial_simplify(tolerance = 0.6) |> collect_sf()
#' unlink(f)
#'
#' @export
spatial_simplify <- function(x, tolerance, by = NULL, geom = "geometry",
                             crs = NA, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed polygon coverage to simplify)")
  if (!is.numeric(tolerance) || length(tolerance) != 1L ||
      !is.finite(tolerance) || tolerance <= 0)
    stop("`tolerance` must be a single positive number (the simplify distance)")
  crs <- .resolve_crs(x, crs)
  .partition_each(x, by, geom, crs, .simplify_fn(geom, crs, tolerance),
                  flush_rows)
}

# -- linear referencing (locate points along a resident line layer) -----------

# For one decoded batch of points, find the nearest resident line and the
# position of each point along it. Returns the batch plus the line identifier,
# the measure (distance along the line from its start to the projected point),
# and the perpendicular distance to the line; with `snap = TRUE` the point
# geometry is replaced by its projection onto the line.
.locate_batch <- function(sb, yg, yid, id_col, measure_col, dist_col, snap) {
  if (nrow(sb) == 0L) {
    sb[[id_col]]      <- yid[integer(0)]
    sb[[measure_col]] <- numeric(0)
    sb[[dist_col]]    <- numeric(0)
    return(sb)
  }
  pts  <- sf::st_geometry(sb)
  nf   <- sf::st_nearest_feature(pts, yg)
  ln   <- yg[nf]
  meas <- as.numeric(sf::st_line_project(ln, pts))
  dper <- as.numeric(sf::st_distance(pts, ln, by_element = TRUE))
  if (snap)
    sf::st_geometry(sb) <- sf::st_line_interpolate(ln, meas)
  sb[[id_col]]      <- yid[nf]
  sb[[measure_col]] <- meas
  sb[[dist_col]]    <- dper
  rownames(sb) <- NULL
  sb
}

#' Locate streamed points along a resident line layer
#'
#' Streams a large point layer `x` through the engine and, for each point, finds
#' the nearest line of a small resident `line` layer and where the point falls
#' along it -- linear referencing (`sf::st_line_project`). Each point gets the
#' identifier of its nearest line, the **measure** (distance along that line from
#' its start to the point's projection), and the perpendicular distance to the
#' line. With `snap = TRUE` the point geometry is moved onto the line at that
#' measure. This is the two-layer companion to a per-feature
#' `sf::st_line_interpolate`, which goes the other way (a measure back to a
#' point); the billion-row point stream never materializes, while `line` (the
#' reference network) stays resident.
#'
#' Nearest line and distance are \pkg{sf}'s [sf::st_nearest_feature] and
#' [sf::st_distance]: planar (CRS units) on projected or unprojected planar data,
#' great-circle (metres) on geographic coordinates with spherical geometry on
#' (`sf::sf_use_s2()`). Points arrive either as a hex-WKB geometry column
#' (`geom`) or as two coordinate columns (`coords`). The \pkg{sf} package is an
#' optional dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param line An `sf` or `sfc` object of the reference lines (the resident
#'   layer).
#' @param y_id Optional name of a column in `line` whose value identifies the
#'   matched line in the output. Default `NULL` uses `line`'s 1-based row index.
#' @param id_col,measure_col,dist_col Names of the output columns holding the
#'   matched-line identifier, the measure along the line, and the perpendicular
#'   distance. Defaults `"line"`, `"measure"`, `"distance"`.
#' @param snap If `TRUE`, replace each point's geometry with its projection onto
#'   the nearest line. Default `FALSE` keeps the original points.
#'
#' @return A `vectra_node` of `x`'s rows -- geometry included (or snapped onto
#'   the line) -- plus the matched-line identifier, the measure, and the
#'   perpendicular distance, backed by temporary `.vtr` spills (removed when the
#'   node is garbage-collected) and carrying the input CRS.
#'
#' @seealso [spatial_knn()] for nearest neighbours with distances,
#'   [spatial_join()] for a nearest-feature attribute join, [spatial_map()] with
#'   `~ sf::st_line_interpolate(line, .x$m)` for the inverse, [collect_sf()] to
#'   materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' line <- sf::st_sfc(
#'   sf::st_linestring(rbind(c(0, 0), c(10, 0))),
#'   sf::st_linestring(rbind(c(0, 5), c(0, 15))))
#' line <- sf::st_sf(road = c("main", "side"), geometry = line)
#' pts  <- data.frame(id = 1:2, x = c(3, 1), y = c(1, 9))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(pts, f)
#'
#' # Each point's position along its nearest road.
#' tbl(f) |>
#'   spatial_locate(line, coords = c("x", "y"), y_id = "road") |>
#'   collect()
#' unlink(f)
#'
#' @export
spatial_locate <- function(x, line, geom = "geometry", coords = NULL, crs = NA,
                           y_id = NULL, id_col = "line", measure_col = "measure",
                           dist_col = "distance", snap = FALSE, out_geom = NULL,
                           flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed point layer)")
  if (!inherits(line, "sf") && !inherits(line, "sfc"))
    stop("`line` must be an sf or sfc object (the resident reference lines)")
  crs  <- .resolve_crs(x, crs)
  line <- .align_resident_crs(line, crs)
  yg   <- sf::st_geometry(line)
  yid  <- if (is.null(y_id)) seq_len(length(yg)) else {
    if (!inherits(line, "sf") || !y_id %in% names(line))
      stop(sprintf("`y_id` column '%s' not found in `line`", y_id))
    line[[y_id]]
  }
  if (is.null(out_geom)) out_geom <- if (is.null(coords)) geom else "geometry"
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  batch_fn <- function(sb)
    .locate_batch(sb, yg, yid, id_col, measure_col, dist_col, snap)
  .spatial_stream(x, batch_fn, geom, coords, crs, out_geom, fr)
}

# -- centerline (medial axis of a polygon) ------------------------------------

# Approximate the medial axis (centerline) of one polygon from the Voronoi
# diagram of its densified boundary: the Voronoi edges that lie inside the
# polygon trace the points equidistant from two boundary stretches, i.e. its
# skeleton. The boundary is densified to spacing `density` so the diagram is fine
# enough; interior edges are kept (their midpoint inside a slightly shrunk
# polygon), merged into maximal lines, and optionally pruned of branches shorter
# than `prune`. A non-polygon geometry, or one too small to sample, returns
# unchanged so a feature never vanishes.
.centerline_one <- function(g, density, prune, crs) {
  gs <- sf::st_sfc(g, crs = crs)
  if (!grepl("POLYGON", class(g)[2L])) return(gs)
  bb   <- sf::st_bbox(gs)
  diag <- sqrt((bb[["xmax"]] - bb[["xmin"]])^2 + (bb[["ymax"]] - bb[["ymin"]])^2)
  d <- if (!is.null(density) && density > 0) density else diag / 100
  if (!is.finite(d) || d <= 0) return(gs)
  b   <- sf::st_segmentize(sf::st_cast(gs, "MULTILINESTRING"), dfMaxLength = d)
  v   <- sf::st_cast(b, "POINT")
  vor <- sf::st_collection_extract(sf::st_voronoi(sf::st_union(v)), "POLYGON")
  edges <- sf::st_cast(sf::st_union(sf::st_boundary(vor)), "LINESTRING",
                       warn = FALSE)
  edges <- sf::st_cast(sf::st_node(sf::st_union(edges)), "LINESTRING",
                       warn = FALSE)
  if (!length(edges)) return(gs)
  mid    <- sf::st_line_sample(edges, sample = 0.5)
  inside <- lengths(sf::st_within(mid, sf::st_buffer(gs, -d * 0.01))) > 0
  ce <- edges[inside]
  if (!length(ce)) return(gs)
  m <- sf::st_cast(sf::st_line_merge(sf::st_union(ce)), "LINESTRING",
                   warn = FALSE)
  if (prune > 0) {
    keep <- as.numeric(sf::st_length(m)) >= prune
    if (any(keep)) m <- m[keep]
  }
  if (!length(m)) gs else m
}

.centerline_batch <- function(sb, density, prune, crs) {
  if (nrow(sb) == 0L) return(sb)
  crs <- .as_crs(crs)
  g   <- sf::st_geometry(sb)
  pieces <- lapply(seq_along(g),
                   function(i) .centerline_one(g[[i]], density, prune, crs))
  np  <- lengths(pieces)
  out <- sb[rep.int(seq_len(nrow(sb)), np), , drop = FALSE]
  sf::st_geometry(out) <- sf::st_sfc(unlist(pieces, recursive = FALSE), crs = crs)
  rownames(out) <- NULL
  out
}

#' Trace the centerline (medial axis) of streamed polygons
#'
#' Approximates the centerline of every polygon in a streamed layer, one batch
#' at a time -- the medial axis a per-feature transform such as a buffer cannot
#' produce. Each polygon's boundary is densified and its Voronoi diagram taken;
#' the Voronoi edges that fall inside the polygon trace the points equidistant
#' from two stretches of boundary, which is its skeleton, and they are merged
#' into maximal lines. This is the usual approximation for river or road
#' centerlines from a filled shape; `prune` drops the short branches that the
#' skeleton grows toward convex corners. A non-polygon geometry passes through
#' unchanged.
#'
#' The centerline is an approximation whose detail is set by `density` (the
#' boundary sampling spacing): finer sampling traces the axis more closely at
#' more cost. Geometry travels through the engine as hex-encoded WKB in a string
#' column and the CRS is carried on the returned node; the Voronoi construction
#' is \pkg{sf}/GEOS and expects projected or unprojected planar data. The
#' \pkg{sf} package is an optional dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param density Boundary sampling spacing in CRS units: the polygon outline is
#'   densified to at most this vertex spacing before the Voronoi diagram is
#'   built. `NULL` (default) uses one-hundredth of each polygon's bounding-box
#'   diagonal.
#' @param prune Drop centerline branches shorter than this length (CRS units),
#'   removing the short spurs the skeleton grows toward convex corners. `0`
#'   (default) keeps every branch.
#'
#' @return A `vectra_node` of the centerlines, each carrying its source polygon's
#'   attributes (replicated if the centerline is several lines) and the input
#'   CRS, backed by temporary `.vtr` spills removed when the node is
#'   garbage-collected.
#'
#' @seealso [spatial_construct()] with `kind = "pole"` for the single deepest
#'   interior point, [spatial_simplify()] to simplify a coverage, [collect_sf()]
#'   to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' road <- sf::st_polygon(list(rbind(
#'   c(0, 0), c(10, 0), c(10, 2), c(0, 2), c(0, 0))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   geometry = sf::st_as_binary(sf::st_sfc(road), hex = TRUE)
#' ), f)
#'
#' # The centerline runs down the middle of the strip.
#' tbl(f) |> spatial_centerline(density = 0.25, prune = 0.5) |> collect_sf()
#' unlink(f)
#'
#' @export
spatial_centerline <- function(x, density = NULL, prune = 0,
                               geom = "geometry", crs = NA, out_geom = NULL,
                               flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed polygon layer)")
  if (!is.null(density) && (!is.numeric(density) || length(density) != 1L ||
                            !is.finite(density) || density <= 0))
    stop("`density` must be a single positive number, or NULL")
  if (!is.numeric(prune) || length(prune) != 1L || !is.finite(prune) ||
      prune < 0)
    stop("`prune` must be a single non-negative number")
  crs <- .resolve_crs(x, crs)
  if (is.null(out_geom)) out_geom <- geom
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  .spatial_stream(x, function(sb) .centerline_batch(sb, density, prune, crs),
                  geom, coords = NULL, crs = crs, out_geom = out_geom,
                  flush_rows = fr)
}

# -- planar topology (shared-edge arc table of a coverage) --------------------

# One group's worth of polygons -> the arcs of their shared-edge topology. The
# boundaries are unioned (so a border shared by two polygons is one line) and
# noded at every junction; each resulting arc is emitted once, tagged with the
# identifiers of the (up to two) polygons whose boundary covers it -- two for an
# internal shared edge, one for an outer edge (the other side `NA`). The arc
# identifiers come from `id` (a column of `x`) or the 1-based feature order.
.topology_fn <- function(id, by, geom, crs, face_cols) function(df) {
  sb   <- .sf_decode_chunk(df, geom, NULL, crs)
  g    <- sf::st_geometry(sb)
  bnd  <- sf::st_boundary(g)
  arcs <- sf::st_cast(sf::st_node(sf::st_union(bnd)), "LINESTRING", warn = FALSE)
  arcs <- arcs[!sf::st_is_empty(arcs)]
  if (!length(arcs)) return(NULL)
  fid <- if (is.null(id)) seq_len(nrow(df)) else df[[id]]
  nb  <- lapply(seq_along(arcs), function(i)
    fid[vapply(seq_along(g),
               function(j) length(sf::st_covered_by(arcs[i], bnd[j])[[1L]]) > 0,
               logical(1))])
  pick <- function(k) {
    out <- fid[rep(NA_integer_, length(nb))]
    for (i in seq_along(nb)) if (length(nb[[i]]) >= k) out[i] <- nb[[i]][[k]]
    out
  }
  rowdf <- if (is.null(by)) data.frame(matrix(nrow = length(arcs), ncol = 0))
           else df[rep(1L, length(arcs)), by, drop = FALSE]
  rowdf[[face_cols[1L]]] <- pick(1L)
  rowdf[[face_cols[2L]]] <- pick(2L)
  rowdf[[geom]] <- sf::st_as_binary(arcs, hex = TRUE)
  rownames(rowdf) <- NULL
  rowdf
}

#' Build the shared-edge topology of a polygon coverage
#'
#' Decomposes a polygon coverage into the arcs of its planar topology: each
#' border is returned once, tagged with the polygons on either side. Where the
#' raw boundaries of adjacent polygons each carry their own copy of a shared
#' edge, this nodes the unioned boundaries so a shared border is a single arc
#' carrying the identifiers of both neighbours -- the "build topology" of a GIS,
#' and the adjacency a dissolve or a coverage edit needs. An internal arc names
#' two faces; an outer arc names one and leaves the other side `NA`. Like
#' [spatial_dissolve()] it rides the **partition tier**: `x` is spilled once and
#' routed into one disjoint shard per `by` group in a single bounded pass, and
#' each group is treated as an independent coverage. Peak memory is the routing
#' budget during the pass, then one group's geometry while its arcs are built.
#'
#' Geometry travels through the engine as hex-encoded WKB in a string column and
#' the CRS is carried on the returned node; the noding is \pkg{sf}/GEOS and
#' expects projected or unprojected planar data. The \pkg{sf} package is an
#' optional dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param id Optional name of a column of `x` whose value identifies each polygon
#'   in the face columns. `NULL` (default) uses the 1-based feature order within
#'   the group.
#' @param by Character vector of attribute columns whose groups are each built as
#'   an independent coverage. `NULL` (default) treats the whole layer as one
#'   coverage.
#' @param face_cols Length-2 character vector naming the two output columns that
#'   hold the identifiers of the polygons on either side of each arc. Default
#'   `c("face1", "face2")`.
#'
#' @return A `vectra_node` of one row per arc -- the arc geometry plus the two
#'   face-identifier columns (and any `by` columns) -- carrying the input CRS and
#'   backed by temporary `.vtr` spills removed when the node is garbage-collected.
#'
#' @seealso [spatial_polygonize()] to rebuild faces from arcs,
#'   [spatial_dissolve()] to merge geometries by group, [collect_sf()] to
#'   materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' p1 <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
#' p2 <- sf::st_polygon(list(rbind(c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   id = c("a", "b"),
#'   geometry = sf::st_as_binary(sf::st_sfc(p1, p2), hex = TRUE)
#' ), f)
#'
#' # The shared edge appears once, tagged with both neighbours.
#' tbl(f) |> spatial_topology(id = "id") |> collect()
#' unlink(f)
#'
#' @export
spatial_topology <- function(x, id = NULL, by = NULL, geom = "geometry",
                             crs = NA, face_cols = c("face1", "face2"),
                             flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed polygon coverage)")
  if (!is.null(id) && (!is.character(id) || length(id) != 1L))
    stop("`id` must be a single column name, or NULL")
  if (!is.character(face_cols) || length(face_cols) != 2L ||
      anyNA(face_cols) || face_cols[1L] == face_cols[2L])
    stop("`face_cols` must be two distinct column names")
  crs <- .resolve_crs(x, crs)
  .partition_each(x, by, geom, crs, .topology_fn(id, by, geom, crs, face_cols),
                  flush_rows, need = id)
}

# -- eliminate (merge sliver polygons into a neighbour) -----------------------

# One group's worth of polygons -> the same coverage with every feature smaller
# than `max_area` absorbed into a neighbour. Adjacent features are found by
# intersection; each small feature is linked to the neighbour it shares the
# longest border with (or the largest-area neighbour), and an area-rooted
# union-find collapses chains of slivers so each connected run flows to its
# single largest member, whose attribute row survives. A small feature with no
# neighbour is kept unchanged, so nothing vanishes.
.eliminate_fn <- function(by, geom, crs, max_area, into) function(df) {
  sb   <- .sf_decode_chunk(df, geom, NULL, crs)
  g    <- sf::st_geometry(sb)
  n    <- length(g)
  if (!n) return(NULL)
  if (n == 1L) return(df)
  cols  <- setdiff(names(df), geom)
  area  <- as.numeric(sf::st_area(g))
  small <- area < max_area
  parent <- seq_len(n)
  find <- function(i) {
    while (parent[i] != i) { parent[i] <<- parent[parent[i]]; i <- parent[i] }
    i
  }
  if (any(small)) {
    touch <- sf::st_intersects(g)
    bnd   <- if (into == "longest_border") sf::st_boundary(g) else NULL
    for (i in which(small)) {
      cand <- setdiff(touch[[i]], i)
      if (!length(cand)) next
      if (into == "longest_border") {
        sl <- vapply(cand, function(j) {
          ix <- suppressWarnings(sf::st_intersection(bnd[i], bnd[j]))
          if (length(ix)) sum(as.numeric(sf::st_length(ix))) else 0
        }, numeric(1))
        best <- cand[which.max(sl)]
      } else {
        best <- cand[which.max(area[cand])]
      }
      ri <- find(i); rb <- find(best)
      if (ri != rb) {
        if (area[rb] >= area[ri]) parent[ri] <- rb else parent[rb] <- ri
      }
    }
  }
  roots <- vapply(seq_len(n), find, integer(1))
  ur    <- unique(roots)
  geoms <- do.call(c, lapply(ur, function(r) sf::st_union(g[roots == r])))
  rowdf <- df[ur, cols, drop = FALSE]
  rowdf[[geom]] <- sf::st_as_binary(geoms, hex = TRUE)
  rownames(rowdf) <- NULL
  rowdf
}

#' Merge sliver polygons into a neighbour
#'
#' Cleans a polygon coverage by absorbing every feature whose area is below
#' `max_area` into an adjacent feature (the QGIS "Eliminate Selected Polygons"):
#' the sliver removal a per-feature transform cannot do, because the target a
#' sliver merges into is one of its neighbours, not the sliver itself. Each small
#' feature is joined to the neighbour it shares the longest border with (or the
#' largest-area neighbour, with `into = "largest_area"`); chains of slivers
#' collapse so a connected run of small features flows to its single largest
#' member, whose attribute row survives. A small feature with no neighbour is
#' kept unchanged, so nothing vanishes. Like [spatial_dissolve()] it rides the
#' **partition tier**: `x` is spilled once and routed into one disjoint shard per
#' `by` group in a single bounded pass, and each group is cleaned as an
#' independent coverage. Peak memory is the routing budget during the pass, then
#' one group's geometry while its slivers are merged -- partition on a key whose
#' groups fit in memory. With no `by`, the whole layer is one coverage.
#'
#' Adjacency and shared-border length are \pkg{sf}/GEOS ([sf::st_intersects],
#' [sf::st_boundary], [sf::st_intersection]) and expect projected or unprojected
#' planar data; `max_area` is in CRS units squared. Geometry travels through the
#' engine as hex-encoded WKB in a string column and the CRS is carried on the
#' returned node. The \pkg{sf} package is an optional dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param max_area Area threshold in CRS units squared: a feature smaller than
#'   this is a sliver and is merged into a neighbour. Larger values absorb more.
#' @param by Character vector of attribute columns whose groups are each cleaned
#'   as an independent coverage. `NULL` (default) treats the whole layer as one
#'   coverage.
#' @param into How to pick the neighbour a sliver merges into: `"longest_border"`
#'   (default) the neighbour sharing the longest boundary, or `"largest_area"`
#'   the neighbour with the greatest area.
#'
#' @return A `vectra_node` of the cleaned coverage -- one row per surviving
#'   feature, each carrying its (largest member's) attributes and the input CRS,
#'   backed by temporary `.vtr` spills removed when the node is garbage-collected.
#'
#' @seealso [spatial_dissolve()] to merge geometries by attribute,
#'   [spatial_simplify()] for coverage-preserving simplification,
#'   [spatial_topology()] for the shared-edge adjacency, [collect_sf()] to
#'   materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' big    <- sf::st_polygon(list(rbind(
#'   c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0))))
#' sliver <- sf::st_polygon(list(rbind(
#'   c(10, 0), c(10.3, 0), c(10.3, 10), c(10, 10), c(10, 0))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   id = c("keep", "sliver"),
#'   geometry = sf::st_as_binary(sf::st_sfc(big, sliver), hex = TRUE)
#' ), f)
#'
#' # The thin sliver is absorbed into the square it borders.
#' tbl(f) |> spatial_eliminate(max_area = 5) |> collect_sf()
#' unlink(f)
#'
#' @export
spatial_eliminate <- function(x, max_area, by = NULL,
                              into = c("longest_border", "largest_area"),
                              geom = "geometry", crs = NA, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed polygon coverage to clean)")
  if (!is.numeric(max_area) || length(max_area) != 1L || !is.finite(max_area) ||
      max_area <= 0)
    stop("`max_area` must be a single positive number (the sliver area threshold)")
  into <- match.arg(into)
  crs  <- .resolve_crs(x, crs)
  .partition_each(x, by, geom, crs, .eliminate_fn(by, geom, crs, max_area, into),
                  flush_rows)
}

# The partition tier shared with dissolve and construct. Routes `x` into one
# shard per `by` group on disk in a single bounded pass, then applies
# `group_fn(df)` to each shard's collected rows (geometry as hex-WKB in `geom`),
# accumulating the returned data frames into one node. Peak memory is the
# routing budget, then one group's geometry while `group_fn` runs.
.partition_each <- function(x, by, geom, crs, group_fn, flush_rows,
                            need = NULL) {
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed layer)")
  if (!is.null(by) && !is.character(by))
    stop("`by` must be a character vector of column names, or NULL")

  spill <- tempfile(fileext = ".vtr")
  on.exit(unlink(spill), add = TRUE)
  write_vtr(x, spill)

  schema <- .Call(C_node_schema, tbl(spill)$.node)
  miss <- setdiff(c(by, geom, need), schema$name)
  if (length(miss))
    stop(sprintf("column(s) not found in the stream: %s",
                 paste(miss, collapse = ", ")))

  budget <- getOption("vectra.partition_budget", .PARTITION_BUDGET)
  res <- .partition_router(spill, .dissolve_assign(by), budget)
  on.exit(unlink(unlist(res$runs, use.names = FALSE)), add = TRUE)

  fr  <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)
  for (lab in sort(names(res$runs))) {
    df  <- collect(.concat_runs(res$runs[[lab]]))
    out <- group_fn(df)
    if (!is.null(out) && nrow(out)) acc$push(.coerce_for_vtr(out))
  }
  acc$finish(crs = crs, empty_geom = geom)
}
