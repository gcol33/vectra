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

# The partition tier shared with dissolve and construct. Routes `x` into one
# shard per `by` group on disk in a single bounded pass, then applies
# `group_fn(df)` to each shard's collected rows (geometry as hex-WKB in `geom`),
# accumulating the returned data frames into one node. Peak memory is the
# routing budget, then one group's geometry while `group_fn` runs.
.partition_each <- function(x, by, geom, crs, group_fn, flush_rows) {
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed layer)")
  if (!is.null(by) && !is.character(by))
    stop("`by` must be a character vector of column names, or NULL")

  spill <- tempfile(fileext = ".vtr")
  on.exit(unlink(spill), add = TRUE)
  write_vtr(x, spill)

  schema <- .Call(C_node_schema, tbl(spill)$.node)
  miss <- setdiff(c(by, geom), schema$name)
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
