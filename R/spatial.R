# spatial.R - streamed spatial operations via sf.
#
# vectra has no geometry type and no GEOS: topology stays sf's job. These verbs
# instead stream a larger-than-RAM query through the engine one batch at a time,
# hand each batch to sf for the actual geometry work, and spill the transformed
# batches back to disk as a fresh lazy node. Geometry rides through the engine as
# hex-encoded WKB in an ordinary string column, so no new VecType is needed; the
# CRS is carried on the returned node (the .vtr file stores no CRS).
#
# One engine (.spatial_stream) drives both front doors:
#   spatial_map(x, fn)        per-feature transforms (buffer, transform, area, ...)
#   spatial_join(x, y, join)  big-x streamed against a resident small-y sf object
# This mirrors offload(by = ...): batch cursor -> per-batch work -> run-files ->
# a ConcatNode with a finalizer that clears the temp spills.

.SPATIAL_FLUSH <- 5e5      # transformed rows buffered before a run-file flush

.check_sf <- function() {
  if (!requireNamespace("sf", quietly = TRUE))
    stop("spatial operations require the 'sf' package; install it with ",
         "install.packages(\"sf\")", call. = FALSE)
}

# Default sentinel-aware CRS resolution: an explicit `crs` wins; otherwise
# inherit the CRS the upstream node carries (so spatial_map() |> spatial_join()
# keeps the projection without re-stating it).
.resolve_crs <- function(x, crs) {
  if ((identical(crs, NA) || is.null(crs)) && !is.null(x$.crs)) return(x$.crs)
  crs
}

# -- geometry transport (hex WKB in a string column) --------------------------

# Decode one engine batch (a data.frame) into an sf object. Either a geometry
# column of hex WKB / WKT strings (`geom`), or point coordinates assembled from
# two numeric columns (`coords = c("x", "y")`), in which case the coordinate
# columns are kept so downstream work can still see them.
# Normalize any CRS input (including the logical-NA default) to an sf crs
# object, since sf rejects a bare logical NA where it wants NA_crs_.
.as_crs <- function(crs) {
  if (identical(crs, NA) || is.null(crs)) sf::st_crs(NA) else sf::st_crs(crs)
}

.sf_decode_chunk <- function(chunk, geom, coords, crs) {
  crs <- .as_crs(crs)
  if (!is.null(coords)) {
    miss <- setdiff(coords, names(chunk))
    if (length(miss))
      stop(sprintf("coords column(s) not found: %s",
                   paste(miss, collapse = ", ")))
    return(sf::st_as_sf(chunk, coords = coords, crs = crs, remove = FALSE))
  }
  if (!geom %in% names(chunk))
    stop(sprintf("geometry column '%s' not found; pass geom= or coords=", geom))
  # st_as_sfc.character parses WKT only; hex WKB must be tagged so the WKB
  # reader runs, which round-trips coordinates losslessly (WKT would round to
  # the print precision).
  g <- sf::st_as_sfc(structure(chunk[[geom]], class = "WKB"), EWKB = FALSE)
  g <- sf::st_set_crs(g, crs)
  rest <- chunk[setdiff(names(chunk), geom)]
  sf::st_sf(rest, geometry = g)
}

# Coerce a data.frame's columns to types the .vtr writer accepts (int64 /
# double / bool / string). sf attribute columns can arrive as factors, dates, or
# units; those collapse to character. List columns other than the geometry we
# already encoded are an error.
.coerce_for_vtr <- function(df) {
  for (nm in names(df)) {
    col <- df[[nm]]
    if (is.factor(col) || inherits(col, "Date") || inherits(col, "POSIXt")) {
      df[[nm]] <- as.character(col)
    } else if (is.integer(col) || is.double(col) ||
               is.logical(col) || is.character(col)) {
      # already writable
    } else if (is.list(col)) {
      stop(sprintf("column '%s' is a list column and cannot be written; ",
                   "drop it before the spatial step", nm))
    } else {
      df[[nm]] <- as.character(col)
    }
  }
  df
}

# Encode the per-batch sf result back into a writable data.frame: the active
# geometry becomes hex WKB in `out_geom`, the rest are coerced attribute columns.
# A plain data.frame (geometry deliberately dropped, e.g. a summary) passes
# through unchanged apart from type coercion.
.sf_encode_result <- function(res, out_geom) {
  if (inherits(res, "sfc")) res <- sf::st_sf(geometry = res)
  if (!inherits(res, "sf")) {
    if (is.data.frame(res)) return(.coerce_for_vtr(as.data.frame(res)))
    stop("spatial batch function must return an sf object, sfc, or data.frame")
  }
  g  <- sf::st_geometry(res)
  df <- as.data.frame(sf::st_drop_geometry(res))
  df[[out_geom]] <- sf::st_as_binary(g, hex = TRUE)
  .coerce_for_vtr(df)
}

# -- self-overlay (QGIS-style Union) ------------------------------------------

# Connected components of the polygon overlap graph, via union-find over the
# sparse adjacency from sf::st_intersects(). Only overlapping polygons can share
# an overlay piece, so each component is overlaid independently -- exact tiling
# with no tile-edge artefacts, and bounded memory.
.overlap_components <- function(hits, n) {
  parent <- seq_len(n)
  find <- function(i) { while (parent[i] != i) i <- parent[i]; i }
  for (i in seq_len(n)) for (j in hits[[i]]) if (j > i) {
    ri <- find(i); rj <- find(j)
    if (ri != rj) parent[rj] <- ri
  }
  vapply(seq_len(n), find, integer(1L))
}

# Robust fixed-precision grid for the overlay. Snapping coordinates to ~1e-7 of
# their magnitude leaves about seven significant digits -- well inside double
# precision, so GEOS overlays on a fixed-precision model and the pieces come out
# exactly disjoint, while staying fine enough not to distort features. Magnitude
# (not extent) keeps it stable against far-flung outliers. Returns NULL when no
# sensible grid exists (degenerate or all-zero coordinates).
.robust_precision <- function(x) {
  mag <- max(abs(sf::st_bbox(x)))
  if (!is.finite(mag) || mag == 0) return(NULL)
  1 / (mag * 1e-7)
}

# -- the streaming engine -----------------------------------------------------

# Pull `x` one batch at a time, run `batch_fn` (an sf-in / sf-out function) on
# each, encode the result, and accumulate to run-files flushed at `flush_rows`.
# Returns a lazy ConcatNode over the run-files, carrying the output CRS and a
# finalizer that removes the spills when the node is garbage-collected.
.spatial_stream <- function(x, batch_fn, geom, coords, crs, out_geom,
                            flush_rows) {
  nxt <- .batch_cursor(x)
  st <- new.env(parent = emptyenv())
  st$buf <- list(); st$buffered <- 0; st$runs <- character(0)
  st$template <- NULL; st$out_crs <- NULL

  do_flush <- function() {
    if (!length(st$buf)) return(invisible())
    df <- if (length(st$buf) == 1) st$buf[[1]] else do.call(rbind, st$buf)
    rf <- tempfile(fileext = ".vtr")
    write_vtr(df, rf)
    st$runs <- c(st$runs, rf)
    st$buf <- list(); st$buffered <- 0
  }

  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    sb  <- .sf_decode_chunk(chunk, geom, coords, crs)
    res <- batch_fn(sb)
    if (is.null(st$out_crs) && (inherits(res, "sf") || inherits(res, "sfc")))
      st$out_crs <- sf::st_crs(res)
    df <- .sf_encode_result(res, out_geom)
    if (is.null(st$template)) st$template <- df[0, , drop = FALSE]
    if (nrow(df)) {
      st$buf <- c(st$buf, list(df))
      st$buffered <- st$buffered + nrow(df)
      if (st$buffered >= flush_rows) do_flush()
    }
  }
  do_flush()

  # Empty result: still return a valid node with the right schema.
  if (!length(st$runs)) {
    tmpl <- st$template
    if (is.null(tmpl))
      tmpl <- stats::setNames(
        data.frame(character(0), stringsAsFactors = FALSE), out_geom)
    rf <- tempfile(fileext = ".vtr")
    write_vtr(tmpl, rf)
    st$runs <- rf
  }

  node <- .concat_runs(st$runs)
  reg <- new.env(parent = emptyenv())
  reg$paths <- st$runs
  reg.finalizer(reg, function(e) try(unlink(e$paths), silent = TRUE),
                onexit = TRUE)
  node$.reg <- reg
  node$.crs <- if (!is.null(st$out_crs)) st$out_crs else crs
  node
}

# -- front doors --------------------------------------------------------------

#' Stream a query through an sf transform
#'
#' Applies a per-feature \pkg{sf} operation (buffer, centroid, area, CRS
#' transform, simplify, ...) to a lazy vectra query one batch at a time and
#' returns a new lazy node. The engine pulls one batch, hands it to `fn` as an
#' `sf` object, encodes the result back into the stream, and spills to disk, so
#' peak memory is one batch regardless of result size. This is the streaming,
#' larger-than-RAM counterpart to running the same `sf` call on a whole
#' in-memory table.
#'
#' Geometry travels through the engine as hex-encoded WKB in an ordinary string
#' column (vectra has no native geometry type), and the coordinate reference
#' system is carried on the returned node rather than in the `.vtr` file. Use
#' [collect_sf()] to materialize the result as an `sf` object, or [collect()]
#' to get the underlying data.frame with the WKB string column.
#'
#' Topology is delegated entirely to \pkg{sf}/GEOS; vectra only supplies the
#' streaming. The `sf` package is an optional dependency (Suggests).
#'
#' @param x A `vectra_node` (from [tbl()], [tbl_tiff()], any verb chain, ...).
#'   It is consumed by the stream.
#' @param fn A function (or purrr-style formula such as `~ sf::st_buffer(.x,
#'   1000)`) taking one `sf` batch and returning an `sf` object, `sfc`, or plain
#'   data.frame. The active geometry of the return becomes the output geometry.
#' @param geom Name of the input geometry column holding hex-WKB or WKT strings.
#'   Default `"geometry"`. Ignored when `coords` is given.
#' @param coords Optional length-2 character vector naming the x and y
#'   coordinate columns to assemble point geometry from (e.g. `c("x", "y")`),
#'   for inputs such as [tiff_extract_points()] output. The coordinate columns
#'   are retained.
#' @param crs Coordinate reference system of the input geometry, in any form
#'   [sf::st_crs()] accepts (EPSG integer, WKT, proj string). Defaults to the
#'   CRS the upstream node carries, or unknown.
#' @param out_geom Name of the output geometry column. Defaults to `geom`
#'   (or `"geometry"` when `coords` is used).
#' @param flush_rows Transformed rows buffered before a spill flush. Larger
#'   values mean fewer, bigger temporary files. Defaults to
#'   `getOption("vectra.spatial_flush", 5e5)`.
#'
#' @return A `vectra_node` backed by temporary `.vtr` spills (removed when the
#'   node is garbage-collected), carrying the output CRS for [collect_sf()].
#'
#' @seealso [spatial_join()] to join a streamed side against a resident `sf`
#'   object, [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   NAME = nc$NAME,
#'   geometry = sf::st_as_binary(sf::st_centroid(sf::st_geometry(nc)),
#'                               hex = TRUE)
#' ), f)
#'
#' # Buffer every county centroid by 0.1 degree, streaming.
#' buffered <- tbl(f) |>
#'   spatial_map(~ sf::st_buffer(.x, 0.1), crs = sf::st_crs(nc))
#' collect_sf(buffered)
#' unlink(f)
#'
#' @export
spatial_map <- function(x, fn, geom = "geometry", coords = NULL, crs = NA,
                        out_geom = NULL, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (build one with tbl(), tbl_csv(), ...)")
  fn <- rlang::as_function(fn)
  crs <- .resolve_crs(x, crs)
  if (is.null(out_geom)) out_geom <- if (is.null(coords)) geom else "geometry"
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  .spatial_stream(x, fn, geom, coords, crs, out_geom, fr)
}

#' Spatial join a streamed query against a resident sf object
#'
#' Streams a large left side `x` through the engine and joins each batch against
#' a small right side `y` held resident in memory, using an \pkg{sf} binary
#' predicate (`st_intersects` by default). This is the spatial analogue of a
#' hash join with the small side on the build side: the billion-row left stream
#' never materializes, while `y` (admin polygons, habitat patches, ...) stays in
#' RAM. The dominant real workload it serves is tagging huge point sets with the
#' polygon they fall in.
#'
#' Both sides huge is out of scope for a single resident `y`; partition the
#' inputs first with [offload()] on a spatial grid key and join within each
#' shard. Topology and CRS handling are \pkg{sf}'s; vectra supplies the stream.
#'
#' @inheritParams spatial_map
#' @param y An `sf` object: the resident right side of the join.
#' @param join An \pkg{sf} binary predicate function, e.g. [sf::st_intersects]
#'   (default), [sf::st_within], [sf::st_contains], [sf::st_nearest_feature].
#' @param left If `TRUE` (default) keep every left row (left join); if `FALSE`
#'   keep only matches (inner join).
#' @param suffix Length-2 character vector disambiguating columns present on
#'   both sides. Default `c(".x", ".y")`.
#' @param ... Further arguments passed to [sf::st_join()].
#'
#' @return A `vectra_node` of the joined stream, backed by temporary `.vtr`
#'   spills and carrying the left CRS.
#'
#' @seealso [spatial_map()] for per-feature transforms, [collect_sf()] to
#'   materialize as `sf`, [offload()] to partition both-sides-huge joins.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'
#' # A stream of points, stored with x/y coordinate columns.
#' set.seed(1)
#' pts <- sf::st_coordinates(sf::st_sample(nc, 200))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)
#'
#' # Tag each point with the county it falls in, streaming.
#' tagged <- tbl(f) |>
#'   spatial_join(nc["NAME"], join = sf::st_intersects,
#'                coords = c("x", "y"), crs = sf::st_crs(nc))
#' head(collect(tagged))
#' unlink(f)
#'
#' @export
spatial_join <- function(x, y, join = NULL, geom = "geometry", coords = NULL,
                         crs = NA, left = TRUE, suffix = c(".x", ".y"),
                         out_geom = NULL, flush_rows = NULL, ...) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed left side)")
  if (!inherits(y, "sf"))
    stop("`y` must be an sf object (the resident right side of the join)")
  if (is.null(join)) join <- sf::st_intersects
  crs <- .resolve_crs(x, crs)
  if (is.null(out_geom)) out_geom <- if (is.null(coords)) geom else "geometry"
  dots <- list(...)
  batch_fn <- function(sb)
    do.call(sf::st_join,
            c(list(sb, y, join = join, left = left, suffix = suffix), dots))
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  .spatial_stream(x, batch_fn, geom, coords, crs, out_geom, fr)
}

#' Materialize a spatial query as an sf object
#'
#' Collects a `vectra_node` (typically the result of [spatial_map()] or
#' [spatial_join()]) into memory and rebuilds an `sf` object from its hex-WKB
#' geometry column. The CRS defaults to the one carried on the node.
#'
#' This is the spatial counterpart to [collect()]: use it when the final result
#' fits in memory as `sf`. For a result still larger than RAM, keep it as a node
#' and write it out with [write_vtr()] (the geometry stays as a WKB string
#' column) or reduce it with [collect_chunked()].
#'
#' @param x A `vectra_node` with a hex-WKB / WKT geometry column, or a
#'   data.frame already collected from one.
#' @param geom Name of the geometry column. Default `"geometry"`.
#' @param crs Override the coordinate reference system. Defaults to the CRS the
#'   node carries, or unknown.
#'
#' @return An `sf` object.
#'
#' @seealso [spatial_map()], [spatial_join()], [collect()].
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   NAME = nc$NAME,
#'   geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#' ), f)
#' result <- tbl(f) |> spatial_map(~ sf::st_centroid(.x), crs = sf::st_crs(nc))
#' collect_sf(result)
#' unlink(f)
#'
#' @export
collect_sf <- function(x, geom = "geometry", crs = NULL) {
  .check_sf()
  node_crs <- if (inherits(x, "vectra_node")) x$.crs else NULL
  df <- if (inherits(x, "vectra_node")) collect(x) else as.data.frame(x)
  if (is.null(crs)) crs <- node_crs
  crs <- .as_crs(crs)
  if (!geom %in% names(df))
    stop(sprintf("geometry column '%s' not found; pass geom=", geom))
  g <- sf::st_as_sfc(structure(df[[geom]], class = "WKB"), EWKB = FALSE)
  g <- sf::st_set_crs(g, crs)
  rest <- df[setdiff(names(df), geom)]
  sf::st_sf(rest, geometry = g)
}

#' Self-overlay a polygon layer into disjoint pieces (QGIS-style Union)
#'
#' Splits a polygon layer along all its own overlaps into disjoint pieces and
#' returns a lazy node with one row per piece per covering polygon: where `k`
#' polygons overlap, that piece appears `k` times, each row carrying one source
#' polygon's attributes. This is the union overlay GIS tools expose as
#' "Union (single layer)", with the overlap retained once per contributing
#' feature rather than dissolved. Resolve the duplicates with a grouped
#' [slice_min()] / [slice_max()] -- for example earliest designation year wins:
#' `group_by(piece_id) |> slice_min(year)`.
#'
#' The topology is done once with \pkg{sf}/GEOS and tiled over connected overlap
#' clusters (disjoint clusters never share a piece, so the tiling is exact and
#' bounded in memory), then the exploded pieces are streamed to a `.vtr` and
#' handed back as a lazy node. Geometry rides through the engine as hex-encoded
#' WKB in a string column; the CRS is carried on the node for [collect_sf()].
#'
#' The overlay runs on a fixed-precision model: coordinates are snapped to a
#' grid derived from their own magnitude so the pieces come out disjoint and
#' their areas reconstruct the union of the inputs, instead of drifting by the
#' fraction of a percent that floating-point sliver artefacts on invalid input
#' otherwise introduce. Inputs are also passed through [sf::st_make_valid()].
#'
#' The input `x` must be a resident `sf` object: building the overlap graph and
#' intersecting needs the geometries in memory. The exploded result, which is
#' typically several times larger, is what streams to disk.
#'
#' @param x An `sf` object with polygon or multipolygon geometry.
#' @param vars Character vector of attribute columns of `x` to carry onto each
#'   piece. Default `NULL` keeps them all; name a subset to keep the streamed
#'   output narrow.
#' @param piece Name of the integer piece-id column added to the output (the key
#'   you group by to resolve overlaps). Default `"piece_id"`.
#' @param geom Name of the output hex-WKB geometry column. Default `"geometry"`.
#' @param flush_rows Exploded rows buffered before a spill flush. Defaults to
#'   `getOption("vectra.spatial_flush", 5e5)`.
#' @param quiet If `FALSE`, show a text progress bar over the overlap clusters.
#'
#' @return A `vectra_node` over the exploded overlay (one row per piece per
#'   covering polygon), backed by temporary `.vtr` spills removed when the node
#'   is garbage-collected, carrying the CRS of `x` for [collect_sf()].
#'
#' @seealso [slice_min()] / [slice_max()] to resolve each piece to one winner,
#'   [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' # Two overlapping squares designated in different years.
#' sq <- function(a, b) sf::st_polygon(list(rbind(
#'   c(a, 0), c(b, 0), c(b, 1), c(a, 1), c(a, 0))))
#' polys <- sf::st_sf(year = c(1990L, 2010L),
#'                    geometry = sf::st_sfc(sq(0, 2), sq(1, 3)))
#'
#' # Split into disjoint pieces; earliest year wins where they overlap.
#' first <- spatial_overlay(polys) |>
#'   group_by(piece_id) |>
#'   slice_min(year, n = 1, with_ties = FALSE) |>
#'   collect_sf()
#' first
#'
#' @export
spatial_overlay <- function(x, vars = NULL, piece = "piece_id",
                            geom = "geometry", flush_rows = NULL,
                            quiet = TRUE) {
  .check_sf()
  if (!inherits(x, "sf"))
    stop("`x` must be an sf object (the polygon layer to self-overlay)")
  crs  <- sf::st_crs(x)
  prec <- .robust_precision(x)
  if (!is.null(prec)) x <- sf::st_set_precision(x, prec)
  x <- sf::st_make_valid(x)
  x <- x[!sf::st_is_empty(x), , drop = FALSE]
  n <- nrow(x)
  if (n == 0L) stop("`x` has no non-empty geometries to overlay")

  g     <- sf::st_geometry(x)
  attrs <- as.data.frame(sf::st_drop_geometry(x))
  if (!is.null(vars)) {
    miss <- setdiff(vars, names(attrs))
    if (length(miss))
      stop(sprintf("vars not found in `x`: %s", paste(miss, collapse = ", ")))
    attrs <- attrs[, vars, drop = FALSE]
  }
  if (piece %in% names(attrs))
    stop(sprintf("piece column '%s' already exists in `x`; pass piece=", piece))

  groups <- split(seq_len(n), .overlap_components(sf::st_intersects(x), n))

  fr  <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  runs <- character(0); buf <- list(); buffered <- 0L; piece_off <- 0L
  pb <- if (!quiet) utils::txtProgressBar(0, length(groups), style = 3) else NULL
  flush <- function() {
    if (!length(buf)) return(invisible())
    df <- if (length(buf) == 1L) buf[[1]] else do.call(rbind, buf)
    rf <- tempfile(fileext = ".vtr"); write_vtr(df, rf)
    runs[[length(runs) + 1L]] <<- rf
    buf <<- list(); buffered <<- 0L
  }

  for (gi in seq_along(groups)) {
    rows <- groups[[gi]]
    if (length(rows) == 1L) {
      pg <- g[rows]; origins <- list(1L)
    } else {
      sub   <- sf::st_sf(.ovl = seq_along(rows), geometry = g[rows])
      parts <- sf::st_intersection(sub)
      d     <- sf::st_dimension(parts)
      parts <- parts[!is.na(d) & d == 2, ]       # drop point/line touch pieces
      if (nrow(parts) &&
          any(sf::st_geometry_type(parts) == "GEOMETRYCOLLECTION"))
        parts <- sf::st_collection_extract(parts, "POLYGON")
      if (!nrow(parts)) { if (!is.null(pb)) utils::setTxtProgressBar(pb, gi); next }
      pg <- sf::st_geometry(parts); origins <- parts$origins
    }
    np  <- length(pg)
    idx <- rep(seq_len(np), lengths(origins))
    src <- rows[unlist(origins)]
    df  <- attrs[src, , drop = FALSE]
    df[[piece]] <- piece_off + idx
    df[[geom]]  <- sf::st_as_binary(pg[idx], hex = TRUE)
    rownames(df) <- NULL
    buf[[length(buf) + 1L]] <- .coerce_for_vtr(df)
    buffered  <- buffered + length(idx)
    piece_off <- piece_off + np
    if (buffered >= fr) flush()
    if (!is.null(pb)) utils::setTxtProgressBar(pb, gi)
  }
  flush()
  if (!is.null(pb)) close(pb)
  if (!length(runs)) stop("overlay produced no polygonal pieces")

  node <- .concat_runs(runs)
  reg  <- new.env(parent = emptyenv()); reg$paths <- runs
  reg.finalizer(reg, function(e) try(unlink(e$paths), silent = TRUE),
                onexit = TRUE)
  node$.reg <- reg
  node$.crs <- crs
  node
}
