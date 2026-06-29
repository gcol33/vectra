# network.R - streamed network analysis over a resident line graph.
#
# Tier 5 of the spatial roadmap: routing and reachability over a line network.
# Unlike the geometry verbs, this needs a graph (nodes at line endpoints, edges
# weighted by length or a cost column) and a shortest-path solver, not a geometry
# stream. It still fits vectra's shape by reusing the resident-`y` streamed-`x`
# family (spatial_knn, spatial_locate): spatial_network() builds the node-edge
# graph once and holds it resident in an external pointer; spatial_route() and
# spatial_service_area() stream origin (and destination) batches past it.
#
# The graph build and the Dijkstra solver are native C (src/network.c); the
# geometry side - endpoint extraction, snapping origins to nodes, assembling a
# route line - is sf, matching how the rest of this family handles geometry. No
# igraph dependency: the solver is a binary-heap Dijkstra over a CSR adjacency.

# Normalise a one-way direction code column to "B" (both ways), "FT" (from->to
# only, the digitised direction), "TF" (to->from only), or "N" (closed). Accepts
# the textual codes and the +/-/0 sign convention; NA becomes "B".
.network_dir_code <- function(v) {
  cc <- toupper(trimws(as.character(v)))
  cc[is.na(cc) | cc == "" | cc %in% c("BOTH", "B", "TRUE")] <- "B"
  cc[cc %in% c("FT", "F", "+", "1")] <- "FT"
  cc[cc %in% c("TF", "T", "-")]      <- "TF"
  cc[cc %in% c("N", "NONE", "0", "FALSE", "CLOSED")] <- "N"
  bad <- !cc %in% c("B", "FT", "TF", "N")
  if (any(bad))
    stop("unknown direction code(s): ",
         paste(unique(v[bad]), collapse = ", "),
         "; use B / FT / TF / N (or +/-/0)", call. = FALSE)
  cc
}

# Snap a set of query geometries to their nearest graph node, returning 1-based
# node ids (indices into the network's node table).
.network_snap <- function(network, g) {
  g <- .align_resident_crs(g, network$crs)
  as.integer(sf::st_nearest_feature(g, network$node_pts))
}

# Coerce a destination argument (sf / sfc) to an sfc of points in the network CRS.
.network_dest <- function(to, crs) {
  if (inherits(to, "sf")) g <- sf::st_geometry(to)
  else if (inherits(to, "sfc")) g <- to
  else stop("`to` must be an sf or sfc object", call. = FALSE)
  .align_resident_crs(g, crs)
}

# Rebuild route line geometry from C's edge-id paths. Each path is the sequence
# of directed-edge ids (0-based) the solver walked; an edge maps to a source line
# (network$coords_list) traversed forward or reversed. Coincident join vertices
# are dropped. An empty path (unreachable, or origin == destination) yields an
# empty linestring.
.route_geometry <- function(network, paths, crs) {
  cl    <- network$coords_list
  eline <- network$edges$line
  erev  <- network$edges$rev
  geoms <- lapply(paths, function(p) {
    if (!length(p)) return(sf::st_linestring())
    full <- NULL
    for (e in p) {
      i <- e + 1L
      m <- cl[[eline[i]]]
      if (erev[i]) m <- m[nrow(m):1L, , drop = FALSE]
      if (is.null(full)) {
        full <- m
      } else {
        if (full[nrow(full), 1L] == m[1L, 1L] &&
            full[nrow(full), 2L] == m[1L, 2L])
          m <- m[-1L, , drop = FALSE]
        full <- rbind(full, m)
      }
    }
    if (is.null(full) || nrow(full) < 2L) sf::st_linestring()
    else sf::st_linestring(full)
  })
  sf::st_sfc(geoms, crs = .as_crs(crs))
}

# Build one origin's service-area geometry for a cost band `b`, given the node
# cost vector `ncost` (Inf where unreached). "nodes" = the reachable nodes as a
# multipoint, "lines" = the edges with both endpoints reachable as a
# multilinestring, "polygon" = the convex hull of the reachable nodes (the
# generalised service area); a band reaching fewer than three nodes yields an
# empty polygon.
.service_geometry <- function(network, ncost, b, output) {
  in_b <- ncost <= b
  if (output == "lines") {
    sel <- which(in_b[network$line_from] & in_b[network$line_to])
    if (!length(sel)) return(sf::st_multilinestring())
    return(sf::st_multilinestring(network$coords_list[sel]))
  }
  idx <- which(in_b)
  if (!length(idx))
    return(if (output == "polygon") sf::st_polygon() else sf::st_multipoint())
  pts <- network$node_xy[idx, , drop = FALSE]
  if (output == "nodes") return(sf::st_multipoint(pts))
  if (nrow(pts) < 3L) return(sf::st_polygon())
  sf::st_convex_hull(sf::st_multipoint(pts))
}

#' Build a routable network graph from a line layer
#'
#' Builds the node-edge graph a shortest-path query needs: nodes at line
#' endpoints, edges weighted by geometry length or a cost column. The graph is
#' held resident in an external pointer and passed to [spatial_route()] and
#' [spatial_service_area()], which stream origin (and destination) batches past
#' it. This is the network counterpart of a resident `sf` `y` in [spatial_knn()]:
#' the graph is the resident budget (bounded by the network size), while the
#' query side scales by streaming.
#'
#' Endpoints within `tolerance` of each other are snapped to a single node, so a
#' layer whose touching lines do not share exactly-equal endpoint coordinates
#' still connects. The input must be (multi)linestrings; a `MULTILINESTRING` is
#' split into its parts, each becoming one edge with the source attributes. Lines
#' are treated as already split at their junctions (the usual road-network
#' convention); two lines that merely cross in their interiors are not connected
#' unless they share an endpoint. The graph and the Dijkstra solver are native C;
#' \pkg{sf} (an optional dependency, Suggests) supplies only the geometry.
#'
#' @param lines An `sf` or `sfc` object of (multi)linestrings, or a `vectra_node`
#'   carrying a hex-WKB line geometry column (it is materialised).
#' @param weight Name of a column in `lines` holding each edge's traversal cost.
#'   `NULL` (default) uses the geometry length ([sf::st_length()]).
#' @param directed If `TRUE`, edges are one-way (by `direction`, or the digitised
#'   from->to direction). `FALSE` (default) makes every edge two-way.
#' @param direction Name of a column of one-way codes, used when `directed`:
#'   `"B"` two-way, `"FT"` from->to only, `"TF"` to->from only, `"N"` closed (the
#'   `+`/`-`/`0` sign convention is also accepted). `NULL` uses `"FT"` for every
#'   line, or `"B"` when `weight_to` is given.
#' @param weight_to Name of a column holding the reverse-direction cost on a
#'   two-way edge of a directed graph. `NULL` reuses `weight`.
#' @param tolerance Endpoints within this distance (CRS units) are snapped to one
#'   node. `0` (default) joins only exactly-coincident endpoints.
#' @param geom,crs Geometry column name and CRS, as in [spatial_map()]. The CRS
#'   defaults to the one `lines` carries.
#'
#' @return A `vectra_network` object: the resident graph (an external pointer
#'   plus the node coordinates, edge table, and source-line geometry needed to
#'   snap queries and rebuild routes). Print it to see the node, edge, and
#'   connected-component counts.
#'
#' @seealso [spatial_route()] for shortest paths and origin-destination costs,
#'   [spatial_service_area()] for reachability and isochrones.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' # A small grid of streets.
#' mk <- function(x1, y1, x2, y2)
#'   sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
#' streets <- sf::st_sfc(
#'   mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(0, 0, 0, 1),
#'   mk(0, 1, 1, 1), mk(1, 0, 1, 1), mk(1, 1, 2, 1), mk(2, 0, 2, 1))
#' net <- spatial_network(streets)
#' net
#'
#' @export
spatial_network <- function(lines, weight = NULL, directed = FALSE,
                            direction = NULL, weight_to = NULL,
                            tolerance = 0, geom = "geometry", crs = NA) {
  .check_sf()
  if (inherits(lines, "vectra_node")) {
    node_crs <- lines$.crs
    lines <- collect_sf(lines, geom = geom, crs = crs)
    if (identical(crs, NA) || is.null(crs)) crs <- node_crs
  }
  if (inherits(lines, "sfc")) lines <- sf::st_sf(geometry = lines)
  if (!inherits(lines, "sf"))
    stop("`lines` must be an sf/sfc object or a vectra_node", call. = FALSE)
  if (identical(crs, NA) || is.null(crs)) crs <- sf::st_crs(lines)
  crs <- .as_crs(crs)

  gt <- as.character(sf::st_geometry_type(sf::st_geometry(lines)))
  if (!all(gt %in% c("LINESTRING", "MULTILINESTRING")))
    stop("`lines` must be LINESTRING or MULTILINESTRING geometry", call. = FALSE)
  # explode multilinestrings into single linestring edges, replicating attributes
  lines <- suppressWarnings(
    sf::st_cast(sf::st_cast(lines, "MULTILINESTRING"), "LINESTRING"))
  g <- sf::st_geometry(lines)

  empt <- sf::st_is_empty(g)
  if (any(empt)) {
    warning(sprintf("dropping %d empty line geometr%s", sum(empt),
                    if (sum(empt) == 1L) "y" else "ies"), call. = FALSE)
    lines <- lines[!empt, , drop = FALSE]
    g <- sf::st_geometry(lines)
  }
  n_lines <- length(g)
  if (n_lines == 0L) stop("`lines` has no usable line geometry", call. = FALSE)

  # edge weights
  if (is.null(weight)) {
    w <- as.numeric(sf::st_length(g))
  } else {
    if (!weight %in% names(lines))
      stop(sprintf("weight column '%s' not found in `lines`", weight),
           call. = FALSE)
    w <- as.numeric(lines[[weight]])
  }
  if (any(!is.finite(w) | w < 0))
    stop("edge weights must be finite and non-negative", call. = FALSE)

  # endpoints + per-line coordinate matrices, from one coordinate pass
  co <- sf::st_coordinates(g)
  L1 <- co[, "L1"]
  coords_list <- lapply(split(seq_len(nrow(co)), L1),
                        function(ix) co[ix, c("X", "Y"), drop = FALSE])
  coords_list <- coords_list[order(as.integer(names(coords_list)))]
  names(coords_list) <- NULL
  first <- !duplicated(L1)
  last  <- !duplicated(L1, fromLast = TRUE)
  a_xy <- co[first, c("X", "Y"), drop = FALSE]   # start of each line
  b_xy <- co[last,  c("X", "Y"), drop = FALSE]   # end of each line

  # snap endpoints to node ids: round to the tolerance grid, hash the coordinate
  allxy <- rbind(a_xy, b_xy)
  rxy <- if (tolerance > 0) round(allxy / tolerance) * tolerance else allxy
  key <- paste(rxy[, 1L], rxy[, 2L], sep = "\r")
  uk  <- unique(key)
  node_of <- match(key, uk)                       # 1-based node id per endpoint
  n_nodes <- length(uk)
  rep_idx <- match(uk, key)                        # first occurrence per node
  node_xy <- allxy[rep_idx, , drop = FALSE]        # representative node coords

  line_from <- node_of[seq_len(n_lines)]
  line_to   <- node_of[n_lines + seq_len(n_lines)]

  # expand into directed edges, carrying each edge's source line and a reversed
  # flag so a route path can rebuild geometry
  if (!directed) {
    dfrom <- c(line_from, line_to)
    dto   <- c(line_to,   line_from)
    dw    <- c(w, w)
    dline <- c(seq_len(n_lines), seq_len(n_lines))
    drev  <- c(rep(FALSE, n_lines), rep(TRUE, n_lines))
  } else {
    wt <- if (is.null(weight_to)) w else {
      if (!weight_to %in% names(lines))
        stop(sprintf("weight_to column '%s' not found in `lines`", weight_to),
             call. = FALSE)
      as.numeric(lines[[weight_to]])
    }
    if (any(!is.finite(wt) | wt < 0))
      stop("reverse weights must be finite and non-negative", call. = FALSE)
    code <- if (is.null(direction)) {
      rep(if (is.null(weight_to)) "FT" else "B", n_lines)
    } else {
      if (!direction %in% names(lines))
        stop(sprintf("direction column '%s' not found in `lines`", direction),
             call. = FALSE)
      .network_dir_code(lines[[direction]])
    }
    fwd <- code %in% c("FT", "B")
    bwd <- code %in% c("TF", "B")
    dfrom <- c(line_from[fwd], line_to[bwd])
    dto   <- c(line_to[fwd],   line_from[bwd])
    dw    <- c(w[fwd],         wt[bwd])
    dline <- c(which(fwd),     which(bwd))
    drev  <- c(rep(FALSE, sum(fwd)), rep(TRUE, sum(bwd)))
  }

  ptr <- .Call(C_network_build, as.integer(dfrom - 1L), as.integer(dto - 1L),
               as.double(dw), as.integer(n_nodes))

  node_pts <- sf::st_as_sf(
    data.frame(.x = node_xy[, 1L], .y = node_xy[, 2L]),
    coords = c(".x", ".y"), crs = crs)

  structure(
    list(ptr = ptr,
         n_nodes = n_nodes,
         node_xy = node_xy,
         node_pts = node_pts,
         coords_list = coords_list,
         line_from = line_from,
         line_to = line_to,
         edges = list(line = as.integer(dline), rev = drev),
         crs = crs,
         directed = directed,
         stats = .Call(C_network_stats, ptr)),
    class = "vectra_network")
}

#' @export
print.vectra_network <- function(x, ...) {
  st <- x$stats
  cat("<vectra_network>\n")
  cat(sprintf("  nodes:      %d\n", st[1L]))
  cat(sprintf("  edges:      %d (%s)\n", st[2L],
              if (x$directed) "directed" else "undirected"))
  cat(sprintf("  components: %d\n", st[3L]))
  crs <- tryCatch(x$crs$input, error = function(e) NA)
  if (!is.null(crs) && !is.na(crs)) cat(sprintf("  crs:        %s\n", crs))
  invisible(x)
}

#' Shortest paths and origin-destination costs over a network
#'
#' Streams a layer of origins `x` past a resident [spatial_network()] and, for
#' each origin, finds the shortest path to one or more destinations `to`. Each
#' origin and destination is snapped to its nearest graph node; the solver
#' (native-C Dijkstra) returns one row per (origin, destination) pair with the
#' total cost and, by default, the route geometry. With `geometry = FALSE` only
#' the cost is returned, so a destination set per origin yields the
#' origin-destination cost matrix in long form. The billion-origin stream never
#' materialises; the graph stays resident.
#'
#' An unreachable destination returns an infinite cost and an empty geometry
#' rather than dropping the row, so a cost matrix stays rectangular. Snapping is
#' to the nearest node, so place origins and destinations on or near the network;
#' costs are in the units of the network's `weight` (or CRS length units when the
#' graph was built from geometry length). The \pkg{sf} package is an optional
#' dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param x A `vectra_node` of origin features (point geometry, or `coords`).
#' @param network A `vectra_network` from [spatial_network()].
#' @param to An `sf`/`sfc` of destination points. One destination routes every
#'   origin to it; several produce one row per (origin, destination).
#' @param to_id Optional column in `to` identifying each destination in the
#'   output. `NULL` (default) uses the 1-based destination index.
#' @param geometry If `TRUE` (default) each row carries the route line; if
#'   `FALSE` only the cost column (the cost-matrix form, no geometry).
#' @param cost_col,dest_col Names of the output cost and destination-identifier
#'   columns. Defaults `"cost"`, `"destination"`.
#'
#' @return A `vectra_node`: one row per (origin, destination) carrying `x`'s
#'   attributes, the destination id, the cost, and (when `geometry = TRUE`) the
#'   route geometry. Backed by temporary `.vtr` spills removed when the node is
#'   garbage-collected, and carrying the network CRS.
#'
#' @seealso [spatial_network()] to build the graph, [spatial_service_area()] for
#'   reachability, [collect_sf()] to materialize routes as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' mk <- function(x1, y1, x2, y2)
#'   sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
#' streets <- sf::st_sfc(
#'   mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(0, 0, 0, 1),
#'   mk(0, 1, 1, 1), mk(1, 0, 1, 1), mk(1, 1, 2, 1), mk(2, 0, 2, 1))
#' net <- spatial_network(streets)
#'
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = 1:2, x = c(0, 0), y = c(0, 1)), f)
#' dest <- sf::st_sfc(sf::st_point(c(2, 1)))
#'
#' tbl(f) |>
#'   spatial_route(net, to = dest, coords = c("x", "y")) |>
#'   collect_sf()
#' unlink(f)
#'
#' @export
spatial_route <- function(x, network, to, to_id = NULL, geometry = TRUE,
                          cost_col = "cost", dest_col = "destination",
                          geom = "geometry", coords = NULL, crs = NA,
                          out_geom = NULL, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed origins)", call. = FALSE)
  if (!inherits(network, "vectra_network"))
    stop("`network` must be a vectra_network from spatial_network()",
         call. = FALSE)
  crs <- .resolve_crs(x, crs)
  if (is.na(.as_crs(crs))) crs <- network$crs

  dst_g    <- .network_dest(to, crs)
  dst_node <- .network_snap(network, dst_g)
  nd <- length(dst_node)
  if (nd == 0L) stop("`to` has no destinations", call. = FALSE)
  did <- if (is.null(to_id)) seq_len(nd) else {
    if (!inherits(to, "sf") || !to_id %in% names(to))
      stop(sprintf("`to_id` column '%s' not found in `to`", to_id),
           call. = FALSE)
    to[[to_id]]
  }

  out_geom <- out_geom %||% (if (is.null(coords)) geom else "geometry")
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)
  nxt <- .batch_cursor(x)
  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    sb <- .sf_decode_chunk(chunk, geom, coords, crs)
    no <- nrow(sb)
    if (no == 0L) next
    o_node <- .network_snap(network, sf::st_geometry(sb))

    src_pairs <- rep(o_node,   each  = nd)
    dst_pairs <- rep(dst_node, times = no)
    res <- .Call(C_network_route, network$ptr,
                 as.integer(src_pairs - 1L), as.integer(dst_pairs - 1L),
                 isTRUE(geometry))

    oi <- rep(seq_len(no), each = nd)
    df <- as.data.frame(sf::st_drop_geometry(sb))[oi, , drop = FALSE]
    rownames(df) <- NULL
    df[[dest_col]] <- did[rep(seq_len(nd), times = no)]
    df[[cost_col]] <- res$cost

    if (isTRUE(geometry)) {
      rg  <- .route_geometry(network, res$paths, crs)
      out <- sf::st_sf(df, geometry = rg)
      acc$push(.sf_encode_result(out, out_geom))
    } else {
      acc$push(.coerce_for_vtr(df))
    }
  }
  acc$finish(crs = .as_crs(crs), empty_geom = out_geom)
}

#' Service areas and isochrones over a network
#'
#' Streams a layer of origins `x` past a resident [spatial_network()] and, for
#' each origin, finds every part of the network reachable within a cost budget -
#' the service area, or, with several budgets, nested travel-cost isochrone bands.
#' Each origin is snapped to its nearest graph node and a budget-bounded Dijkstra
#' (native C) collects the reachable nodes; the reachable set is returned as the
#' convex hull (`output = "polygon"`, the generalised service area), the
#' reachable edges (`"lines"`), or the reachable nodes (`"nodes"`).
#'
#' A vector `cost` returns one row per (origin, band), each band the area
#' reachable within that budget, so the rows nest from the smallest budget out.
#' Costs are in the network's `weight` units. The convex-hull polygon is a
#' generalisation; use `output = "lines"` for the exact reachable network. The
#' \pkg{sf} package is an optional dependency (Suggests).
#'
#' @inheritParams spatial_route
#' @param cost A cost budget (scalar), or several budgets for nested isochrone
#'   bands (e.g. `c(5, 10, 15)`).
#' @param output `"polygon"` (default) for the convex hull of the reachable
#'   nodes, `"lines"` for the reachable edges, or `"nodes"` for the reachable
#'   nodes as a multipoint.
#' @param band_col Name of the output column holding each row's budget. Default
#'   `"band"`.
#'
#' @return A `vectra_node`: one row per (origin, band) carrying `x`'s attributes,
#'   the band value, and the service-area geometry. Backed by temporary `.vtr`
#'   spills removed when the node is garbage-collected, and carrying the network
#'   CRS.
#'
#' @seealso [spatial_network()] to build the graph, [spatial_route()] for
#'   shortest paths, [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' mk <- function(x1, y1, x2, y2)
#'   sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
#' streets <- sf::st_sfc(
#'   mk(0, 0, 1, 0), mk(1, 0, 2, 0), mk(0, 0, 0, 1),
#'   mk(0, 1, 1, 1), mk(1, 0, 1, 1), mk(1, 1, 2, 1), mk(2, 0, 2, 1))
#' net <- spatial_network(streets)
#'
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = 1L, x = 0, y = 0), f)
#'
#' tbl(f) |>
#'   spatial_service_area(net, cost = c(1, 2), output = "lines",
#'                        coords = c("x", "y")) |>
#'   collect_sf()
#' unlink(f)
#'
#' @export
spatial_service_area <- function(x, network, cost,
                                 output = c("polygon", "lines", "nodes"),
                                 band_col = "band",
                                 geom = "geometry", coords = NULL, crs = NA,
                                 out_geom = NULL, flush_rows = NULL) {
  .check_sf()
  output <- match.arg(output)
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed origins)", call. = FALSE)
  if (!inherits(network, "vectra_network"))
    stop("`network` must be a vectra_network from spatial_network()",
         call. = FALSE)
  bands <- sort(unique(as.numeric(cost)))
  if (!length(bands) || any(!is.finite(bands) | bands < 0))
    stop("`cost` must be one or more finite, non-negative budgets", call. = FALSE)
  nb <- length(bands)
  max_budget <- max(bands)

  crs <- .resolve_crs(x, crs)
  if (is.na(.as_crs(crs))) crs <- network$crs
  out_geom <- out_geom %||% "geometry"
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)
  nxt <- .batch_cursor(x)
  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    sb <- .sf_decode_chunk(chunk, geom, coords, crs)
    no <- nrow(sb)
    if (no == 0L) next
    o_node <- .network_snap(network, sf::st_geometry(sb))
    res <- .Call(C_network_service, network$ptr,
                 as.integer(o_node - 1L), as.double(max_budget))

    geoms <- vector("list", no * nb)
    k <- 0L
    for (i in seq_len(no)) {
      ri <- res[[i]]
      ncost <- rep(Inf, network$n_nodes)
      if (length(ri[[1L]])) ncost[ri[[1L]] + 1L] <- ri[[2L]]
      for (b in bands) {
        k <- k + 1L
        geoms[[k]] <- .service_geometry(network, ncost, b, output)
      }
    }
    oi <- rep(seq_len(no), each = nb)
    df <- as.data.frame(sf::st_drop_geometry(sb))[oi, , drop = FALSE]
    rownames(df) <- NULL
    df[[band_col]] <- rep(bands, times = no)
    out <- sf::st_sf(df, geometry = sf::st_sfc(geoms, crs = .as_crs(crs)))
    acc$push(.sf_encode_result(out, out_geom))
  }
  acc$finish(crs = .as_crs(crs), empty_geom = out_geom)
}
