# Join key parsing: handles by = "col", by = c("a" = "b"), or NULL (natural)
parse_join_keys <- function(x, y, by) {
  x_schema <- .Call(C_node_schema, x$.node)
  y_schema <- .Call(C_node_schema, y$.node)

  if (is.null(by)) {
    # Natural join: common column names
    common <- intersect(x_schema$name, y_schema$name)
    if (length(common) == 0)
      stop("no common columns found for natural join; specify 'by'")
    message(sprintf("Joining by: %s", paste(common, collapse = ", ")))
    return(list(left = common, right = common))
  }

  if (is.null(names(by))) {
    # by = c("a", "b") -> same names on both sides
    for (nm in by) {
      if (!nm %in% x_schema$name) stop(sprintf("column '%s' not found in x", nm))
      if (!nm %in% y_schema$name) stop(sprintf("column '%s' not found in y", nm))
    }
    return(list(left = by, right = by))
  }

  # by = c("a" = "b") -> left_key = "a", right_key = "b"
  left_keys <- names(by)
  right_keys <- unname(by)
  # Handle unnamed entries (same name on both sides)
  for (i in seq_along(left_keys)) {
    if (left_keys[i] == "") left_keys[i] <- right_keys[i]
  }
  for (nm in left_keys) {
    if (!nm %in% x_schema$name) stop(sprintf("column '%s' not found in x", nm))
  }
  for (nm in right_keys) {
    if (!nm %in% y_schema$name) stop(sprintf("column '%s' not found in y", nm))
  }
  list(left = left_keys, right = right_keys)
}

#' Join two vectra tables
#'
#' @param x A `vectra_node` object (left table).
#' @param y A `vectra_node` object (right table).
#' @param by A character vector of column names to join by, or a named vector
#'   like `c("a" = "b")`. `NULL` for natural join (common columns).
#' @param suffix A character vector of length 2 for disambiguating non-key
#'   columns with the same name (default `c(".x", ".y")`).
#' @param na_matches How to match `NA` keys: `"na"` (default, as in dplyr) treats
#'   `NA` as matching `NA`; `"never"` uses SQL semantics where `NA` never matches.
#' @param ... Ignored.
#'
#' @return A `vectra_node` with the joined result.
#'
#' @details
#' All joins use a build-right, probe-left hash join. The entire right-side
#' table is materialized into a hash table; left-side batches stream through.
#' Memory cost is proportional to the right-side table size.
#'
#' By default `NA` keys match `NA` (dplyr's `na_matches = "na"`); pass
#' `na_matches = "never"` for SQL NULL semantics. Key types are auto-coerced
#' following the `bool < int64 < double` hierarchy. Joining string against
#' numeric keys is an error.
#'
#' @examples
#' f1 <- tempfile(fileext = ".vtr")
#' f2 <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
#' write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
#' left_join(tbl(f1), tbl(f2), by = "id") |> collect()
#' unlink(c(f1, f2))
#'
#' @export
left_join <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                       na_matches = c("na", "never"), ...) {
  UseMethod("left_join")
}

# Internal: shared implementation for standard joins (left, inner, full)
# Single R entry point for every join node: threads the resolved memory budget
# (the grace-hash spill threshold) so all joins share one source of truth. When
# the build side exceeds the budget, the C engine spills to a partitioned join.
.join_node <- function(left, right, kind, lkeys, rkeys, sx, sy,
                       mem = vectra_mem(), na_matches = TRUE) {
  .Call(C_join_node, left, right, kind, lkeys, rkeys, sx, sy, as.numeric(mem),
        isTRUE(na_matches))
}

# Resolve the dplyr-style na_matches argument to a logical.
.na_matches_flag <- function(na_matches) {
  if (is.logical(na_matches)) return(isTRUE(na_matches))
  match.arg(na_matches, c("na", "never")) == "na"
}

join_impl <- function(x, y, by, suffix, type, na_matches = "na") {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .join_node(x$.node, y$.node,
                         type, keys$left, keys$right, suffix[1], suffix[2],
                         na_matches = .na_matches_flag(na_matches))
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}

# Internal: shared implementation for filtering joins (semi, anti)
filter_join_impl <- function(x, y, by, type, na_matches = "na") {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .join_node(x$.node, y$.node,
                         type, keys$left, keys$right, ".x", ".y",
                         na_matches = .na_matches_flag(na_matches))
  structure(list(.node = new_xptr, .path = x$.path), class = "vectra_node")
}

#' @export
left_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                                  na_matches = c("na", "never"), ...) {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  if (!is.character(suffix) || length(suffix) != 2)
    stop(sprintf("suffix must be character(2), got %s of length %d", class(suffix)[1], length(suffix)))
  join_impl(x, y, by, suffix, "left", na_matches)
}

#' @rdname left_join
#' @export
inner_join <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                       na_matches = c("na", "never"), ...) {
  UseMethod("inner_join")
}

#' @export
inner_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                                   na_matches = c("na", "never"), ...) {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  if (!is.character(suffix) || length(suffix) != 2)
    stop(sprintf("suffix must be character(2), got %s of length %d", class(suffix)[1], length(suffix)))
  join_impl(x, y, by, suffix, "inner", na_matches)
}

#' @rdname left_join
#' @export
right_join <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                       na_matches = c("na", "never"), ...) {
  UseMethod("right_join")
}

#' @export
right_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                                   na_matches = c("na", "never"), ...) {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  if (!is.character(suffix) || length(suffix) != 2)
    stop(sprintf("suffix must be character(2), got %s of length %d", class(suffix)[1], length(suffix)))
  # right_join(x, y) = left_join(y, x) with swapped keys and reordered columns
  keys <- parse_join_keys(x, y, by)

  # Get schemas before C_join_node clears the external pointers
  x_schema <- .Call(C_node_schema, x$.node)
  y_schema <- .Call(C_node_schema, y$.node)

  # Swap: build on left (x), probe with right (y)
  new_xptr <- .join_node(y$.node, x$.node,
                         "left", keys$right, keys$left, suffix[2], suffix[1],
                         na_matches = .na_matches_flag(na_matches))
  result_node <- structure(list(.node = new_xptr, .path = NULL),
                           class = "vectra_node")
  schema <- .Call(C_node_schema, result_node$.node)

  # Current output: y_cols + x_non_key_cols
  # Desired output: x_cols + y_non_key_cols (with key values from y for matched)
  # This is complex to reorder at the node level. Use a project node.
  cur_names <- schema$name

  # x columns come after y columns in current output
  # Build the desired column order
  x_names <- x_schema$name
  y_names <- y_schema$name
  y_key_set <- keys$right

  # Desired: all x columns, then y non-key columns. A non-key column present on
  # both sides is suffixed in the output (like dplyr) -- using the bare name
  # would emit two columns with the same name.
  x_nonkey <- setdiff(x_names, keys$left)
  y_nonkey <- setdiff(y_names, y_key_set)
  collide  <- intersect(x_nonkey, y_nonkey)

  desired <- character(0)
  expr_lists <- list()

  # x key columns: use values from y side (key col in current output)
  for (xn in x_names) {
    ki <- match(xn, keys$left)
    if (!is.na(ki)) {
      # This x key maps to y key: find the y key col name in current output
      yk <- keys$right[ki]
      desired <- c(desired, xn)
      if (xn != yk) {
        expr_lists <- c(expr_lists, list(list(kind = "col_ref", name = yk)))
      } else {
        expr_lists <- c(expr_lists, list(NULL))
      }
    } else {
      # Non-key x column: suffixed in the current output iff it collides with y.
      out_name <- if (xn %in% collide) paste0(xn, suffix[1]) else xn
      match_name <- if (out_name %in% cur_names) out_name else xn
      desired <- c(desired, out_name)
      if (match_name != out_name) {
        expr_lists <- c(expr_lists, list(list(kind = "col_ref", name = match_name)))
      } else {
        expr_lists <- c(expr_lists, list(NULL))
      }
    }
  }
  # y non-key columns
  for (yn in y_names) {
    if (yn %in% y_key_set) next
    out_name <- if (yn %in% collide) paste0(yn, suffix[2]) else yn
    match_name <- if (out_name %in% cur_names) out_name else yn
    desired <- c(desired, out_name)
    if (match_name != out_name) {
      expr_lists <- c(expr_lists, list(list(kind = "col_ref", name = match_name)))
    } else {
      expr_lists <- c(expr_lists, list(NULL))
    }
  }

  new_xptr2 <- .Call(C_project_node, result_node$.node, desired, expr_lists)
  structure(list(.node = new_xptr2, .path = NULL), class = "vectra_node")
}

#' @rdname left_join
#' @export
full_join <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                       na_matches = c("na", "never"), ...) {
  UseMethod("full_join")
}

#' @export
full_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"),
                                  na_matches = c("na", "never"), ...) {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  if (!is.character(suffix) || length(suffix) != 2)
    stop(sprintf("suffix must be character(2), got %s of length %d", class(suffix)[1], length(suffix)))
  join_impl(x, y, by, suffix, "full", na_matches)
}

#' @rdname left_join
#' @export
semi_join <- function(x, y, by = NULL, na_matches = c("na", "never"), ...) {
  UseMethod("semi_join")
}

#' @export
semi_join.vectra_node <- function(x, y, by = NULL,
                                  na_matches = c("na", "never"), ...) {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  filter_join_impl(x, y, by, "semi", na_matches)
}

#' @rdname left_join
#' @export
anti_join <- function(x, y, by = NULL, na_matches = c("na", "never"), ...) {
  UseMethod("anti_join")
}

#' @export
anti_join.vectra_node <- function(x, y, by = NULL,
                                  na_matches = c("na", "never"), ...) {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  filter_join_impl(x, y, by, "anti", na_matches)
}

#' Cross join two vectra tables
#'
#' Returns every combination of rows from `x` and `y` (Cartesian product).
#' Both tables are collected before joining.
#'
#' @param x A `vectra_node` object or data.frame.
#' @param y A `vectra_node` object or data.frame.
#' @param suffix Suffixes for disambiguating column names (default `c(".x", ".y")`).
#' @param ... Ignored.
#'
#' @return A data.frame with `nrow(x) * nrow(y)` rows.
#'
#' @examples
#' f1 <- tempfile(fileext = ".vtr")
#' f2 <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(a = 1:2), f1)
#' write_vtr(data.frame(b = c("x", "y", "z"), stringsAsFactors = FALSE), f2)
#' cross_join(tbl(f1), tbl(f2))
#' unlink(c(f1, f2))
#'
#' @export
cross_join <- function(x, y, suffix = c(".x", ".y"), ...) {
  UseMethod("cross_join")
}

#' @export
cross_join.vectra_node <- function(x, y, suffix = c(".x", ".y"), ...) {
  if (!inherits(y, "vectra_node") && !is.data.frame(y))
    stop(sprintf("y must be a vectra_node or data.frame, got %s", class(y)[1]))
  if (!is.character(suffix) || length(suffix) != 2)
    stop(sprintf("suffix must be character(2), got %s of length %d", class(suffix)[1], length(suffix)))
  df_x <- if (inherits(x, "vectra_node")) collect(x) else x
  df_y <- if (inherits(y, "vectra_node")) collect(y) else y

  nx <- nrow(df_x)
  ny <- nrow(df_y)

  # Expand: repeat each x row ny times, tile y nx times
  idx_x <- rep(seq_len(nx), each = ny)
  idx_y <- rep(seq_len(ny), times = nx)

  result_x <- df_x[idx_x, , drop = FALSE]
  result_y <- df_y[idx_y, , drop = FALSE]

  # Handle name collisions
  common <- intersect(names(result_x), names(result_y))
  if (length(common) > 0) {
    for (nm in common) {
      names(result_x)[names(result_x) == nm] <- paste0(nm, suffix[1])
      names(result_y)[names(result_y) == nm] <- paste0(nm, suffix[2])
    }
  }

  result <- cbind(result_x, result_y)
  rownames(result) <- NULL
  result
}


#' Fuzzy join two vectra tables by string distance
#'
#' Joins two tables using approximate string matching on key columns.
#' Optionally blocks by a second column (e.g., genus) for performance —
#' only rows sharing the same blocking key are compared.
#'
#' @param x A `vectra_node` object (probe / query side).
#' @param y A `vectra_node` object (build / reference side).
#' @param by A named character vector of length 1: `c("probe_col" = "build_col")`.
#'   The columns to compute string distance on.
#' @param method Character. Distance algorithm: `"dl"` (Damerau-Levenshtein,
#'   default), `"levenshtein"`, or `"jw"` (Jaro-Winkler).
#' @param max_dist Numeric. Maximum normalized distance (0-1) to keep a match.
#'   Default `0.2`.
#' @param block_by Optional named character vector of length 1:
#'   `c("probe_col" = "build_col")`. Rows must match exactly on these columns
#'   before distance is computed. Dramatically reduces comparisons.
#' @param n_threads Integer. Number of OpenMP threads for parallel distance
#'   computation over partitions. Default `4L`.
#' @param suffix Character. Suffix appended to build-side column names that
#'   collide with probe-side names. Default `".y"`.
#'
#' @return A `vectra_node` with all probe columns, all build columns (suffixed
#'   on collision), and a `fuzzy_dist` column (double).
#'
#' @export
fuzzy_join <- function(x, y, by, method = "dl", max_dist = 0.2,
                       block_by = NULL, n_threads = 4L,
                       suffix = ".y") {
  UseMethod("fuzzy_join")
}

#' @export
fuzzy_join.vectra_node <- function(x, y, by, method = "dl", max_dist = 0.2,
                                   block_by = NULL, n_threads = 4L,
                                   suffix = ".y") {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  if (!is.character(by) || length(by) != 1)
    stop(sprintf("by must be a named character vector of length 1, got length %d", length(by)))
  if (!is.numeric(max_dist) || length(max_dist) != 1 || max_dist < 0 || max_dist > 1)
    stop(sprintf("max_dist must be a number between 0 and 1, got %s", deparse(max_dist)))
  if (!is.numeric(n_threads) || length(n_threads) != 1 || n_threads < 1)
    stop(sprintf("n_threads must be a positive integer, got %s", deparse(n_threads)))
  method_int <- match(method, c("dl", "levenshtein", "jw")) - 1L
  if (is.na(method_int)) stop("method must be 'dl', 'levenshtein', or 'jw'")

  by_probe <- names(by)
  by_build <- unname(by)
  if (is.null(by_probe) || by_probe == "") by_probe <- by_build

  block_probe <- if (!is.null(block_by)) names(block_by) else NULL
  block_build <- if (!is.null(block_by)) unname(block_by) else NULL
  if (!is.null(block_probe) && block_probe == "") block_probe <- block_build

  new_xptr <- .Call(C_fuzzy_join_node,
                    x$.node, y$.node,
                    by_probe, by_build,
                    block_probe, block_build,
                    as.integer(method_int),
                    as.double(max_dist),
                    as.integer(n_threads),
                    suffix,
                    as.numeric(vectra_mem()))
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}


# Split a `c("probe" = "build")` pair (or a bare "col" shared by both sides)
# into its probe and build column names.
.split_join_pair <- function(v, arg) {
  if (!is.character(v) || length(v) != 1)
    stop(sprintf("%s must be a character vector of length 1 (optionally named %s), got length %d",
                 arg, "c(\"probe\" = \"build\")", length(v)))
  probe <- names(v)
  build <- unname(v)
  if (is.null(probe) || probe == "") probe <- build
  list(probe = probe, build = build)
}

#' Interval (range overlap) join of two vectra tables
#'
#' Joins each row of `x` to every row of `y` whose `[start, end]` interval
#' overlaps it -- the one-dimensional analogue of a spatial bounding-box join
#' (`data.table::foverlaps`, `GenomicRanges::findOverlaps`). An optional
#' equality key in `by` restricts overlap testing to rows that share that key
#' (a chromosome, a sensor id), the same role a regular join's `by` plays.
#'
#' Both sides are read into memory, then within each `by` group a sweep over
#' the interval endpoints emits each overlapping pair once, scaling with the
#' number of overlaps rather than the product of the row counts.
#'
#' @param x A `vectra_node` (the streamed / probe side).
#' @param y A `vectra_node` (the resident / build side).
#' @param start The interval start columns, as `c("x_col" = "y_col")` or a
#'   single name shared by both sides.
#' @param end The interval end columns, in the same form as `start`.
#' @param by Optional equality key restricting overlap to rows that match on
#'   it, as `c("x_col" = "y_col")` or a single shared name. `NULL` (default)
#'   tests every pair.
#' @param type `"inner"` (default) keeps only overlapping pairs; `"left"`
#'   keeps every `x` row, filling `y` columns with `NA` where nothing overlaps.
#' @param closed Logical. `TRUE` (default) counts intervals that touch at an
#'   endpoint as overlapping; `FALSE` requires a strictly positive overlap.
#' @param n_threads Integer. OpenMP threads for the per-group sweep. Default `4L`.
#' @param suffix Character. Suffix appended to `y` column names that collide
#'   with `x` names. Default `".y"`.
#'
#' @return A `vectra_node` with all `x` columns followed by all `y` columns
#'   (suffixed on collision).
#'
#' @export
interval_join <- function(x, y, start, end, by = NULL,
                          type = c("inner", "left"), closed = TRUE,
                          n_threads = 4L, suffix = ".y") {
  UseMethod("interval_join")
}

#' @export
interval_join.vectra_node <- function(x, y, start, end, by = NULL,
                                      type = c("inner", "left"), closed = TRUE,
                                      n_threads = 4L, suffix = ".y") {
  if (!inherits(y, "vectra_node"))
    stop(sprintf("y must be a vectra_node, got %s", class(y)[1]))
  type <- match.arg(type)
  if (!is.logical(closed) || length(closed) != 1 || is.na(closed))
    stop("closed must be TRUE or FALSE")
  if (!is.numeric(n_threads) || length(n_threads) != 1 || n_threads < 1)
    stop(sprintf("n_threads must be a positive integer, got %s", deparse(n_threads)))

  s <- .split_join_pair(start, "start")
  e <- .split_join_pair(end, "end")

  block_probe <- NULL
  block_build <- NULL
  if (!is.null(by)) {
    b <- .split_join_pair(by, "by")
    block_probe <- b$probe
    block_build <- b$build
  }

  new_xptr <- .Call(C_interval_join_node,
                    x$.node, y$.node,
                    s$probe, e$probe,
                    s$build, e$build,
                    block_probe, block_build,
                    type, as.logical(closed),
                    as.integer(n_threads),
                    suffix,
                    as.numeric(vectra_mem()))
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}
