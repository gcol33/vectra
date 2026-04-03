# Utility/convenience verbs: rename, relocate, transmute, distinct, pull,
# head, tail, slice_head, slice_tail, slice_min, slice_max, slice

#' Rename columns
#'
#' @param .data A `vectra_node` object.
#' @param ... Rename pairs: `new_name = old_name`.
#'
#' @return A new `vectra_node` with renamed columns.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> rename(miles_per_gallon = mpg) |> collect() |> head()
#' unlink(f)
#'
#' @export
rename <- function(.data, ...) {
  UseMethod("rename")
}

#' @export
rename.vectra_node <- function(.data, ...) {
  schema <- .Call(C_node_schema, .data$.node)
  existing <- schema$name
  proxy <- schema_proxy(schema)

  sel <- tidyselect::eval_rename(rlang::expr(c(...)), data = proxy)
  new_names <- names(sel)
  old_names <- unname(existing[sel])

  # Build project: pass-through all columns, with col_ref exprs for renames
  out_names <- existing
  expr_lists <- vector("list", length(out_names))
  for (i in seq_along(old_names)) {
    idx <- match(old_names[i], out_names)
    out_names[idx] <- new_names[i]
    expr_lists[[idx]] <- list(kind = "col_ref", name = old_names[i])
  }
  new_xptr <- .Call(C_project_node, .data$.node, out_names, expr_lists)
  # Update group names if any were renamed
  grps <- .data$.groups
  if (!is.null(grps)) {
    for (i in seq_along(old_names)) {
      grps[grps == old_names[i]] <- new_names[i]
    }
  }
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = grps), class = "vectra_node")
}

#' Relocate columns
#'
#' @param .data A `vectra_node` object.
#' @param ... Column names to move.
#' @param .before Column name to place before (unquoted).
#' @param .after Column name to place after (unquoted).
#'
#' @return A new `vectra_node` with reordered columns.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> relocate(hp, wt, .before = cyl) |> collect() |> head()
#' unlink(f)
#'
#' @export
relocate <- function(.data, ..., .before = NULL, .after = NULL) {
  UseMethod("relocate")
}

#' @export
relocate.vectra_node <- function(.data, ..., .before = NULL, .after = NULL) {
  schema <- .Call(C_node_schema, .data$.node)
  existing <- schema$name
  proxy <- schema_proxy(schema)

  sel <- tidyselect::eval_select(rlang::expr(c(...)), data = proxy)
  to_move <- unname(existing[sel])

  .before <- if (!missing(.before)) {
    bsel <- tidyselect::eval_select(rlang::enquo(.before), data = proxy)
    unname(existing[bsel])
  } else NULL
  .after <- if (!missing(.after)) {
    asel <- tidyselect::eval_select(rlang::enquo(.after), data = proxy)
    unname(existing[asel])
  } else NULL

  remaining <- setdiff(existing, to_move)

  if (!is.null(.before)) {
    pos <- match(.before[1], remaining)
    if (is.na(pos)) stop(sprintf(".before column not found: %s", .before[1]))
    if (pos > 1) {
      out_names <- c(remaining[seq_len(pos - 1)], to_move, remaining[pos:length(remaining)])
    } else {
      out_names <- c(to_move, remaining)
    }
  } else if (!is.null(.after)) {
    pos <- match(.after[1], remaining)
    if (is.na(pos)) stop(sprintf(".after column not found: %s", .after[1]))
    if (pos < length(remaining)) {
      out_names <- c(remaining[seq_len(pos)], to_move, remaining[(pos + 1):length(remaining)])
    } else {
      out_names <- c(remaining, to_move)
    }
  } else {
    out_names <- c(to_move, remaining)
  }

  expr_lists <- vector("list", length(out_names))
  new_xptr <- .Call(C_project_node, .data$.node, out_names, expr_lists)
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = .data$.groups), class = "vectra_node")
}

#' Keep only columns from mutate expressions
#'
#' Like [mutate()] but drops all other columns.
#'
#' @param .data A `vectra_node` object.
#' @param ... Named expressions.
#'
#' @return A new `vectra_node` with only the computed columns.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> transmute(kpl = mpg * 0.425) |> collect() |> head()
#' unlink(f)
#'
#' @export
transmute <- function(.data, ...) {
  UseMethod("transmute")
}

#' @export
transmute.vectra_node <- function(.data, ...) {
  dots <- eval(substitute(alist(...)))
  # Expand across() calls
  schema <- .Call(C_node_schema, .data$.node)
  proxy <- schema_proxy(schema)
  dots <- expand_across(dots, schema$name, parent.frame(), proxy)
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all transmute expressions must be named")

  out_names <- character(length(dots))
  out_exprs <- vector("list", length(dots))
  for (i in seq_along(dots)) {
    out_names[i] <- dot_names[i]
    out_exprs[[i]] <- serialize_expr(dots[[i]], parent.frame(), schema$name)
  }

  new_xptr <- .Call(C_project_node, .data$.node, out_names, out_exprs)
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' Keep distinct/unique rows
#'
#' @param .data A `vectra_node` object.
#' @param ... Column names (unquoted). If empty, uses all columns.
#' @param .keep_all If `TRUE`, keep all columns (not just those in `...`).
#'
#' @return A `vectra_node` with unique rows.
#'
#' @details
#' Uses hash-based grouping with zero aggregations. When `.keep_all = TRUE`
#' with a column subset, falls back to R's `duplicated()` with a message.
#'
#' This is a materializing operation.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> distinct(cyl) |> collect()
#' unlink(f)
#'
#' @export
distinct <- function(.data, ..., .keep_all = FALSE) {
  UseMethod("distinct")
}

#' @export
distinct.vectra_node <- function(.data, ..., .keep_all = FALSE) {
  schema <- .Call(C_node_schema, .data$.node)
  proxy <- schema_proxy(schema)

  col_exprs <- eval(substitute(alist(...)))
  if (length(col_exprs) == 0) {
    key_names <- schema$name
  } else {
    sel <- tidyselect::eval_select(rlang::expr(c(...)), data = proxy)
    key_names <- unname(schema$name[sel])
  }

  if (.keep_all && length(col_exprs) > 0) {
    # .keep_all with subset of columns: fall back to collect + base R
    message("distinct(.keep_all = TRUE) with column subset: falling back to R")
    df <- collect(.data)
    return(df[!duplicated(df[, key_names, drop = FALSE]), , drop = FALSE])
  }

  # Use group_agg with zero aggregations to get unique key combos
  agg_specs <- list()
  new_xptr <- .Call(C_group_agg_node, .data$.node, key_names, agg_specs)
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' Extract a single column as a vector
#'
#' @param .data A `vectra_node` object.
#' @param var Column name (unquoted) or positive integer position.
#'
#' @return A vector.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> pull(mpg) |> head()
#' unlink(f)
#'
#' @export
pull <- function(.data, var = -1) {
  UseMethod("pull")
}

#' @export
pull.vectra_node <- function(.data, var = -1) {
  var_expr <- substitute(var)
  # Validate length 1 for literal values (not symbols that need schema lookup)
  if (!is.name(var_expr)) {
    val <- eval(var_expr, parent.frame())
    if (length(val) != 1)
      stop(sprintf("var must be length 1, got length %d", length(val)))
  }
  schema <- .Call(C_node_schema, .data$.node)

  if (is.name(var_expr)) {
    nm <- as.character(var_expr)
    if (nm %in% schema$name) {
      col_name <- nm
    } else {
      # Could be a variable in the caller's env
      val <- eval(var_expr, parent.frame())
      if (is.numeric(val)) {
        idx <- as.integer(val)
        if (idx < 0) idx <- length(schema$name) + idx + 1L
        if (idx < 1 || idx > length(schema$name))
          stop(sprintf("column index %d out of range (1:%d)", idx, length(schema$name)))
        col_name <- schema$name[idx]
      } else {
        col_name <- as.character(val)
      }
    }
  } else {
    val <- eval(var_expr, parent.frame())
    if (is.numeric(val)) {
      idx <- as.integer(val)
      if (idx < 0) idx <- length(schema$name) + idx + 1L
      if (idx < 1 || idx > length(schema$name))
        stop(sprintf("column index %d out of range (1:%d)", idx, length(schema$name)))
      col_name <- schema$name[idx]
    } else {
      col_name <- as.character(val)
    }
  }

  # Select just the one column, collect, extract
  expr_lists <- list(NULL)
  new_xptr <- .Call(C_project_node, .data$.node, col_name, expr_lists)
  result <- .Call(C_collect, new_xptr)
  result[[1]]
}

#' Limit results to first n rows
#'
#' @param x A `vectra_node` object.
#' @param n Number of rows to return.
#' @param ... Ignored.
#'
#' @return A data.frame with the first `n` rows.
#'
#' @importFrom utils head
#' @export
head.vectra_node <- function(x, n = 6L, ...) {
  new_xptr <- .Call(C_limit_node, x$.node, as.double(n))
  node <- structure(list(.node = new_xptr, .path = x$.path), class = "vectra_node")
  collect(node)
}

#' Select first or last rows
#'
#' @param .data A `vectra_node` object.
#' @param n Number of rows to select.
#' @param order_by Column to order by (for `slice_min`/`slice_max`).
#' @param with_ties If `TRUE` (default), includes all rows that tie with the
#'   nth value. If `FALSE`, returns exactly `n` rows.
#'
#' @return A `vectra_node` for `slice_head()` and `slice_min/max(...,
#'   with_ties = FALSE)`. A data.frame for `slice_tail()` and
#'   `slice_min/max(..., with_ties = TRUE)` (the default), since these must
#'   materialize all rows.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> slice_head(n = 3) |> collect()
#' tbl(f) |> slice_min(order_by = mpg, n = 3) |> collect()
#' tbl(f) |> slice_max(order_by = mpg, n = 3) |> collect()
#' unlink(f)
#'
#' @export
slice_head <- function(.data, n = 1L) {
  UseMethod("slice_head")
}

#' @export
slice_head.vectra_node <- function(.data, n = 1L) {
  if (!is.numeric(n) || length(n) != 1 || is.na(n) || n < 1 || n != floor(n))
    stop(sprintf("n must be a positive integer, got %s", deparse(n)))
  new_xptr <- .Call(C_limit_node, .data$.node, as.double(n))
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' @rdname slice_head
#' @export
slice_tail <- function(.data, n = 1L) {
  UseMethod("slice_tail")
}

#' @export
slice_tail.vectra_node <- function(.data, n = 1L) {
  if (!is.numeric(n) || length(n) != 1 || is.na(n) || n < 1 || n != floor(n))
    stop(sprintf("n must be a positive integer, got %s", deparse(n)))
  # Must materialize to know total rows, then take last n
  df <- collect(.data)
  nr <- nrow(df)
  if (n >= nr) return(df)
  df[(nr - n + 1):nr, , drop = FALSE]
}

#' @rdname slice_head
#' @export
slice_min <- function(.data, order_by, n = 1L, with_ties = TRUE) {
  UseMethod("slice_min")
}

#' @export
slice_min.vectra_node <- function(.data, order_by, n = 1L, with_ties = TRUE) {
  if (!is.numeric(n) || length(n) != 1 || is.na(n) || n < 1 || n != floor(n))
    stop(sprintf("n must be a positive integer, got %s", deparse(n)))
  if (!is.logical(with_ties) || length(with_ties) != 1 || is.na(with_ties))
    stop(sprintf("with_ties must be TRUE or FALSE, got %s", deparse(with_ties)))
  order_col <- as.character(substitute(order_by))
  if (!with_ties) {
    new_xptr <- .Call(C_topn_node, .data$.node, order_col, FALSE,
                      as.double(n))
    return(structure(list(.node = new_xptr, .path = .data$.path),
                     class = "vectra_node"))
  }
  # with_ties = TRUE: collect all data, sort, find the nth value, keep all
  # rows that tie with it. Must collect first because C nodes are single-use.
  df <- collect(.data)
  if (nrow(df) == 0) return(df)
  vals <- df[[order_col]]
  ord <- order(vals, na.last = TRUE)
  # Take at most n non-NA values; if fewer than n non-NA, include NAs up to n
  n_nonNA <- sum(!is.na(vals))
  take <- min(n, nrow(df))
  selected <- ord[seq_len(take)]
  result <- df[selected, , drop = FALSE]
  # Check for ties: if there are more rows beyond n with same boundary value
  if (take <= n_nonNA && take < nrow(df)) {
    boundary <- vals[ord[take]]
    extra <- which(!is.na(vals) & vals == boundary)
    all_keep <- union(selected, extra)
    result <- df[sort(all_keep), , drop = FALSE]
  }
  result[order(result[[order_col]], na.last = TRUE), , drop = FALSE]
}

#' @rdname slice_head
#' @export
slice_max <- function(.data, order_by, n = 1L, with_ties = TRUE) {
  UseMethod("slice_max")
}

#' @export
slice_max.vectra_node <- function(.data, order_by, n = 1L, with_ties = TRUE) {
  if (!is.numeric(n) || length(n) != 1 || is.na(n) || n < 1 || n != floor(n))
    stop(sprintf("n must be a positive integer, got %s", deparse(n)))
  if (!is.logical(with_ties) || length(with_ties) != 1 || is.na(with_ties))
    stop(sprintf("with_ties must be TRUE or FALSE, got %s", deparse(with_ties)))
  order_col <- as.character(substitute(order_by))
  if (!with_ties) {
    new_xptr <- .Call(C_topn_node, .data$.node, order_col, TRUE,
                      as.double(n))
    return(structure(list(.node = new_xptr, .path = .data$.path),
                     class = "vectra_node"))
  }
  df <- collect(.data)
  if (nrow(df) == 0) return(df)
  vals <- df[[order_col]]
  ord <- order(vals, decreasing = TRUE, na.last = TRUE)
  n_nonNA <- sum(!is.na(vals))
  take <- min(n, nrow(df))
  selected <- ord[seq_len(take)]
  result <- df[selected, , drop = FALSE]
  if (take <= n_nonNA && take < nrow(df)) {
    boundary <- vals[ord[take]]
    extra <- which(!is.na(vals) & vals == boundary)
    all_keep <- union(selected, extra)
    result <- df[sort(all_keep), , drop = FALSE]
  }
  result[order(result[[order_col]], decreasing = TRUE, na.last = TRUE), , drop = FALSE]
}

#' Select rows by position
#'
#' @param .data A `vectra_node` object.
#' @param ... Integer row indices (positive or negative).
#'
#' @return A data.frame with the selected rows.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> slice(1, 3, 5)
#' unlink(f)
#'
#' @export
slice <- function(.data, ...) {
  UseMethod("slice")
}

#' @export
slice.vectra_node <- function(.data, ...) {
  indices <- c(...)
  df <- collect(.data)
  if (all(indices > 0)) {
    indices <- indices[indices <= nrow(df)]
    df[indices, , drop = FALSE]
  } else if (all(indices < 0)) {
    df[indices, , drop = FALSE]
  } else {
    stop("slice indices must be all positive or all negative")
  }
}
