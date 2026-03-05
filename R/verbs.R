#' Sort rows by column values
#'
#' @param .data A `vectra_node` object.
#' @param ... Column names (unquoted). Wrap in [desc()] for descending order.
#'
#' @return A new `vectra_node` with sorted rows.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> arrange(desc(mpg)) |> collect() |> head()
#' unlink(f)
#'
#' @export
arrange <- function(.data, ...) {
  UseMethod("arrange")
}

#' @export
arrange.vectra_node <- function(.data, ...) {
  dots <- eval(substitute(alist(...)))
  if (length(dots) == 0) return(.data)

  col_names <- character(length(dots))
  desc_flags <- logical(length(dots))
  for (i in seq_along(dots)) {
    expr <- dots[[i]]
    if (is.call(expr) && identical(expr[[1]], as.name("desc"))) {
      col_names[i] <- as.character(expr[[2]])
      desc_flags[i] <- TRUE
    } else {
      col_names[i] <- as.character(expr)
      desc_flags[i] <- FALSE
    }
  }

  new_xptr <- .Call(C_sort_node, .data$.node, col_names, desc_flags)
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' Mark a column for descending sort order
#'
#' Used inside [arrange()] to sort a column in descending order.
#'
#' @param x A column name.
#'
#' @return A marker used by [arrange()].
#'
#' @export
desc <- function(x) {
  structure(x, desc = TRUE)
}

#' Filter rows of a vectra query
#'
#' @param .data A `vectra_node` object.
#' @param ... Filter expressions (combined with `&`).
#'
#' @return A new `vectra_node` with the filter applied.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> filter(cyl > 4) |> collect() |> head()
#' unlink(f)
#'
#' @export
filter <- function(.data, ...) {
  UseMethod("filter")
}

#' @export
filter.vectra_node <- function(.data, ...) {
  exprs <- eval(substitute(alist(...)))
  if (length(exprs) == 0) return(.data)
  pred <- combine_predicates(exprs, parent.frame())
  new_xptr <- .Call(C_filter_node, .data$.node, pred)
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' Select columns from a vectra query
#'
#' @param .data A `vectra_node` object.
#' @param ... Column names (unquoted).
#'
#' @return A new `vectra_node` with only the selected columns.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> select(mpg, cyl) |> collect() |> head()
#' unlink(f)
#'
#' @export
select <- function(.data, ...) {
  UseMethod("select")
}

#' @export
select.vectra_node <- function(.data, ...) {
  schema <- .Call(C_node_schema, .data$.node)
  col_names <- schema$name
  names(col_names) <- col_names

  sel <- tidyselect::eval_select(rlang::expr(c(...)), data = col_names)
  out_names <- names(sel)

  n <- length(out_names)
  expr_lists <- vector("list", n)
  # If renamed (name differs from original), use col_ref
  orig_names <- unname(col_names[sel])
  for (i in seq_len(n)) {
    if (out_names[i] != orig_names[i]) {
      expr_lists[[i]] <- list(kind = "col_ref", name = orig_names[i])
    }
  }

  new_xptr <- .Call(C_project_node, .data$.node, out_names, expr_lists)
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' Add or transform columns
#'
#' @param .data A `vectra_node` object.
#' @param ... Named expressions for new or transformed columns.
#'
#' @return A new `vectra_node` with mutated columns.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> mutate(kpl = mpg * 0.425144) |> collect() |> head()
#' unlink(f)
#'
#' @export
mutate <- function(.data, ...) {
  UseMethod("mutate")
}

#' @export
mutate.vectra_node <- function(.data, ...) {
  dots <- eval(substitute(alist(...)))
  # Expand across() calls
  schema <- .Call(C_node_schema, .data$.node)
  dots <- expand_across(dots, schema$name, parent.frame())
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all mutate expressions must be named")

  # Split into window functions and regular expressions
  split <- split_window_exprs(dots)
  node <- .data

  # Apply window functions first (if any)
  if (length(split$win_specs) > 0) {
    node <- create_window_node(node, split$win_specs)
  }

  # Apply regular expressions (if any)
  if (length(split$regular_dots) > 0) {
    schema <- .Call(C_node_schema, node$.node)
    existing_names <- schema$name

    out_names <- character(0)
    out_exprs <- list()

    for (nm in existing_names) {
      out_names <- c(out_names, nm)
      out_exprs <- c(out_exprs, list(NULL))
    }

    for (i in seq_along(split$regular_dots)) {
      nm <- split$regular_names[i]
      expr_ser <- serialize_expr(split$regular_dots[[i]], parent.frame())
      idx <- match(nm, out_names)
      if (!is.na(idx)) {
        out_exprs[[idx]] <- expr_ser
      } else {
        out_names <- c(out_names, nm)
        out_exprs <- c(out_exprs, list(expr_ser))
      }
    }

    new_xptr <- .Call(C_project_node, node$.node, out_names, out_exprs)
    node <- structure(list(.node = new_xptr, .path = node$.path,
                           .groups = node$.groups), class = "vectra_node")
  }

  # If window node added columns that should not have been pass-through
  # but no regular exprs, just return the window node
  node
}

#' Group a vectra query by columns
#'
#' @param .data A `vectra_node` object.
#' @param ... Grouping column names (unquoted).
#'
#' @return A `vectra_node` with grouping information stored.
#'
#' @export
group_by <- function(.data, ...) {
  UseMethod("group_by")
}

#' @export
group_by.vectra_node <- function(.data, ...) {
  grp_exprs <- eval(substitute(alist(...)))
  grp_names <- vapply(grp_exprs, as.character, character(1))
  structure(list(.node = .data$.node, .path = .data$.path,
                 .groups = grp_names),
            class = "vectra_node")
}

#' Summarise grouped data
#'
#' @param .data A grouped `vectra_node` (from [group_by()]).
#' @param ... Named aggregation expressions using `n()`, `sum()`, `mean()`,
#'   `min()`, `max()`.
#' @param .groups How to handle groups in the result. One of `"drop_last"`
#'   (default), `"drop"`, or `"keep"`.
#'
#' @return A `vectra_node` with one row per group.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> group_by(cyl) |> summarise(avg_mpg = mean(mpg)) |> collect()
#' unlink(f)
#'
#' @export
summarise <- function(.data, ..., .groups = NULL) {
  UseMethod("summarise")
}

#' @export
summarise.vectra_node <- function(.data, ..., .groups = NULL) {
  dots <- eval(substitute(alist(...)))
  # Expand across() calls
  schema <- .Call(C_node_schema, .data$.node)
  dots <- expand_across(dots, schema$name, parent.frame())
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all summarise expressions must be named")

  key_names <- .data$.groups
  if (is.null(key_names)) key_names <- character(0)

  agg_specs <- vector("list", length(dots))
  for (i in seq_along(dots)) {
    agg_specs[[i]] <- parse_agg_expr(dots[[i]], dot_names[i])
  }

  new_xptr <- .Call(C_group_agg_node, .data$.node, key_names, agg_specs)

  # Determine residual grouping
  if (is.null(.groups)) .groups <- "drop_last"
  result_groups <- switch(.groups,
    drop_last = if (length(key_names) > 1) key_names[-length(key_names)] else NULL,
    drop = NULL,
    keep = key_names,
    stop(sprintf(".groups must be 'drop_last', 'drop', or 'keep', got '%s'", .groups))
  )

  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = result_groups), class = "vectra_node")
}

#' @rdname summarise
#' @export
summarize <- summarise

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
  names(existing) <- existing

  sel <- tidyselect::eval_rename(rlang::expr(c(...)), data = existing)
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
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
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
#' @export
relocate <- function(.data, ..., .before = NULL, .after = NULL) {
  UseMethod("relocate")
}

#' @export
relocate.vectra_node <- function(.data, ..., .before = NULL, .after = NULL) {
  schema <- .Call(C_node_schema, .data$.node)
  existing <- schema$name
  names(existing) <- existing

  sel <- tidyselect::eval_select(rlang::expr(c(...)), data = existing)
  to_move <- unname(existing[sel])

  .before <- if (!missing(.before)) {
    bsel <- tidyselect::eval_select(rlang::enquo(.before), data = existing)
    unname(existing[bsel])
  } else NULL
  .after <- if (!missing(.after)) {
    asel <- tidyselect::eval_select(rlang::enquo(.after), data = existing)
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
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
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
#' @export
transmute <- function(.data, ...) {
  UseMethod("transmute")
}

#' @export
transmute.vectra_node <- function(.data, ...) {
  dots <- eval(substitute(alist(...)))
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all transmute expressions must be named")

  out_names <- character(length(dots))
  out_exprs <- vector("list", length(dots))
  for (i in seq_along(dots)) {
    out_names[i] <- dot_names[i]
    out_exprs[[i]] <- serialize_expr(dots[[i]], parent.frame())
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
  col_exprs <- eval(substitute(alist(...)))
  schema <- .Call(C_node_schema, .data$.node)

  if (length(col_exprs) == 0) {
    key_names <- schema$name
  } else {
    key_names <- vapply(col_exprs, as.character, character(1))
  }

  if (.keep_all && length(col_exprs) > 0) {
    # TODO: keep_all with subset of columns needs first-row-per-group
    # For now, fall back to collect + base R
    df <- collect(.data)
    return(df[!duplicated(df[, key_names, drop = FALSE]), , drop = FALSE])
  }

  # Use group_agg with zero aggregations to get unique key combos
  agg_specs <- list()
  new_xptr <- .Call(C_group_agg_node, .data$.node, key_names, agg_specs)
  structure(list(.node = new_xptr, .path = .data$.path), class = "vectra_node")
}

#' Remove grouping from a vectra query
#'
#' @param x A `vectra_node` object.
#' @param ... Ignored.
#'
#' @return An ungrouped `vectra_node`.
#'
#' @export
ungroup <- function(x, ...) {
  UseMethod("ungroup")
}

#' @export
ungroup.vectra_node <- function(x, ...) {
  structure(list(.node = x$.node, .path = x$.path), class = "vectra_node")
}

#' Count observations by group
#'
#' @param x A `vectra_node` object.
#' @param ... Grouping columns (unquoted).
#' @param wt Column to weight by (unquoted). If `NULL`, counts rows.
#' @param sort If `TRUE`, sort output in descending order of `n`.
#' @param name Name of the count column (default `"n"`).
#'
#' @return A `vectra_node` with group columns and a count column.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> count(cyl) |> collect()
#' unlink(f)
#'
#' @export
count <- function(x, ..., wt = NULL, sort = FALSE, name = NULL) {
  UseMethod("count")
}

#' @export
count.vectra_node <- function(x, ..., wt = NULL, sort = FALSE, name = NULL) {
  grp_exprs <- eval(substitute(alist(...)))
  grp_names <- vapply(grp_exprs, as.character, character(1))
  cnt_name <- if (!is.null(name)) name else "n"
  wt_expr <- substitute(wt)

  # Build the grouped summarise
  node <- x
  if (length(grp_names) > 0) {
    node <- structure(list(.node = node$.node, .path = node$.path,
                           .groups = grp_names), class = "vectra_node")
  }

  if (is.null(wt_expr) || identical(wt_expr, quote(NULL))) {
    agg_specs <- list(list(name = cnt_name, kind = "n", col = NULL, na_rm = FALSE))
  } else {
    wt_name <- as.character(wt_expr)
    agg_specs <- list(list(name = cnt_name, kind = "sum", col = wt_name, na_rm = FALSE))
  }

  new_xptr <- .Call(C_group_agg_node, node$.node, grp_names, agg_specs)
  structure(list(.node = new_xptr, .path = node$.path), class = "vectra_node")
}

#' @rdname count
#' @export
tally <- function(x, wt = NULL, sort = FALSE, name = NULL) {
  UseMethod("tally")
}

#' @export
tally.vectra_node <- function(x, wt = NULL, sort = FALSE, name = NULL) {
  cnt_name <- if (!is.null(name)) name else "n"
  wt_expr <- substitute(wt)
  key_names <- if (!is.null(x$.groups)) x$.groups else character(0)

  if (is.null(wt_expr) || identical(wt_expr, quote(NULL))) {
    agg_specs <- list(list(name = cnt_name, kind = "n", col = NULL, na_rm = FALSE))
  } else {
    wt_name <- as.character(wt_expr)
    agg_specs <- list(list(name = cnt_name, kind = "sum", col = wt_name, na_rm = FALSE))
  }

  new_xptr <- .Call(C_group_agg_node, x$.node, key_names, agg_specs)
  structure(list(.node = new_xptr, .path = x$.path), class = "vectra_node")
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
#' @param with_ties If `TRUE`, include ties. Currently ignored.
#'
#' @return A `vectra_node` or data.frame.
#'
#' @export
slice_head <- function(.data, n = 1L) {
  UseMethod("slice_head")
}

#' @export
slice_head.vectra_node <- function(.data, n = 1L) {
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
  order_col <- as.character(substitute(order_by))
  # Sort ascending by column, then take first n
  col_names <- order_col
  desc_flags <- FALSE
  new_xptr <- .Call(C_sort_node, .data$.node, col_names, desc_flags)
  sorted <- structure(list(.node = new_xptr, .path = .data$.path),
                      class = "vectra_node")
  slice_head(sorted, n = n)
}

#' @rdname slice_head
#' @export
slice_max <- function(.data, order_by, n = 1L, with_ties = TRUE) {
  UseMethod("slice_max")
}

#' @export
slice_max.vectra_node <- function(.data, order_by, n = 1L, with_ties = TRUE) {
  order_col <- as.character(substitute(order_by))
  # Sort descending by column, then take first n
  col_names <- order_col
  desc_flags <- TRUE
  new_xptr <- .Call(C_sort_node, .data$.node, col_names, desc_flags)
  sorted <- structure(list(.node = new_xptr, .path = .data$.path),
                      class = "vectra_node")
  slice_head(sorted, n = n)
}

# Parse an aggregation expression like sum(x), mean(y, na.rm = TRUE), n()
parse_agg_expr <- function(expr, output_name) {
  if (!is.call(expr))
    stop(sprintf("summarise expression '%s' must be a function call", output_name))

  fn <- as.character(expr[[1]])
  valid_aggs <- c("n", "sum", "mean", "min", "max")
  if (!fn %in% valid_aggs)
    stop(sprintf("unknown aggregation function: %s. Use one of: %s",
                 fn, paste(valid_aggs, collapse = ", ")))

  if (fn == "n") {
    return(list(name = output_name, kind = "n", col = NULL, na_rm = FALSE))
  }

  # Extract column name (first argument)
  col_arg <- expr[[2]]
  if (!is.name(col_arg))
    stop(sprintf("aggregation column must be a column name, got: %s",
                 deparse(col_arg)))
  col_name <- as.character(col_arg)

  # Check for na.rm argument
  na_rm <- FALSE
  if (length(expr) >= 3) {
    arg_names <- names(expr)
    if (!is.null(arg_names)) {
      idx <- match("na.rm", arg_names)
      if (!is.na(idx)) {
        na_rm <- isTRUE(eval(expr[[idx]]))
      }
    }
  }

  list(name = output_name, kind = fn, col = col_name, na_rm = na_rm)
}
