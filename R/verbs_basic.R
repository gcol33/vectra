# Basic dplyr verbs: arrange, desc, filter, select, mutate
# (including window function detection logic inside mutate)

#' Sort rows by column values
#'
#' @param .data A `vectra_node` object.
#' @param ... Column names (unquoted). Wrap in [desc()] for descending order.
#'
#' @return A new `vectra_node` with sorted rows.
#'
#' @details
#' Uses an external merge sort with a 1 GB memory budget. When data exceeds
#' this limit, sorted runs are spilled to temporary `.vtr` files and merged
#' via a k-way min-heap. NAs sort last in ascending order.
#'
#' This is a materializing operation.
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
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = .data$.groups), class = "vectra_node")
}

#' Mark a column for descending sort order
#'
#' Used inside [arrange()] to sort a column in descending order.
#'
#' @param x A column name.
#'
#' @return A marker used by [arrange()].
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> arrange(desc(mpg)) |> collect() |> head()
#' unlink(f)
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
#' @details
#' Filter uses zero-copy selection vectors: matching rows are indexed without
#' copying data. Multiple conditions are combined with `&`. Supported
#' expression types: arithmetic (`+`, `-`, `*`, `/`, `%%`), comparison
#' (`==`, `!=`, `<`, `<=`, `>`, `>=`), boolean (`&`, `|`, `!`), `is.na()`,
#' and string functions (`nchar()`, `substr()`, `grepl()` with fixed patterns).
#'
#' NA comparisons return NA (SQL semantics). Use `is.na()` to filter NAs
#' explicitly.
#'
#' This is a streaming operation (constant memory per batch).
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
  schema <- .Call(C_node_schema, .data$.node)
  pred <- combine_predicates(exprs, parent.frame(), schema$name)
  new_xptr <- .Call(C_filter_node, .data$.node, pred)
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = .data$.groups), class = "vectra_node")
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
  proxy <- schema_proxy(schema)

  sel <- tidyselect::eval_select(rlang::expr(c(...)), data = proxy)
  col_names <- schema$name
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
  # Drop group columns that were removed by select
  grps <- .data$.groups
  if (!is.null(grps)) {
    grps <- intersect(grps, out_names)
    if (length(grps) == 0) grps <- NULL
  }
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = grps), class = "vectra_node")
}

#' Add or transform columns
#'
#' @param .data A `vectra_node` object.
#' @param ... Named expressions for new or transformed columns.
#'
#' @return A new `vectra_node` with mutated columns.
#'
#' @details
#' Supported expression types: arithmetic (`+`, `-`, `*`, `/`, `%%`),
#' comparison, boolean, `is.na()`, `nchar()`, `substr()`, `grepl()` (fixed
#' match only). Window functions (`row_number()`, `rank()`, `dense_rank()`,
#' `lag()`, `lead()`, `cumsum()`, `cummean()`, `cummin()`, `cummax()`) are
#' detected automatically and routed to a dedicated window node.
#'
#' When grouped, window functions respect partition boundaries.
#'
#' This is a streaming operation for regular expressions; window functions
#' materialize all rows within each partition.
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
  proxy <- schema_proxy(schema)
  dots <- expand_across(dots, schema$name, parent.frame(), proxy)
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
      expr_ser <- serialize_expr(split$regular_dots[[i]], parent.frame(), existing_names)
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
