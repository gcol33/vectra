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
  pred <- combine_predicates(exprs, parent.frame())
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
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> group_by(cyl) |> summarise(avg = mean(mpg)) |> collect()
#' unlink(f)
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
#'   `min()`, `max()`, `sd()`, `var()`, `first()`, `last()`, `any()`, `all()`,
#'   `median()`, `n_distinct()`.
#' @param .groups How to handle groups in the result. One of `"drop_last"`
#'   (default), `"drop"`, or `"keep"`.
#'
#' @return A `vectra_node` with one row per group.
#'
#' @details
#' Aggregation is hash-based by default. When the engine detects it is
#' advantageous, it switches to a sort-based path that can spill to disk,
#' keeping memory bounded regardless of group count.
#'
#' All aggregation functions accept `na.rm = TRUE` to skip NA values.
#' Without `na.rm`, any NA in a group poisons the result (returns NA).
#' R-matching edge cases: `sum(na.rm = TRUE)` on all-NA returns 0,
#' `mean(na.rm = TRUE)` on all-NA returns NaN, `min/max(na.rm = TRUE)` on
#' all-NA returns Inf/-Inf with a warning.
#'
#' This is a materializing operation.
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
  proxy <- schema_proxy(schema)
  dots <- expand_across(dots, schema$name, parent.frame(), proxy)
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all summarise expressions must be named")

  key_names <- .data$.groups
  if (is.null(key_names)) key_names <- character(0)

  # Parse agg expressions, detecting nested expressions like mean(x + y)
  agg_specs <- vector("list", length(dots))
  mutate_exprs <- list()  # nested exprs that need a hidden mutate
  for (i in seq_along(dots)) {
    parsed <- parse_agg_expr(dots[[i]], dot_names[i])
    if (!is.null(parsed$.nested_expr)) {
      # The inner expression needs a hidden mutate column
      tmp_name <- parsed$.nested_col
      mutate_exprs[[tmp_name]] <- parsed$.nested_expr
      parsed$.nested_expr <- NULL
      parsed$.nested_col <- NULL
    }
    agg_specs[[i]] <- parsed
  }

  # Insert hidden mutate node if there are nested expressions
  node <- .data
  if (length(mutate_exprs) > 0) {
    cur_schema <- .Call(C_node_schema, node$.node)
    existing_names <- cur_schema$name

    out_names <- existing_names
    out_exprs <- vector("list", length(existing_names))

    for (tmp_nm in names(mutate_exprs)) {
      out_names <- c(out_names, tmp_nm)
      out_exprs <- c(out_exprs, list(
        serialize_expr(mutate_exprs[[tmp_nm]], parent.frame())))
    }

    new_xptr <- .Call(C_project_node, node$.node, out_names, out_exprs)
    node <- structure(list(.node = new_xptr, .path = node$.path,
                           .groups = node$.groups), class = "vectra_node")
  }

  # Check for R-fallback aggregations (median, n_distinct)
  has_fallback <- any(vapply(agg_specs, function(s) isTRUE(s$.r_fallback), logical(1)))
  if (has_fallback) {
    df <- collect(node)
    .eval_agg <- function(spec, chunk) {
      col <- if (!is.null(spec$col)) chunk[[spec$col]] else NULL
      switch(spec$kind,
        n = nrow(chunk),
        sum = sum(col, na.rm = spec$na_rm),
        mean = mean(col, na.rm = spec$na_rm),
        min = min(col, na.rm = spec$na_rm),
        max = max(col, na.rm = spec$na_rm),
        sd = sd(col, na.rm = spec$na_rm),
        var = var(col, na.rm = spec$na_rm),
        first = col[!is.na(col)][1],
        last = rev(col[!is.na(col)])[1],
        any = any(as.logical(col), na.rm = spec$na_rm),
        all = all(as.logical(col), na.rm = spec$na_rm),
        median = median(col, na.rm = spec$na_rm),
        n_distinct = length(unique(col[!is.na(col)])))
    }
    if (is.null(key_names) || length(key_names) == 0) {
      results <- list()
      for (i in seq_along(agg_specs)) {
        results[[agg_specs[[i]]$name]] <- .eval_agg(agg_specs[[i]], df)
      }
      return(as.data.frame(results, stringsAsFactors = FALSE))
    } else {
      split_idx <- interaction(df[key_names], drop = TRUE)
      pieces <- split(df, split_idx, drop = TRUE)
      result_list <- lapply(pieces, function(chunk) {
        row <- chunk[1, key_names, drop = FALSE]
        for (i in seq_along(agg_specs)) {
          row[[agg_specs[[i]]$name]] <- .eval_agg(agg_specs[[i]], chunk)
        }
        row
      })
      result <- do.call(rbind, result_list)
      rownames(result) <- NULL
      return(result)
    }
  }

  # Remove .r_fallback flags before passing to C
  agg_specs <- lapply(agg_specs, function(s) { s$.r_fallback <- NULL; s })

  new_xptr <- .Call(C_group_agg_node, node$.node, key_names, agg_specs)

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

#' Remove grouping from a vectra query
#'
#' @param x A `vectra_node` object.
#' @param ... Ignored.
#'
#' @return An ungrouped `vectra_node`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> group_by(cyl) |> ungroup()
#' unlink(f)
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
#' @details
#' Equivalent to `group_by(...) |> summarise(n = n())`. When `wt` is
#' provided, uses `sum(wt)` instead of `n()`. When `sort = TRUE`, results
#' are sorted in descending order of the count column.
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
  if (sort) {
    sort_xptr <- .Call(C_sort_node, new_xptr, cnt_name, TRUE)
    return(structure(list(.node = sort_xptr, .path = node$.path), class = "vectra_node"))
  }
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
  if (sort) {
    sort_xptr <- .Call(C_sort_node, new_xptr, cnt_name, TRUE)
    return(structure(list(.node = sort_xptr, .path = x$.path), class = "vectra_node"))
  }
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

# Parse an aggregation expression like sum(x), mean(y, na.rm = TRUE), n()
# Supports nested expressions: mean(x + y) auto-inserts a hidden mutate column.
parse_agg_expr <- function(expr, output_name) {
  if (!is.call(expr))
    stop(sprintf("summarise expression '%s' must be a function call", output_name))

  fn <- as.character(expr[[1]])
  valid_aggs <- c("n", "sum", "mean", "min", "max", "sd", "var", "first", "last",
                   "any", "all", "median", "n_distinct")
  if (!fn %in% valid_aggs)
    stop(sprintf("unknown aggregation function: %s. Use one of: %s",
                 fn, paste(valid_aggs, collapse = ", ")))

  if (fn == "n") {
    return(list(name = output_name, kind = "n", col = NULL, na_rm = FALSE))
  }

  # Extract column argument
  col_arg <- expr[[2]]

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

  # median and n_distinct are R-level fallbacks (need all values per group)
  if (fn == "median") {
    col_name <- if (is.name(col_arg)) as.character(col_arg) else NULL
    if (is.null(col_name))
      stop("median() requires a simple column reference, not an expression")
    return(list(name = output_name, kind = "median", col = col_name,
                na_rm = na_rm, .r_fallback = TRUE))
  }

  if (fn == "n_distinct") {
    col_name <- if (is.name(col_arg)) as.character(col_arg) else NULL
    if (is.null(col_name))
      stop("n_distinct() requires a simple column reference, not an expression")
    return(list(name = output_name, kind = "n_distinct", col = col_name,
                na_rm = FALSE, .r_fallback = TRUE))
  }

  if (is.name(col_arg)) {
    # Simple column reference
    col_name <- as.character(col_arg)
    return(list(name = output_name, kind = fn, col = col_name, na_rm = na_rm))
  }

  # Nested expression: e.g. mean(x + y) or sum(x * 2)
  # Generate a temp column name and return the inner expression for mutate
  tmp_name <- paste0(".vectra_tmp_", output_name)
  list(name = output_name, kind = fn, col = tmp_name, na_rm = na_rm,
       .nested_expr = col_arg, .nested_col = tmp_name)
}
