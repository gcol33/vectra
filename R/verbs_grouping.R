# Grouping and aggregation verbs: group_by, summarise, ungroup, count, tally
# Includes internal helper: parse_agg_expr

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
        serialize_expr(mutate_exprs[[tmp_nm]], parent.frame(), existing_names)))
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
  if (!is.logical(sort) || length(sort) != 1 || is.na(sort))
    stop(sprintf("sort must be TRUE or FALSE, got %s", deparse(sort)))
  if (!is.null(name) && (!is.character(name) || length(name) != 1))
    stop(sprintf("name must be NULL or a single string, got %s of length %d", class(name)[1], length(name)))
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
  if (!is.logical(sort) || length(sort) != 1 || is.na(sort))
    stop(sprintf("sort must be TRUE or FALSE, got %s", deparse(sort)))
  if (!is.null(name) && (!is.character(name) || length(name) != 1))
    stop(sprintf("name must be NULL or a single string, got %s of length %d", class(name)[1], length(name)))
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

  # median and n_distinct are now native C aggregations
  if (fn == "median") {
    col_name <- if (is.name(col_arg)) as.character(col_arg) else NULL
    if (is.null(col_name))
      stop("median() requires a simple column reference, not an expression")
    return(list(name = output_name, kind = "median", col = col_name,
                na_rm = na_rm))
  }

  if (fn == "n_distinct") {
    col_name <- if (is.name(col_arg)) as.character(col_arg) else NULL
    if (is.null(col_name))
      stop("n_distinct() requires a simple column reference, not an expression")
    return(list(name = output_name, kind = "n_distinct", col = col_name,
                na_rm = FALSE))
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
