# Grouping and aggregation verbs: group_by, summarise, ungroup, count, tally
# Includes internal helper: parse_agg_expr

# Build the group-by + aggregate node. The memory budget is the single
# vectra_mem() knob: it is both the external sort's spill threshold and the
# per-group spill threshold for holistic aggregates (median, n_distinct).
.group_agg_node <- function(node, key_names, agg_specs, mem = vectra_mem()) {
  .Call(C_group_agg_node, node, key_names, agg_specs, as.numeric(mem))
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
  meta <- .strip_meta_args(dots)
  dots <- meta$dots
  # summarise() takes .by and .groups; .keep/.preserve are not summarise args,
  # so reject them rather than silently discarding (base R would error too).
  if (!is.null(meta$keep))     stop("summarise() does not support `.keep`.")
  if (!is.null(meta$preserve)) stop("summarise() does not support `.preserve`.")
  # Expand across() calls
  schema <- .Call(C_node_schema, .data$.node)
  proxy <- schema_proxy(schema)
  dots <- expand_across(dots, schema$name, parent.frame(), proxy)
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all summarise expressions must be named")

  # dplyr 1.1 `.by`: group by the selected columns for this call only and
  # return an ungrouped result. Mutually exclusive with group_by()/.groups.
  use_by <- !is.null(meta$by)
  if (use_by) {
    if (!is.null(.data$.groups))
      stop("Can't supply `.by` when `.data` is already grouped.")
    if (!is.null(.groups))
      stop("Can't supply both `.by` and `.groups`.")
    key_names <- .resolve_by_cols(meta$by, schema, parent.frame())
  } else {
    key_names <- .data$.groups
    if (is.null(key_names)) key_names <- character(0)
  }

  # Parse agg expressions. A bare aggregate (sum(x), mean(y, na.rm=TRUE)) becomes
  # an agg spec directly; a compound post-aggregation expression (mean(x)/sum(x),
  # or m2 = m * 2 referencing an earlier output) has its aggregate sub-calls
  # extracted into hidden specs, and the surrounding expression runs as a mutate
  # on the aggregated result. Nested aggregate arguments (mean(x + y)) still
  # auto-insert a hidden pre-aggregation mutate column.
  agg_specs <- list()
  mutate_exprs <- list()  # nested agg-argument exprs -> hidden pre-agg mutate
  post_dots <- list()     # compound outputs -> post-agg mutate, in declaration order
  ctx <- new.env(parent = emptyenv()); ctx$k <- 0L; ctx$specs <- list()

  add_spec <- function(sp) {
    if (!is.null(sp$.nested_expr)) {
      mutate_exprs[[sp$.nested_col]] <<- sp$.nested_expr
      sp$.nested_expr <- NULL
      sp$.nested_col <- NULL
    }
    agg_specs[[length(agg_specs) + 1L]] <<- sp
  }

  for (i in seq_along(dots)) {
    if (.summ_is_agg_call(dots[[i]])) {
      add_spec(parse_agg_expr(dots[[i]], dot_names[i]))
    } else {
      post_dots[[dot_names[i]]] <- .summ_extract_aggs(dots[[i]], ctx)
    }
  }
  for (sp in ctx$specs) add_spec(sp)

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

  new_xptr <- .group_agg_node(node$.node, key_names, agg_specs)

  # Post-aggregation expressions: apply the compound outputs as a mutate on the
  # aggregated result (which carries the group keys and the extracted temp agg
  # columns), then select the group keys plus the declared outputs in order,
  # dropping the temps. The mutate runs ungrouped and left-to-right, so a later
  # output can reference an earlier one (dplyr's sequential summarise()).
  if (length(post_dots) > 0) {
    agg_node <- structure(list(.node = new_xptr, .path = .data$.path,
                               .groups = NULL), class = "vectra_node")
    agg_node <- .apply_mutate_dots(agg_node, post_dots, parent.frame())
    final_schema <- .Call(C_node_schema, agg_node$.node)
    sel_names <- c(key_names, dot_names)
    sel_exprs <- lapply(sel_names, function(nm)
      serialize_expr(as.name(nm), parent.frame(), final_schema$name))
    new_xptr <- .Call(C_project_node, agg_node$.node, sel_names, sel_exprs)
  }

  # Determine residual grouping. `.by` always yields an ungrouped result.
  if (use_by) {
    result_groups <- NULL
  } else {
    if (is.null(.groups)) .groups <- "drop_last"
    result_groups <- switch(.groups,
      drop_last = if (length(key_names) > 1) key_names[-length(key_names)] else NULL,
      drop = NULL,
      keep = key_names,
      stop(sprintf(".groups must be 'drop_last', 'drop', or 'keep', got '%s'", .groups))
    )
  }

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
  # count() on grouped data groups by the existing group_by() keys plus the
  # count columns, as dplyr does (group_by(g) |> count(b) counts per g, b).
  existing <- if (!is.null(x$.groups)) x$.groups else character(0)
  grp_names <- unique(c(existing, grp_names))
  cnt_name <- if (!is.null(name)) name else "n"
  wt_expr <- substitute(wt)

  # Build the grouped summarise
  node <- x
  if (length(grp_names) > 0) {
    node <- structure(list(.node = node$.node, .path = node$.path,
                           .groups = grp_names), class = "vectra_node")
  }

  wt <- .count_wt_agg(node, wt_expr, cnt_name, parent.frame())
  node <- wt$node
  agg_specs <- list(wt$spec)

  new_xptr <- .group_agg_node(node$.node, grp_names, agg_specs)
  if (sort) {
    sort_xptr <- .sort_node(new_xptr, cnt_name, TRUE)
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

  wt <- .count_wt_agg(x, wt_expr, cnt_name, parent.frame())
  node <- wt$node
  agg_specs <- list(wt$spec)

  new_xptr <- .group_agg_node(node$.node, key_names, agg_specs)
  if (sort) {
    sort_xptr <- .sort_node(new_xptr, cnt_name, TRUE)
    return(structure(list(.node = sort_xptr, .path = node$.path), class = "vectra_node"))
  }
  structure(list(.node = new_xptr, .path = node$.path), class = "vectra_node")
}

# Build the aggregation spec (and, if needed, a hidden weight column) for
# count()/tally()'s `wt`. dplyr: no wt -> row count; a bare column -> sum(col);
# an expression -> sum(<expr>). An expression weight is materialized once as a
# temp column that the sum consumes, so it never appears in the output.
.count_wt_agg <- function(node, wt_expr, cnt_name, env) {
  if (is.null(wt_expr) || identical(wt_expr, quote(NULL))) {
    return(list(node = node,
                spec = list(name = cnt_name, kind = "n", col = NULL,
                            na_rm = FALSE)))
  }
  # dplyr sums weights with na.rm = TRUE.
  if (is.name(wt_expr)) {
    return(list(node = node,
                spec = list(name = cnt_name, kind = "sum",
                            col = as.character(wt_expr), na_rm = TRUE)))
  }
  # Expression weight: materialize a hidden column, then sum it.
  cur_schema <- .Call(C_node_schema, node$.node)
  existing <- cur_schema$name
  tmp_name <- ".vectra_wt"
  out_names <- c(existing, tmp_name)
  out_exprs <- c(vector("list", length(existing)),
                 list(serialize_expr(wt_expr, env, existing)))
  new_xptr <- .Call(C_project_node, node$.node, out_names, out_exprs)
  node2 <- structure(list(.node = new_xptr, .path = node$.path,
                          .groups = node$.groups), class = "vectra_node")
  list(node = node2,
       spec = list(name = cnt_name, kind = "sum", col = tmp_name, na_rm = TRUE))
}

.summ_valid_aggs <- c("n", "sum", "mean", "min", "max", "sd", "var", "first",
                      "last", "any", "all", "median", "n_distinct")

# Is `expr` a bare aggregate call (top-level function is an aggregate)? Handles
# namespace qualification (vectra::mean). A compound expression such as
# mean(x) + 1 or a / b is not a bare aggregate and takes the post-agg path.
# A namespace-qualified top-level call (vectra::foo(x)) is always treated as an
# aggregation attempt so an unknown name reports the clean "unknown aggregation
# function" error from parse_agg_expr rather than the scalar post-path.
.summ_is_agg_call <- function(expr) {
  if (!is.call(expr)) return(FALSE)
  fc <- expr[[1L]]
  if (is.call(fc) && length(fc) == 3L && is.name(fc[[1L]]) &&
      as.character(fc[[1L]]) %in% c("::", ":::"))
    return(TRUE)
  is.name(fc) && as.character(fc) %in% .summ_valid_aggs
}

# Rewrite a post-aggregation expression: every aggregate sub-call is replaced by
# a reference to a hidden temp column and parsed into an agg spec (accumulated in
# ctx$specs, named .__sagg_k__). The returned expression computes the summarise
# output from those temp columns, the group keys, and earlier outputs -- run as a
# mutate on the aggregated result, mirroring dplyr's sequential summarise().
.summ_extract_aggs <- function(expr, ctx) {
  if (.summ_is_agg_call(expr)) {
    ctx$k <- ctx$k + 1L
    tmp <- paste0(".__sagg_", ctx$k, "__")
    ctx$specs[[length(ctx$specs) + 1L]] <- parse_agg_expr(expr, tmp)
    return(as.name(tmp))
  }
  if (is.call(expr)) {
    idx <- seq_along(expr)
    for (i in idx[-1L]) expr[[i]] <- .summ_extract_aggs(expr[[i]], ctx)
    return(expr)
  }
  expr
}

# Parse an aggregation expression like sum(x), mean(y, na.rm = TRUE), n()
# Supports nested expressions: mean(x + y) auto-inserts a hidden mutate column.
parse_agg_expr <- function(expr, output_name) {
  if (!is.call(expr))
    stop(sprintf("summarise expression '%s' must be a function call", output_name))

  fn_call <- expr[[1]]
  # Accept namespace-qualified calls like vectra::n() or dplyr:::sum()
  if (is.call(fn_call) && length(fn_call) == 3L && is.name(fn_call[[1L]]) &&
      as.character(fn_call[[1L]]) %in% c("::", ":::")) {
    fn <- as.character(fn_call[[3L]])
  } else if (is.name(fn_call)) {
    fn <- as.character(fn_call)
  } else {
    stop(sprintf(
      "summarise expression '%s' must call a named aggregation function, got %s",
      output_name, deparse(fn_call)))
  }
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

  # median and n_distinct are native C aggregations. A bare column feeds the C
  # aggregation directly; anything else (an expression like median(x + y), or the
  # .data[[var]] pronoun) is materialized into a hidden mutate column first, the
  # same nested-expression path mean()/sum() use below.
  if (fn == "median" || fn == "n_distinct") {
    if (is.name(col_arg))
      return(list(name = output_name, kind = fn, col = as.character(col_arg),
                  na_rm = na_rm))
    tmp_name <- paste0(".vectra_tmp_", output_name)
    return(list(name = output_name, kind = fn, col = tmp_name, na_rm = na_rm,
                .nested_expr = col_arg, .nested_col = tmp_name))
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
