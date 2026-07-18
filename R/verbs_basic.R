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
  tmp_exprs <- list()   # computed sort keys to materialize then drop
  for (i in seq_along(dots)) {
    expr <- dots[[i]]
    desc <- FALSE
    # Unwrap a descending wrapper: desc(<x>) or a leading unary minus -<x>.
    if (is.call(expr) && identical(expr[[1]], as.name("desc")) &&
        length(expr) == 2L) {
      expr <- expr[[2]]; desc <- TRUE
    } else if (is.call(expr) && identical(expr[[1]], as.name("-")) &&
               length(expr) == 2L) {
      expr <- expr[[2]]; desc <- TRUE
    }
    if (is.name(expr)) {
      col_names[i] <- as.character(expr)
    } else {
      # arrange(x + y), arrange(desc(x * 2)): dplyr sorts by arbitrary
      # expressions. Materialize the expression into a hidden sort key, sort by
      # it, then drop it, instead of erroring on the multi-token as.character().
      tnm <- sprintf(".__arrange_key%d__", i)
      tmp_exprs[[tnm]] <- expr
      col_names[i] <- tnm
    }
    desc_flags[i] <- desc
  }

  node <- .data
  if (length(tmp_exprs) > 0)
    node <- .window_materialize(node, tmp_exprs, parent.frame())

  new_xptr <- .sort_node(node$.node, col_names, desc_flags)
  out <- structure(list(.node = new_xptr, .path = node$.path,
                        .groups = node$.groups), class = "vectra_node")
  if (length(tmp_exprs) > 0)
    out <- .window_drop(out, names(tmp_exprs))
  out
}

# Single R entry point for every sort node: threads the resolved memory budget
# (the external-sort spill threshold) so all sorts share one source of truth.
.sort_node <- function(node, col_names, desc, mem = vectra_mem()) {
  .Call(C_sort_node, node, col_names, desc, as.numeric(mem))
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
#' and string functions (`nchar()`, `substr()`, `grepl()`; patterns are regex
#' by default, as in base R, or literal with `fixed = TRUE`).
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
  meta <- .strip_meta_args(exprs)
  exprs <- meta$dots

  if (is.null(meta$by)) {
    # Plain filter (no `.by`): grouping passes through unchanged.
    if (length(exprs) == 0) return(.data)
    schema <- .Call(C_node_schema, .data$.node)
    exprs <- .expand_if_dots(exprs, schema, parent.frame())
    pred <- combine_predicates(exprs, parent.frame(), schema$name)
    new_xptr <- .Call(C_filter_node, .data$.node, pred)
    return(structure(list(.node = new_xptr, .path = .data$.path,
                          .groups = .data$.groups), class = "vectra_node"))
  }

  # dplyr 1.1 `.by`: the result is always ungrouped. Grouping would only change
  # the outcome for a grouped predicate (n(), row_number()), which the filter
  # node does not evaluate; the selection is still resolved here to validate it.
  if (!is.null(.data$.groups))
    stop("Can't supply `.by` when `.data` is already grouped.")
  schema <- .Call(C_node_schema, .data$.node)
  .resolve_by_cols(meta$by, schema, parent.frame())
  if (length(exprs) == 0)
    return(structure(list(.node = .data$.node, .path = .data$.path,
                          .groups = NULL), class = "vectra_node"))
  exprs <- .expand_if_dots(exprs, schema, parent.frame())
  pred <- combine_predicates(exprs, parent.frame(), schema$name)
  new_xptr <- .Call(C_filter_node, .data$.node, pred)
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = NULL), class = "vectra_node")
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

  # enquos(...) captures each selection as a quosure carrying its own
  # environment, so tidyselect helpers that reference an external variable
  # (all_of(v), any_of(cols)) resolve in the caller's frame.
  sel <- tidyselect::eval_select(
    rlang::expr(c(!!!rlang::enquos(...))), data = proxy)
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

# Apply a set of already-captured, across-expanded mutate expressions to a node
# with dplyr's left-to-right semantics, evaluating each against `env`. Shared by
# mutate() and transmute(). Returns the resulting node (all input columns pass
# through; transmute drops the extras afterward).
.apply_mutate_dots <- function(node, dots, env) {
  n <- length(dots)
  if (n == 0) return(node)
  dot_names <- names(dots)

  # dplyr evaluates a mutate() left-to-right: any expression -- window OR regular
  # -- may reference a column created earlier in the SAME call. We walk the dots
  # in declaration order and accumulate a maximal run of the same kind (window
  # vs regular) whose members do not reference an output produced earlier in
  # this call but not yet materialized. A kind switch, or a reference to a
  # pending output, flushes the run into its own node before the next begins.
  # This keeps window outputs available to later regular exprs (and vice versa),
  # preserves dplyr's new-column order, and still batches independent columns.
  schema <- .Call(C_node_schema, node$.node)
  avail   <- schema$name        # columns already materialized on `node`
  pending <- character(0)       # output names named in the current run

  seg_dots  <- list()
  seg_names <- character(0)
  seg_win   <- NA               # TRUE = window run, FALSE = regular run

  flush_regular <- function() {
    out_names <- avail
    out_exprs <- vector("list", length(avail))   # NULL = pass-through
    for (k in seq_along(seg_names)) {
      ser <- serialize_expr(seg_dots[[k]], env, avail)
      idx <- match(seg_names[k], out_names)
      if (!is.na(idx)) {
        out_exprs[[idx]] <- ser
      } else {
        out_names <- c(out_names, seg_names[k])
        out_exprs <- c(out_exprs, list(ser))
      }
    }
    new_xptr <- .Call(C_project_node, node$.node, out_names, out_exprs)
    node  <<- structure(list(.node = new_xptr, .path = node$.path,
                             .groups = node$.groups), class = "vectra_node")
    avail <<- out_names
  }

  flush_window <- function() {
    specs_dots <- seg_dots
    names(specs_dots) <- seg_names
    hoist <- .hoist_window_args(specs_dots)
    hd <- hoist$dots
    win_specs <- lapply(seq_along(hd),
                        function(k) parse_window_spec(hd[[k]], names(hd)[k]))
    if (length(hoist$pre) > 0)  node <<- .window_materialize(node, hoist$pre, env)
    node <<- create_window_node(node, win_specs)
    if (length(hoist$drop) > 0) node <<- .window_drop(node, hoist$drop)
    avail <<- .Call(C_node_schema, node$.node)$name
  }

  flush <- function() {
    if (length(seg_dots) == 0) return(invisible(NULL))
    if (isTRUE(seg_win)) flush_window() else flush_regular()
    seg_dots  <<- list()
    seg_names <<- character(0)
    seg_win   <<- NA
    pending   <<- character(0)
  }

  for (i in seq_len(n)) {
    expr <- dots[[i]]
    is_win <- is_window_call(expr)
    refs   <- all.vars(expr)
    # Flush when the kind changes or this dot depends on a pending output.
    if (length(seg_dots) > 0 &&
        (!identical(is_win, seg_win) || any(refs %in% pending)))
      flush()
    seg_dots  <- c(seg_dots, list(expr))
    seg_names <- c(seg_names, dot_names[i])
    seg_win   <- is_win
    pending   <- c(pending, dot_names[i])
  }
  flush()

  node
}

#' @export
mutate.vectra_node <- function(.data, ...) {
  dots <- eval(substitute(alist(...)))
  meta <- .strip_meta_args(dots)
  dots <- meta$dots
  .check_mutate_keep(meta$keep, parent.frame())
  if (!is.null(meta$preserve)) stop("mutate() does not support `.preserve`.")

  # Expand across() and if_any()/if_all() calls
  schema <- .Call(C_node_schema, .data$.node)
  proxy <- schema_proxy(schema)
  dots <- expand_across(dots, schema$name, parent.frame(), proxy)
  dots <- .expand_if_dots(dots, schema, parent.frame())
  dot_names <- names(dots)
  if (is.null(dot_names) || any(dot_names == ""))
    stop("all mutate expressions must be named")

  # dplyr 1.1 `.by`: run grouped by the selected columns, then return an
  # ungrouped result. Reuses the same `.groups` plumbing group_by() sets.
  data <- .data
  if (!is.null(meta$by)) {
    if (!is.null(.data$.groups))
      stop("Can't supply `.by` when `.data` is already grouped.")
    by_cols <- .resolve_by_cols(meta$by, schema, parent.frame())
    data <- structure(list(.node = .data$.node, .path = .data$.path,
                           .groups = by_cols), class = "vectra_node")
  }

  out <- .apply_mutate_dots(data, dots, parent.frame())
  if (!is.null(meta$by)) out$.groups <- NULL
  out
}

# ---------------------------------------------------------------------------
# dplyr 1.1 meta-arguments (.by / .keep / .preserve) and if_any()/if_all().
# Shared by mutate()/filter() (this file) and summarise() (verbs_grouping.R).
# ---------------------------------------------------------------------------

# Split the dplyr 1.1 meta-arguments out of a captured alist of dots so they are
# never mistaken for data expressions. Returns the remaining dots plus each meta
# value (still unevaluated) or NULL when absent.
.strip_meta_args <- function(dots) {
  out <- list(dots = dots, by = NULL, keep = NULL, preserve = NULL)
  nms <- names(dots)
  if (is.null(nms)) return(out)
  drop <- integer(0)
  for (key in c(".by", ".keep", ".preserve")) {
    idx <- match(key, nms)
    if (!is.na(idx)) {
      out[[substring(key, 2L)]] <- dots[[idx]]
      drop <- c(drop, idx)
    }
  }
  if (length(drop)) out$dots <- dots[-drop]
  out
}

# Resolve a `.by` selection to column names via tidyselect, so bare symbols,
# c(a, b), and selection helpers all behave like dplyr's `.by`.
.resolve_by_cols <- function(by_expr, schema, env) {
  proxy <- schema_proxy(schema)
  sel <- tidyselect::eval_select(by_expr, data = proxy, env = env)
  unname(schema$name[sel])
}

# mutate()'s `.keep`: every column already passes through the project node, so
# "all" is a no-op. Any other value would need column-dropping the engine does
# not do here, so reject it clearly rather than silently ignoring it.
.check_mutate_keep <- function(keep, env) {
  if (is.null(keep)) return(invisible())
  kv <- tryCatch(eval(keep, env), error = function(e) NULL)
  if (!is.character(kv) || length(kv) != 1L)
    stop(".keep must be a single string")
  if (!identical(kv, "all"))
    stop(sprintf("mutate(.keep=) supports only \"all\" here, got \"%s\"", kv))
  invisible()
}

# Expand any if_any()/if_all() calls in a list of captured expressions into a
# plain boolean combination the expression serializer already understands. A
# no-op (returns the input) when no such call is present, so ordinary dots stay
# byte-identical.
.expand_if_dots <- function(dots, schema, env) {
  if (!any(vapply(dots, .contains_if_call, logical(1)))) return(dots)
  proxy <- schema_proxy(schema)
  lapply(dots, .expand_if_calls, schema_names = schema$name,
         env = env, proxy = proxy)
}

.contains_if_call <- function(expr) {
  if (!is.call(expr)) return(FALSE)
  h <- expr[[1]]
  if (is.name(h) && as.character(h) %in% c("if_any", "if_all")) return(TRUE)
  for (i in seq_along(expr)[-1]) {
    a <- expr[[i]]
    if (!missing(a) && .contains_if_call(a)) return(TRUE)
  }
  FALSE
}

# Recursively rewrite if_any()/if_all() subcalls in an expression tree.
.expand_if_calls <- function(expr, schema_names, env, proxy) {
  if (!is.call(expr)) return(expr)
  h <- expr[[1]]
  if (is.name(h) && as.character(h) %in% c("if_any", "if_all"))
    return(.expand_if_any_all(expr, schema_names, env, proxy))
  for (i in seq_along(expr)[-1]) {
    a <- expr[[i]]
    if (!missing(a))
      expr[[i]] <- .expand_if_calls(a, schema_names, env, proxy)
  }
  expr
}

# Expand a single if_any()/if_all(.cols, .fns, ...) into the row-wise boolean
# reduction dplyr defines: if_all -> AND across columns, if_any -> OR. The
# per-column predicate is built the same way across() builds its calls (formula
# lambda with .x/. substituted, or fn(col, <extra args>)), so no machinery is
# duplicated.
.expand_if_any_all <- function(expr, schema_names, env, proxy) {
  fn <- as.character(expr[[1]])
  op <- if (fn == "if_all") "&" else "|"
  args <- as.list(expr)[-1]
  nms <- names(args)
  if (is.null(nms)) nms <- rep("", length(args))

  cols_expr <- NULL
  fns_expr <- NULL
  positional <- list()
  extra_args <- list()
  for (i in seq_along(args)) {
    if (nms[i] == ".cols") cols_expr <- args[[i]]
    else if (nms[i] == ".fns") fns_expr <- args[[i]]
    else if (nms[i] == "") positional[[length(positional) + 1L]] <- args[[i]]
    else extra_args[[nms[i]]] <- args[[i]]
  }
  if (is.null(cols_expr) && length(positional) >= 1L) {
    cols_expr <- positional[[1L]]; positional <- positional[-1L]
  }
  if (is.null(fns_expr) && length(positional) >= 1L) {
    fns_expr <- positional[[1L]]; positional <- positional[-1L]
  }
  if (is.null(cols_expr)) stop(sprintf("%s() requires a column selection", fn))
  if (is.null(fns_expr))
    stop(sprintf("%s() requires a predicate function or formula", fn))

  sel <- tidyselect::eval_select(cols_expr, data = proxy, env = env)
  cols <- unname(schema_names[sel])
  if (length(cols) == 0L) stop(sprintf("%s() selected no columns", fn))

  fns <- eval(fns_expr, env)
  make_pred <- function(col) {
    if (rlang::is_formula(fns)) {
      .subst_across_dot(rlang::f_rhs(fns), as.name(col))
    } else if (is.function(fns)) {
      fn_str <- resolve_fn_str(fns)
      if (!is.primitive(fns) && !.is_syntactic_name(fn_str)) {
        # anonymous closure (\(x) ...): inline its body, like across() does
        .subst_across_closure(fns, as.name(col))
      } else {
        as.call(c(list(as.name(fn_str), as.name(col)), extra_args))
      }
    } else {
      stop(sprintf("%s() .fns must be a function or formula", fn))
    }
  }
  preds <- lapply(cols, make_pred)
  Reduce(function(a, b) call(op, a, b), preds)
}
