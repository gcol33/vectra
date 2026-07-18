# Window function support for vectra
#
# Window functions are detected inside mutate() and routed to C_window_node.
# Supported: lag(), lead(), row_number(), cumsum(), cummean(), cummin(), cummax()

# Known window function names
.win_fns <- c("lag", "lead", "row_number", "rank", "min_rank", "dense_rank",
              "cumsum", "cummean", "cummin", "cummax",
              "ntile", "percent_rank", "cume_dist", "n",
              "roll_sum", "roll_mean", "roll_min", "roll_max", "roll_n")

# Convert a time-window string ("1 hour", "15 min") to seconds. Rolling
# windows must be fixed-width; calendar units (month/quarter/year) vary in
# length and are rejected.
.window_seconds <- function(u) {
  pu <- .parse_time_unit(u)
  secs <- c(s = 1, n = 60, h = 3600, d = 86400, w = 604800)[pu$code]
  if (is.na(secs))
    stop("a rolling window must be a fixed unit (sec/min/hour/day/week), ",
         "not month/quarter/year")
  pu$n * unname(secs)
}

# Check if an expression is a window function call
is_window_call <- function(expr) {
  if (!is.call(expr)) return(FALSE)
  fn <- as.character(expr[[1]])
  fn %in% .win_fns
}

# Resolve an order argument that may be wrapped in desc(): returns col + desc.
.parse_order_arg <- function(a) {
  if (is.call(a) && identical(as.character(a[[1]]), "desc"))
    list(col = as.character(a[[2]]), desc = TRUE)
  else
    list(col = as.character(a), desc = FALSE)
}

# Parse a window function call into a spec list for C
parse_window_spec <- function(expr, output_name) {
  fn <- as.character(expr[[1]])

  if (fn == "row_number") {
    # row_number() -> input order; row_number(col) / row_number(desc(col)) ->
    # ordered, deterministic 1..n within each partition.
    if (length(expr) >= 2) {
      pa <- .parse_order_arg(expr[[2]])
      return(list(name = output_name, kind = "row_number", col = pa$col,
                  offset = 1L, default = NULL, desc = pa$desc))
    }
    return(list(name = output_name, kind = "row_number", col = NULL,
                offset = 1L, default = NULL, desc = FALSE))
  }

  if (fn == "rank" || fn == "min_rank") {
    # dplyr's min_rank() is rank with ties.method = "min", which is exactly this
    # engine's rank window. Bare rank() keeps that "min" default (established
    # behaviour); base::rank's "average" is available when requested explicitly.
    pa <- .parse_order_arg(expr[[2]])
    kind <- "rank"
    if (fn == "rank") {
      args <- as.list(expr)[-1]
      an <- names(args)
      if (!is.null(an) && !is.na(match("ties.method", an))) {
        tm <- as.character(eval(args[[match("ties.method", an)]]))
        if (identical(tm, "average"))   kind <- "avg_rank"
        else if (identical(tm, "min"))  kind <- "rank"
        else stop(sprintf(paste0("rank(ties.method = \"%s\") is not supported; ",
                                 "use \"min\" (dplyr min_rank) or \"average\"."), tm))
      }
    }
    return(list(name = output_name, kind = kind, col = pa$col,
                offset = 1L, default = NULL, desc = pa$desc))
  }

  if (fn == "n") {
    # dplyr n() inside mutate(): the partition (or group) size repeated per row.
    if (length(expr) != 1L)
      stop("n() takes no arguments")
    return(list(name = output_name, kind = "n", col = NULL,
                offset = 1L, default = NULL))
  }

  if (fn == "dense_rank") {
    col <- as.character(expr[[2]])
    return(list(name = output_name, kind = "dense_rank", col = col,
                offset = 1L, default = NULL))
  }

  if (fn %in% c("lag", "lead")) {
    # lag(col, n = 1, default = NA)
    col <- as.character(expr[[2]])
    offset <- 1L
    default_val <- NULL

    args <- as.list(expr)[-1]  # drop function name
    arg_names <- names(args)

    if (!is.null(arg_names) && !is.na(match("order_by", arg_names)))
      stop(sprintf(paste0("%s(order_by=) is not supported; arrange() the data ",
                          "first, then %s() operates in that order"), fn, fn))

    if (length(args) >= 2) {
      # Second arg is n (positional or named)
      if (!is.null(arg_names) && !is.na(match("n", arg_names))) {
        offset <- as.integer(eval(args[[match("n", arg_names)]]))
      } else if (length(args) >= 2 && (is.null(arg_names) || arg_names[2] == "")) {
        offset <- as.integer(eval(args[[2]]))
      }
    }

    # dplyr keeps the default in the column's type. The C window node stores a
    # single numeric default (Rf_asReal) and always emits a double column, so a
    # numeric or logical default is carried through as its literal value (not
    # force-coerced) and a string default -- which the engine cannot represent
    # -- is rejected rather than silently turned into NA.
    default_raw <- NULL
    if (!is.null(arg_names) && !is.na(match("default", arg_names))) {
      default_raw <- eval(args[[match("default", arg_names)]])
    } else if (length(args) >= 3 && (is.null(arg_names) || arg_names[3] == "")) {
      default_raw <- eval(args[[3]])
    }
    if (!is.null(default_raw)) {
      if (length(default_raw) != 1L)
        stop(sprintf("%s(default=) must be a length-1 value", fn))
      if (is.na(default_raw)) {
        default_val <- NULL             # NA default == no default (engine fills NA)
      } else if (is.character(default_raw)) {
        stop(sprintf(paste0("%s(default=) with a string value is not supported: ",
                            "the window engine returns a numeric column, so a ",
                            "string default cannot be represented. Use ",
                            "default = NA or omit it."), fn))
      } else if (is.numeric(default_raw) || is.logical(default_raw)) {
        default_val <- default_raw      # carry the literal; C coerces via Rf_asReal
      } else if (is.double(unclass(default_raw))) {
        default_val <- as.double(default_raw)  # Date/POSIXct etc.: double-backed
      } else {
        stop(sprintf("%s(default=) must be numeric, logical, or NA", fn))
      }
    }

    return(list(name = output_name, kind = fn, col = col,
                offset = offset, default = default_val))
  }

  if (fn == "ntile") {
    # ntile(n) - divide arrival order into n buckets. dplyr's ntile(x, n)
    # orders by x first, which this engine does not do inline.
    if (length(expr) >= 3)
      stop("ntile(order_col, n) with an ordering column is not supported; ",
           "arrange() the data first, then use ntile(n)")
    n_tiles <- as.integer(eval(expr[[2]]))
    return(list(name = output_name, kind = "ntile", col = NULL,
                offset = n_tiles, default = NULL))
  }

  if (fn == "percent_rank") {
    col <- as.character(expr[[2]])
    return(list(name = output_name, kind = "percent_rank", col = col,
                offset = 1L, default = NULL))
  }

  if (fn == "cume_dist") {
    col <- as.character(expr[[2]])
    return(list(name = output_name, kind = "cume_dist", col = col,
                offset = 1L, default = NULL))
  }

  # cumsum, cummean, cummin, cummax: single column argument
  if (fn %in% c("cumsum", "cummean", "cummin", "cummax")) {
    col <- as.character(expr[[2]])
    return(list(name = output_name, kind = fn, col = col,
                offset = 1L, default = NULL))
  }

  # Time-based rolling aggregates: roll_*(value, time, every).
  if (fn %in% c("roll_sum", "roll_mean", "roll_min", "roll_max")) {
    col <- as.character(expr[[2]])
    order <- as.character(expr[[3]])
    win <- .window_seconds(eval(expr[[4]]))
    return(list(name = output_name, kind = fn, col = col,
                order = order, window = win, offset = 1L, default = NULL))
  }

  # roll_n(time, every): count of rows in the trailing window (no value column).
  if (fn == "roll_n") {
    order <- as.character(expr[[2]])
    win <- .window_seconds(eval(expr[[3]]))
    return(list(name = output_name, kind = "roll_n", col = NULL,
                order = order, window = win, offset = 1L, default = NULL))
  }

  stop(sprintf("unsupported window function: %s", fn))
}

# Split mutate dots into window specs and regular expressions.
# Returns list(win_specs, win_names, regular_dots, regular_names)
split_window_exprs <- function(dots) {
  dot_names <- names(dots)
  win_specs <- list()
  win_names <- character(0)
  reg_dots <- list()
  reg_names <- character(0)

  for (i in seq_along(dots)) {
    if (is_window_call(dots[[i]])) {
      spec <- parse_window_spec(dots[[i]], dot_names[i])
      win_specs <- c(win_specs, list(spec))
      win_names <- c(win_names, dot_names[i])
    } else {
      reg_dots <- c(reg_dots, list(dots[[i]]))
      reg_names <- c(reg_names, dot_names[i])
    }
  }

  names(reg_dots) <- reg_names
  list(win_specs = win_specs, win_names = win_names,
       regular_dots = reg_dots, regular_names = reg_names)
}

# Create a window node from a vectra_node and window specs
create_window_node <- function(.data, win_specs) {
  key_names <- if (!is.null(.data$.groups)) .data$.groups else character(0)
  new_xptr <- .Call(C_window_node, .data$.node, key_names, win_specs)
  structure(list(.node = new_xptr, .path = .data$.path,
                 .groups = .data$.groups), class = "vectra_node")
}

# Argument position(s) of a window call that name a column (call element index,
# where [[1]] is the function). A compound expression there is hoisted into a
# temp column so e.g. cumsum(x + y) or rank(desc(a * b)) works like dplyr.
.win_col_argpos <- function(fn) {
  switch(fn,
         row_number   = 2L,
         rank = , min_rank = , dense_rank = ,
         percent_rank = , cume_dist = ,
         cumsum = , cummean = , cummin = , cummax = ,
         lag = , lead = 2L,
         roll_sum = , roll_mean = , roll_min = , roll_max = c(2L, 3L),
         roll_n       = 2L,
         integer(0))            # ntile has no column argument to hoist
}

# Hoist compound column arguments of window calls into temp columns. Returns the
# rewritten dots (window calls now reference temp symbols), `pre` (named exprs to
# materialize before the window node), and `drop` (temp names to remove after).
.hoist_window_args <- function(dots) {
  pre <- list(); drop <- character(0); k <- 0L
  out <- dots
  for (i in seq_along(dots)) {
    e <- dots[[i]]
    if (!is_window_call(e)) next
    fn <- as.character(e[[1]])
    for (pos in .win_col_argpos(fn)) {
      if (length(e) < pos) next
      a <- e[[pos]]
      if (is.name(a) || is.atomic(a) || is.null(a)) next   # bare col / constant
      is_desc <- is.call(a) && identical(as.character(a[[1]]), "desc")
      if (is_desc && (is.name(a[[2]]) || is.atomic(a[[2]]))) next  # desc(col)
      k <- k + 1L
      tnm <- paste0(".__win_arg", k, "__")
      if (is_desc) {
        pre[[tnm]] <- a[[2]]
        e[[pos]] <- call("desc", as.name(tnm))
      } else {
        pre[[tnm]] <- a
        e[[pos]] <- as.name(tnm)
      }
      drop <- c(drop, tnm)
    }
    out[[i]] <- e
  }
  list(dots = out, pre = pre, drop = unique(drop))
}

# Materialize a named list of expressions as new columns on `node` (one project
# node, each evaluated against the current input schema).
.window_materialize <- function(node, exprs, env) {
  schema <- .Call(C_node_schema, node$.node)
  out_names <- schema$name
  out_exprs <- vector("list", length(out_names))
  for (nm in names(exprs)) {
    out_names <- c(out_names, nm)
    out_exprs <- c(out_exprs, list(serialize_expr(exprs[[nm]], env, schema$name)))
  }
  new_xptr <- .Call(C_project_node, node$.node, out_names, out_exprs)
  structure(list(.node = new_xptr, .path = node$.path,
                 .groups = node$.groups), class = "vectra_node")
}

# Drop named columns from `node` via a pass-through projection of the survivors.
.window_drop <- function(node, drop) {
  schema <- .Call(C_node_schema, node$.node)
  keep <- setdiff(schema$name, drop)
  new_xptr <- .Call(C_project_node, node$.node, keep,
                    vector("list", length(keep)))
  structure(list(.node = new_xptr, .path = node$.path,
                 .groups = node$.groups), class = "vectra_node")
}
