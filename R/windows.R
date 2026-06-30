# Window function support for vectra
#
# Window functions are detected inside mutate() and routed to C_window_node.
# Supported: lag(), lead(), row_number(), cumsum(), cummean(), cummin(), cummax()

# Known window function names
.win_fns <- c("lag", "lead", "row_number", "rank", "dense_rank",
              "cumsum", "cummean", "cummin", "cummax",
              "ntile", "percent_rank", "cume_dist",
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

  if (fn == "rank") {
    pa <- .parse_order_arg(expr[[2]])
    return(list(name = output_name, kind = "rank", col = pa$col,
                offset = 1L, default = NULL, desc = pa$desc))
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

    if (length(args) >= 2) {
      # Second arg is n (positional or named)
      if (!is.null(arg_names) && !is.na(match("n", arg_names))) {
        offset <- as.integer(eval(args[[match("n", arg_names)]]))
      } else if (length(args) >= 2 && (is.null(arg_names) || arg_names[2] == "")) {
        offset <- as.integer(eval(args[[2]]))
      }
    }

    if (!is.null(arg_names) && !is.na(match("default", arg_names))) {
      default_val <- as.double(eval(args[[match("default", arg_names)]]))
    } else if (length(args) >= 3 && (is.null(arg_names) || arg_names[3] == "")) {
      default_val <- as.double(eval(args[[3]]))
    }

    return(list(name = output_name, kind = fn, col = col,
                offset = offset, default = default_val))
  }

  if (fn == "ntile") {
    # ntile(n) - divide into n buckets
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
