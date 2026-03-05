# Window function support for vectra
#
# Window functions are detected inside mutate() and routed to C_window_node.
# Supported: lag(), lead(), row_number(), cumsum(), cummean(), cummin(), cummax()

# Known window function names
.win_fns <- c("lag", "lead", "row_number", "cumsum", "cummean", "cummin", "cummax")

# Check if an expression is a window function call
is_window_call <- function(expr) {
  if (!is.call(expr)) return(FALSE)
  fn <- as.character(expr[[1]])
  fn %in% .win_fns
}

# Parse a window function call into a spec list for C
parse_window_spec <- function(expr, output_name) {
  fn <- as.character(expr[[1]])

  if (fn == "row_number") {
    return(list(name = output_name, kind = "row_number", col = NULL,
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

  # cumsum, cummean, cummin, cummax: single column argument
  if (fn %in% c("cumsum", "cummean", "cummin", "cummax")) {
    col <- as.character(expr[[2]])
    return(list(name = output_name, kind = fn, col = col,
                offset = 1L, default = NULL))
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
