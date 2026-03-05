#' Apply a function across multiple columns
#'
#' Used inside [mutate()] or [summarise()] to apply a function to multiple
#' columns selected with tidyselect. Returns a named list of expressions.
#'
#' @param .cols Column selection (tidyselect).
#' @param .fns A function, formula, or named list of functions.
#' @param ... Additional arguments passed to `.fns`.
#' @param .names A glue-style naming pattern. Uses `{.col}` and `{.fn}`.
#'   Default: `"{.col}"` if `.fns` is a single function,
#'   `"{.col}_{.fn}"` if `.fns` is a named list.
#'
#' @return A named list used internally by mutate/summarise.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' # In summarise (conceptual; across is expanded to individual expressions)
#' unlink(f)
#'
#' @export
across <- function(.cols, .fns, ..., .names = NULL) {
  stop("across() can only be used inside mutate() or summarise()")
}

# Internal: expand across() calls in mutate/summarise dots
# Returns a named list of expressions
expand_across <- function(dots, schema_names, env) {
  result_names <- character(0)
  result_exprs <- list()

  for (i in seq_along(dots)) {
    expr <- dots[[i]]
    nm <- names(dots)[i]

    # Check if this is an across() call
    if (is.call(expr) && identical(expr[[1]], as.name("across"))) {
      expanded <- do_expand_across(expr, schema_names, env)
      result_names <- c(result_names, names(expanded))
      result_exprs <- c(result_exprs, expanded)
    } else {
      result_names <- c(result_names, nm)
      result_exprs <- c(result_exprs, list(expr))
    }
  }

  names(result_exprs) <- result_names
  result_exprs
}

# Resolve a function to its name string (e.g., sum -> "sum")
resolve_fn_str <- function(fn) {
  # Check if it's a primitive or builtin
  if (is.primitive(fn)) {
    # Extract name from the deparse
    d <- deparse(fn)[1]
    m <- regmatches(d, regexpr('"[^"]*"', d))
    if (length(m) > 0) return(gsub('"', '', m))
  }
  # Check if it's a named function in an environment
  env <- environment(fn)
  if (!is.null(env)) {
    for (nm in ls(env)) {
      if (identical(get(nm, envir = env), fn)) return(nm)
    }
  }
  # Try matching known functions
  for (nm in c("sum", "mean", "min", "max", "n", "sd", "var", "median")) {
    if (identical(fn, get(nm, envir = baseenv(), inherits = TRUE)))
      return(nm)
  }
  # Fallback: deparse
  deparse(fn)[1]
}

do_expand_across <- function(expr, schema_names, env) {
  # Parse across(cols, fns, ..., .names = pattern)
  args <- as.list(expr)[-1]  # drop "across"
  arg_names <- names(args)

  cols_expr <- args[[1]]
  fns_expr <- args[[2]]

  # Get .names pattern if present
  names_pattern <- NULL
  if (!is.null(arg_names)) {
    nm_idx <- match(".names", arg_names)
    if (!is.na(nm_idx)) names_pattern <- eval(args[[nm_idx]], env)
  }

  # Resolve column selection
  named_cols <- schema_names
  names(named_cols) <- schema_names
  sel <- tidyselect::eval_select(cols_expr, data = named_cols)
  selected_cols <- unname(schema_names[sel])

  # Evaluate fns
  fns <- eval(fns_expr, env)

  # Handle different fn formats
  if (is.function(fns)) {
    fn_list <- list(fns)
    fn_names <- NULL
  } else if (rlang::is_formula(fns)) {
    fn_list <- list(rlang::as_function(fns))
    fn_names <- NULL
  } else if (is.list(fns)) {
    fn_list <- lapply(fns, function(f) {
      if (rlang::is_formula(f)) rlang::as_function(f) else f
    })
    fn_names <- names(fns)
  } else {
    stop("across .fns must be a function, formula, or named list")
  }

  # Generate expressions
  result <- list()
  for (fi in seq_along(fn_list)) {
    for (col in selected_cols) {
      fn_name <- if (!is.null(fn_names)) fn_names[fi] else NULL

      # Determine output name
      if (!is.null(names_pattern)) {
        out_name <- names_pattern
        out_name <- gsub("{.col}", col, out_name, fixed = TRUE)
        if (!is.null(fn_name))
          out_name <- gsub("{.fn}", fn_name, out_name, fixed = TRUE)
      } else if (!is.null(fn_names)) {
        out_name <- paste0(col, "_", fn_names[fi])
      } else {
        out_name <- col
      }

      # Build the call expression: fn(col_name)
      # Resolve the actual function name from the function object
      fn_obj <- fn_list[[fi]]
      fn_str <- resolve_fn_str(fn_obj)

      call_expr <- call(fn_str, as.name(col))
      result[[out_name]] <- call_expr
    }
  }

  result
}
