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
expand_across <- function(dots, schema_names, env, proxy = NULL) {
  result_names <- character(0)
  result_exprs <- list()

  for (i in seq_along(dots)) {
    expr <- dots[[i]]
    nm <- names(dots)[i]

    # Check if this is an across() call
    if (is.call(expr) && identical(expr[[1]], as.name("across"))) {
      expanded <- do_expand_across(expr, schema_names, env, proxy)
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

# Substitute the purrr lambda pronoun (.x, or the bare .) with a column symbol
# throughout an expression tree, so a formula like ~ mean(.x, na.rm = TRUE)
# becomes mean(<col>, na.rm = TRUE).
.subst_across_dot <- function(e, sym) {
  if (is.name(e)) {
    nm <- as.character(e)
    if (nm == ".x" || nm == ".") return(sym)
    return(e)
  }
  if (is.call(e)) {
    for (i in seq_along(e)) e[[i]] <- .subst_across_dot(e[[i]], sym)
    return(e)
  }
  e
}

# Substitute a specific formal-argument symbol (e.g. the `x` of \(x) ...) with a
# column symbol throughout an expression tree.
.subst_named_sym <- function(e, name, sym) {
  if (is.name(e)) {
    if (identical(as.character(e), name)) return(sym)
    return(e)
  }
  if (is.call(e)) {
    for (i in seq_along(e)) e[[i]] <- .subst_named_sym(e[[i]], name, sym)
    return(e)
  }
  e
}

# Inline an anonymous closure (\(x) body) by substituting its first formal
# argument with the target column symbol -- the closure analogue of the formula
# lambda path.
.subst_across_closure <- function(fn_obj, sym) {
  fmls <- formals(fn_obj)
  if (length(fmls) < 1L)
    stop("across(): an anonymous function must take at least one argument")
  .subst_named_sym(body(fn_obj), names(fmls)[1], sym)
}

# TRUE when a string is a single syntactic R name (so it came from resolving a
# named function), FALSE for a deparsed anonymous closure ("function (x) ...").
.is_syntactic_name <- function(s) {
  length(s) == 1L && nzchar(s) && grepl("^[.a-zA-Z][.a-zA-Z0-9._]*$", s)
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
  # Try matching known functions. get() must be guarded: some of these (e.g.
  # "n") are not base objects, and an unguarded get() would error out before an
  # anonymous lambda reaches the deparse fallback below.
  for (nm in c("sum", "mean", "min", "max", "n", "sd", "var", "median")) {
    fx <- tryCatch(get(nm, envir = baseenv(), inherits = TRUE),
                   error = function(e) NULL)
    if (!is.null(fx) && identical(fn, fx))
      return(nm)
  }
  # Fallback: deparse
  deparse(fn)[1]
}

do_expand_across <- function(expr, schema_names, env, proxy = NULL) {
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

  # Extra arguments (the `...` of across, e.g. na.rm = TRUE) are forwarded to
  # every generated call. Everything after the first two positional args, minus
  # the `.names` pattern.
  extra_args <- if (length(args) > 2) args[-(1:2)] else list()
  if (length(extra_args)) {
    en <- names(extra_args)
    if (!is.null(en)) extra_args <- extra_args[is.na(en) | en != ".names"]
  }

  # Resolve column selection via typed proxy (enables where())
  if (is.null(proxy)) {
    named_cols <- schema_names
    names(named_cols) <- schema_names
    proxy <- named_cols
  }
  sel <- tidyselect::eval_select(cols_expr, data = proxy, env = env)
  selected_cols <- unname(schema_names[sel])

  # Evaluate fns
  fns <- eval(fns_expr, env)

  # Handle different fn formats. Formulas are kept as formulas (not coerced to
  # functions) so the purrr-style lambda body (~ .x + 1, ~ mean(.x, na.rm=TRUE))
  # can be inlined by substituting .x / . with the column, which the expression
  # serializer understands. Plain functions resolve to a name-string call.
  if (is.function(fns) || rlang::is_formula(fns)) {
    fn_list <- list(fns)
    fn_names <- NULL
  } else if (is.list(fns)) {
    fn_list <- fns
    fn_names <- names(fns)
  } else {
    stop("across .fns must be a function, formula, or named list")
  }

  # {.fn} in .names is the function's name when named, else its 1-based position
  # ("1", "2", ...), matching dplyr -- including the single-unnamed-function case
  # (dplyr substitutes {.fn} = "1"). Whether the default naming appends "_{.fn}"
  # still depends on there being several or named functions.
  append_fn <- !is.null(fn_names) || length(fn_list) > 1

  # Generate expressions
  result <- list()
  for (fi in seq_along(fn_list)) {
    fn_label <- if (!is.null(fn_names) && nzchar(fn_names[fi])) fn_names[fi]
                else as.character(fi)
    for (col in selected_cols) {
      # Determine output name
      if (!is.null(names_pattern)) {
        out_name <- names_pattern
        out_name <- gsub("{.col}", col, out_name, fixed = TRUE)
        out_name <- gsub("{.fn}", fn_label, out_name, fixed = TRUE)
      } else if (append_fn) {
        out_name <- paste0(col, "_", fn_label)
      } else {
        out_name <- col
      }

      # Build the call expression for this (fn, col). A formula lambda (~ .x + 1)
      # and an anonymous closure (\(x) x + 1) both inline their body with the
      # lambda argument replaced by the column symbol; a named function becomes
      # fn(col, <extra args>) with the across `...` forwarded.
      fn_obj <- fn_list[[fi]]
      if (rlang::is_formula(fn_obj)) {
        call_expr <- .subst_across_dot(rlang::f_rhs(fn_obj), as.name(col))
      } else {
        fn_str <- resolve_fn_str(fn_obj)
        if (is.function(fn_obj) && !is.primitive(fn_obj) &&
            !.is_syntactic_name(fn_str)) {
          call_expr <- .subst_across_closure(fn_obj, as.name(col))
        } else {
          call_expr <- as.call(c(list(as.name(fn_str), as.name(col)), extra_args))
        }
      }
      if (out_name %in% names(result))
        stop(sprintf(paste0("across() would produce the duplicate output name '%s'. ",
                            "Names must be unique -- use a .names pattern with ",
                            "{.fn} (e.g. .names = \"{.col}_{.fn}\")."), out_name))
      result[[out_name]] <- call_expr
    }
  }

  result
}
