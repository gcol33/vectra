# Convert a scalar R value to a literal expression node
.env_val_to_literal <- function(varname, val) {
  if (is.logical(val) && length(val) == 1) return(list(kind = "lit_logical", value = val))
  if (is.integer(val) && length(val) == 1) return(list(kind = "lit_integer", value = val))
  if (is.double(val) && length(val) == 1) return(list(kind = "lit_double", value = val))
  if (is.character(val) && length(val) == 1) return(list(kind = "lit_string", value = val))
  stop(sprintf(".env$%s must be a scalar logical/integer/double/string, got %s of length %d",
               varname, class(val)[1], length(val)))
}

# NSE expression capture -> serialized list for C bridge

# ---------------------------------------------------------------------------
# Dispatch table: function name -> handler
# Each handler receives (fn, expr, env, cols) and returns a serialized list,
# or NULL if the function name is not handled by that group.
# ---------------------------------------------------------------------------

# Arithmetic operators: +, -, *, /, %%
.serialize_arith <- function(fn, expr, env, cols) {
  if (length(expr) == 2 && fn == "-") {
    return(list(kind = "negate",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  op <- if (fn == "%%") "%" else fn
  list(kind = "arith", op = op,
       left = serialize_expr(expr[[2]], env, cols),
       right = serialize_expr(expr[[3]], env, cols))
}

# Comparison operators: ==, !=, <, <=, >, >=
.serialize_cmp <- function(fn, expr, env, cols) {
  list(kind = "cmp", op = fn,
       left = serialize_expr(expr[[2]], env, cols),
       right = serialize_expr(expr[[3]], env, cols))
}

# Boolean operators: &, &&, |, ||, !
.serialize_bool <- function(fn, expr, env, cols) {
  if (fn == "!" ) {
    return(list(kind = "bool", op = "!",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  op <- if (fn %in% c("&", "&&")) "&" else "|"
  list(kind = "bool", op = op,
       left = serialize_expr(expr[[2]], env, cols),
       right = serialize_expr(expr[[3]], env, cols))
}

# Math functions: abs, sqrt, log, exp, floor, ceiling, round, log2, log10,
#                 sign, trunc, pmin, pmax
.serialize_math <- function(fn, expr, env, cols) {
  if (fn == "pmin" || fn == "pmax") {
    return(list(kind = fn,
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }
  fn_char <- switch(fn,
    abs = "a", sqrt = "s", log = "l", exp = "e",
    floor = "f", ceiling = "c", round = "r",
    log2 = "2", log10 = "t", sign = "g", trunc = "u")
  list(kind = "math_unary", fn = fn_char,
       operand = serialize_expr(expr[[2]], env, cols))
}

# String functions: nchar, substr, substring, grepl, gsub, sub, str_extract,
#   tolower, toupper, trimws, paste, paste0, startsWith, endsWith
.serialize_string <- function(fn, expr, env, cols) {
  if (fn == "nchar") {
    return(list(kind = "nchar",
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  if (fn == "substr" || fn == "substring") {
    return(list(kind = "substr",
                operand = serialize_expr(expr[[2]], env, cols),
                start = serialize_expr(expr[[3]], env, cols),
                stop = serialize_expr(expr[[4]], env, cols)))
  }

  if (fn == "grepl") {
    pattern <- expr[[2]]
    x <- expr[[3]]
    if (!is.character(pattern))
      stop("grepl: pattern must be a string literal")
    fixed <- TRUE
    nms <- names(expr)
    if (!is.null(nms)) {
      fi <- match("fixed", nms)
      if (!is.na(fi)) fixed <- isTRUE(eval(expr[[fi]], env))
    }
    return(list(kind = "grepl",
                pattern = as.character(pattern),
                operand = serialize_expr(x, env, cols),
                fixed = fixed))
  }

  if (fn %in% c("tolower", "toupper", "trimws")) {
    return(list(kind = fn,
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  if (fn == "paste0") {
    args <- lapply(as.list(expr)[-1], serialize_expr, env = env, cols = cols)
    return(list(kind = "paste", args = args, sep = NULL))
  }

  if (fn == "paste") {
    call_args <- as.list(expr)[-1]
    nms <- names(call_args)
    sep <- " "
    data_args <- call_args
    if (!is.null(nms)) {
      si <- match("sep", nms)
      if (!is.na(si)) {
        sep <- as.character(eval(call_args[[si]], env))
        data_args <- call_args[-si]
      }
      ci <- match("collapse", names(data_args))
      if (!is.na(ci)) data_args <- data_args[-ci]
    }
    args <- lapply(data_args, serialize_expr, env = env, cols = cols)
    return(list(kind = "paste", args = args, sep = sep))
  }

  if (fn == "startsWith") {
    prefix <- expr[[3]]
    if (!is.character(prefix)) stop("startsWith: prefix must be a string literal")
    return(list(kind = "startsWith", prefix = as.character(prefix),
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  if (fn == "endsWith") {
    suffix <- expr[[3]]
    if (!is.character(suffix)) stop("endsWith: suffix must be a string literal")
    return(list(kind = "endsWith", suffix = as.character(suffix),
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  if (fn == "gsub" || fn == "sub") {
    pattern <- expr[[2]]
    replacement <- expr[[3]]
    x <- expr[[4]]
    if (!is.character(pattern)) stop(paste0(fn, ": pattern must be a string literal"))
    if (!is.character(replacement)) stop(paste0(fn, ": replacement must be a string literal"))
    fixed <- TRUE
    nms <- names(expr)
    if (!is.null(nms)) {
      fi <- match("fixed", nms)
      if (!is.na(fi)) fixed <- isTRUE(eval(expr[[fi]], env))
    }
    return(list(kind = fn,
                pattern = as.character(pattern),
                replacement = as.character(replacement),
                operand = serialize_expr(x, env, cols),
                fixed = fixed))
  }

  if (fn == "str_extract") {
    x <- expr[[2]]
    pattern <- expr[[3]]
    if (!is.character(pattern))
      stop("str_extract: pattern must be a string literal")
    return(list(kind = "str_extract",
                pattern = as.character(pattern),
                operand = serialize_expr(x, env, cols)))
  }

  NULL
}

# Control flow: case_when, coalesce, ifelse, if_else
.serialize_control_flow <- function(fn, expr, env, cols) {
  if (fn == "case_when") {
    call_args <- as.list(expr)[-1]
    nms <- names(call_args)
    cases <- list()
    default_expr <- NULL
    for (i in seq_along(call_args)) {
      nm <- if (!is.null(nms)) nms[i] else ""
      a <- call_args[[i]]
      if (nm == ".default") {
        default_expr <- serialize_expr(a, env, cols)
      } else if (is.call(a) && identical(a[[1]], as.name("~"))) {
        lhs <- a[[2]]
        rhs <- a[[3]]
        if (is.logical(lhs) && isTRUE(lhs)) {
          default_expr <- serialize_expr(rhs, env, cols)
        } else {
          cases[[length(cases) + 1]] <- list(
            cond = serialize_expr(lhs, env, cols),
            val  = serialize_expr(rhs, env, cols))
        }
      } else {
        stop("case_when: each argument must be a formula (condition ~ value) or .default")
      }
    }
    return(list(kind = "case_when", cases = cases, default = default_expr))
  }

  if (fn == "coalesce") {
    args <- lapply(as.list(expr)[-1], serialize_expr, env = env, cols = cols)
    return(list(kind = "coalesce", args = args))
  }

  # if_else / ifelse
  list(kind = "if_else",
       cond = serialize_expr(expr[[2]], env, cols),
       then_expr = serialize_expr(expr[[3]], env, cols),
       else_expr = serialize_expr(expr[[4]], env, cols))
}

# Type casting: as.numeric, as.double, as.integer, as.character, as.logical
.serialize_cast <- function(fn, expr, env, cols) {
  to <- switch(fn,
    as.numeric = , as.double = "double",
    as.integer = "int64",
    as.character = "string",
    as.logical = "bool")
  list(kind = "cast", to = to,
       operand = serialize_expr(expr[[2]], env, cols))
}

# Date functions: year, month, day, hour, minute, second, as.Date, as.POSIXct
.serialize_date <- function(fn, expr, env, cols) {
  if (fn %in% c("year", "month", "day", "hour", "minute", "second")) {
    part_char <- switch(fn, year = "Y", month = "M", day = "D",
                        hour = "h", minute = "m", second = "s")
    return(list(kind = "date_part", part = part_char,
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  if (fn == "as.Date") {
    arg <- expr[[2]]
    if (is.character(arg)) {
      d <- as.Date(arg)
      return(list(kind = "lit_double", value = as.double(d)))
    }
    return(list(kind = "as_date",
                operand = serialize_expr(arg, env, cols)))
  }

  # as.POSIXct
  arg <- expr[[2]]
  if (is.character(arg)) {
    tz <- "UTC"
    if (length(expr) >= 3) {
      arg_names <- names(expr)
      if (!is.null(arg_names)) {
        tz_idx <- match("tz", arg_names)
        if (!is.na(tz_idx)) tz <- as.character(eval(expr[[tz_idx]], env))
      }
    }
    d <- as.POSIXct(arg, tz = tz)
    return(list(kind = "lit_double", value = as.double(d)))
  }
  list(kind = "cast", to = "double",
       operand = serialize_expr(arg, env, cols))
}

# Fuzzy matching: levenshtein, levenshtein_norm, dl_dist, dl_dist_norm,
#                 jaro_winkler
.serialize_fuzzy <- function(fn, expr, env, cols) {
  args <- as.list(expr[-1])

  if (fn %in% c("levenshtein", "dl_dist")) {
    res <- list(kind = fn,
                operand = serialize_expr(args[[1]], env, cols),
                pattern = serialize_expr(args[[2]], env, cols))
    nm <- names(args)
    if (length(args) >= 3) {
      md <- if (!is.null(nm) && "max_dist" %in% nm) args[["max_dist"]] else args[[3]]
      res$max_dist <- eval(md, env)
    }
    return(res)
  }

  # levenshtein_norm, dl_dist_norm, jaro_winkler
  list(kind = fn,
       operand = serialize_expr(args[[1]], env, cols),
       pattern = serialize_expr(args[[2]], env, cols))
}

# Set operations: %in%, between
.serialize_set_ops <- function(fn, expr, env, cols) {
  if (fn == "%in%") {
    set_val <- eval(expr[[3]], env)
    return(list(kind = "in",
                operand = serialize_expr(expr[[2]], env, cols),
                set = set_val))
  }
  # between
  x <- serialize_expr(expr[[2]], env, cols)
  left <- serialize_expr(expr[[3]], env, cols)
  right <- serialize_expr(expr[[4]], env, cols)
  list(kind = "bool", op = "&",
       left = list(kind = "cmp", op = ">=", left = x, right = left),
       right = list(kind = "cmp", op = "<=", left = x, right = right))
}

# Graph/lookup: resolve, propagate
.serialize_graph <- function(fn, expr, env, cols) {
  if (fn == "resolve") {
    if (length(expr) != 4)
      stop("resolve() requires exactly 3 arguments: fk_col, pk_col, value_col")
    return(list(kind = "resolve",
                fk = serialize_expr(expr[[2]], env, cols),
                pk = serialize_expr(expr[[3]], env, cols),
                val = serialize_expr(expr[[4]], env, cols)))
  }
  # propagate
  if (length(expr) != 4)
    stop("propagate() requires exactly 3 arguments: parent_fk, pk_col, seed_expr")
  list(kind = "propagate",
       parent_fk = serialize_expr(expr[[2]], env, cols),
       pk = serialize_expr(expr[[3]], env, cols),
       seed = serialize_expr(expr[[4]], env, cols))
}

# ---------------------------------------------------------------------------
# Dispatch registry: maps function names to handler functions.
# Adding a new expression type = one entry here + one handler above.
# ---------------------------------------------------------------------------
.expr_dispatch <- new.env(parent = emptyenv())

# Populate dispatch table
local({
  register <- function(names, handler) {
    for (nm in names) .expr_dispatch[[nm]] <- handler
  }

  register(c("+", "-", "*", "/", "%%"),               .serialize_arith)
  register(c("==", "!=", "<", "<=", ">", ">="),       .serialize_cmp)
  register(c("&", "&&", "|", "||", "!"),              .serialize_bool)
  register(c("abs", "sqrt", "log", "exp", "floor",
             "ceiling", "round", "log2", "log10",
             "sign", "trunc", "pmin", "pmax"),         .serialize_math)
  register(c("nchar", "substr", "substring", "grepl",
             "tolower", "toupper", "trimws", "paste0",
             "paste", "startsWith", "endsWith", "gsub",
             "sub", "str_extract"),                     .serialize_string)
  register(c("case_when", "coalesce",
             "if_else", "ifelse"),                      .serialize_control_flow)
  register(c("as.numeric", "as.double", "as.integer",
             "as.character", "as.logical"),              .serialize_cast)
  register(c("year", "month", "day", "hour", "minute",
             "second", "as.Date", "as.POSIXct"),        .serialize_date)
  register(c("levenshtein", "levenshtein_norm",
             "dl_dist", "dl_dist_norm",
             "jaro_winkler"),                            .serialize_fuzzy)
  register(c("%in%", "between"),                        .serialize_set_ops)
  register(c("resolve", "propagate"),                   .serialize_graph)
})

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
serialize_expr <- function(expr, env = parent.frame(), cols = NULL) {
  # Bare names: column refs, constants, or env lookups

  if (is.name(expr)) {
    name <- as.character(expr)
    if (name == "TRUE") return(list(kind = "lit_logical", value = TRUE))
    if (name == "FALSE") return(list(kind = "lit_logical", value = FALSE))
    if (name == "NA") return(list(kind = "lit_na"))
    if (name == "NA_real_") return(list(kind = "lit_na"))
    if (name == "NA_integer_") return(list(kind = "lit_na"))
    if (name == "NA_character_") return(list(kind = "lit_na"))
    if (!is.null(cols) && !(name %in% cols)) {
      val <- tryCatch(get(name, envir = env), error = function(e) NULL)
      if (!is.null(val)) return(.env_val_to_literal(name, val))
    }
    return(list(kind = "col_ref", name = name))
  }

  # Scalar literals
  if (is.numeric(expr) || is.logical(expr) || is.character(expr)) {
    val <- expr
    if (is.logical(val) && is.na(val)) return(list(kind = "lit_na"))
    if (is.logical(val)) return(list(kind = "lit_logical", value = val))
    if (is.integer(val) && is.na(val)) return(list(kind = "lit_na"))
    if (is.integer(val)) return(list(kind = "lit_integer", value = val))
    if (is.double(val) && is.na(val)) return(list(kind = "lit_na"))
    if (is.double(val)) return(list(kind = "lit_double", value = val))
    if (is.character(val) && is.na(val)) return(list(kind = "lit_na"))
    if (is.character(val)) return(list(kind = "lit_string", value = val))
  }

  if (!is.call(expr))
    stop(sprintf("unsupported expression type: %s", typeof(expr)))

  # .env$varname or .env[["varname"]] — evaluate in caller's environment
  if (length(expr) == 3) {
    op <- expr[[1]]
    lhs <- expr[[2]]
    if (is.name(lhs) && identical(as.character(lhs), ".env") &&
        (identical(op, quote(`$`)) || identical(op, quote(`[[`)))) {
      varname <- if (identical(op, quote(`$`))) as.character(expr[[3]]) else eval(expr[[3]], env)
      val <- get(varname, envir = env)
      return(.env_val_to_literal(varname, val))
    }
  }

  fn <- as.character(expr[[1]])

  # Parentheses — pass through
  if (fn == "(") return(serialize_expr(expr[[2]], env, cols))

  # is.na — standalone because it doesn't group with any category

  if (fn == "is.na") {
    return(list(kind = "is_na",
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # Dispatch to handler

  handler <- .expr_dispatch[[fn]]
  if (!is.null(handler)) return(handler(fn, expr, env, cols))

  stop(sprintf("unsupported function in expression: %s", fn))
}

# Combine multiple filter expressions with &
combine_predicates <- function(exprs, env, cols = NULL) {
  if (length(exprs) == 0) stop("no filter expressions provided")
  result <- serialize_expr(exprs[[1]], env, cols)
  for (i in seq_along(exprs)[-1]) {
    result <- list(kind = "bool", op = "&",
                   left = result,
                   right = serialize_expr(exprs[[i]], env, cols))
  }
  result
}
