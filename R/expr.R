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

serialize_expr <- function(expr, env = parent.frame(), cols = NULL) {
  if (is.name(expr)) {
    name <- as.character(expr)
    # Check if it's a known R constant
    if (name == "TRUE") return(list(kind = "lit_logical", value = TRUE))
    if (name == "FALSE") return(list(kind = "lit_logical", value = FALSE))
    if (name == "NA") return(list(kind = "lit_na"))
    if (name == "NA_real_") return(list(kind = "lit_na"))
    if (name == "NA_integer_") return(list(kind = "lit_na"))
    if (name == "NA_character_") return(list(kind = "lit_na"))
    # If schema is available: column wins, otherwise try env (dplyr data masking)
    if (!is.null(cols) && !(name %in% cols)) {
      val <- tryCatch(get(name, envir = env), error = function(e) NULL)
      if (!is.null(val)) return(.env_val_to_literal(name, val))
    }
    return(list(kind = "col_ref", name = name))
  }

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
  if (is.call(expr) && length(expr) == 3) {
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

  # Arithmetic operators
  if (fn %in% c("+", "-", "*", "/", "%%")) {
    if (length(expr) == 2 && fn == "-") {
      # Unary minus
      return(list(kind = "negate",
                  operand = serialize_expr(expr[[2]], env, cols)))
    }
    op <- if (fn == "%%") "%" else fn
    return(list(kind = "arith", op = op,
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }

  # Comparison operators
  if (fn %in% c("==", "!=", "<", "<=", ">", ">=")) {
    return(list(kind = "cmp", op = fn,
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }

  # Boolean operators
  if (fn == "&" || fn == "&&") {
    return(list(kind = "bool", op = "&",
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }
  if (fn == "|" || fn == "||") {
    return(list(kind = "bool", op = "|",
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }
  if (fn == "!") {
    return(list(kind = "bool", op = "!",
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # is.na
  if (fn == "is.na") {
    return(list(kind = "is_na",
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # Parentheses
  if (fn == "(") {
    return(serialize_expr(expr[[2]], env, cols))
  }

  # String functions
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
    # grepl(pattern, x) — pattern must be a literal string
    if (!is.character(pattern))
      stop("grepl: pattern must be a string literal")
    return(list(kind = "grepl",
                pattern = as.character(pattern),
                operand = serialize_expr(x, env, cols)))
  }

  # Math functions
  if (fn %in% c("abs", "sqrt", "log", "exp", "floor", "ceiling", "round")) {
    fn_char <- switch(fn, abs = "a", sqrt = "s", log = "l", exp = "e",
                      floor = "f", ceiling = "c", round = "r")
    return(list(kind = "math_unary", fn = fn_char,
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # if_else(cond, true, false)
  if (fn == "if_else" || fn == "ifelse") {
    return(list(kind = "if_else",
                cond = serialize_expr(expr[[2]], env, cols),
                then_expr = serialize_expr(expr[[3]], env, cols),
                else_expr = serialize_expr(expr[[4]], env, cols)))
  }

  # Type casting
  if (fn %in% c("as.numeric", "as.double")) {
    return(list(kind = "cast", to = "double",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  if (fn == "as.integer") {
    return(list(kind = "cast", to = "int64",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  if (fn == "as.character") {
    return(list(kind = "cast", to = "string",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  if (fn == "as.logical") {
    return(list(kind = "cast", to = "bool",
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # String functions
  if (fn == "tolower") {
    return(list(kind = "tolower",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  if (fn == "toupper") {
    return(list(kind = "toupper",
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  if (fn == "trimws") {
    return(list(kind = "trimws",
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # Additional math functions
  if (fn %in% c("log2", "log10", "sign", "trunc")) {
    fn_char <- switch(fn, log2 = "2", log10 = "t", sign = "g", trunc = "u")
    return(list(kind = "math_unary", fn = fn_char,
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # paste0(a, b) — two-argument string concatenation
  if (fn == "paste0") {
    if (length(expr) != 3)
      stop("paste0 in vectra supports exactly 2 arguments")
    return(list(kind = "paste0",
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }

  # startsWith / endsWith
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

  # gsub / sub (fixed string replacement)
  if (fn == "gsub" || fn == "sub") {
    pattern <- expr[[2]]
    replacement <- expr[[3]]
    x <- expr[[4]]
    if (!is.character(pattern)) stop(paste0(fn, ": pattern must be a string literal"))
    if (!is.character(replacement)) stop(paste0(fn, ": replacement must be a string literal"))
    return(list(kind = fn,
                pattern = as.character(pattern),
                replacement = as.character(replacement),
                operand = serialize_expr(x, env, cols)))
  }

  # pmin / pmax
  if (fn == "pmin" || fn == "pmax") {
    return(list(kind = fn,
                left = serialize_expr(expr[[2]], env, cols),
                right = serialize_expr(expr[[3]], env, cols)))
  }

  # Date component extraction
  if (fn %in% c("year", "month", "day", "hour", "minute", "second")) {
    part_char <- switch(fn, year = "Y", month = "M", day = "D",
                        hour = "h", minute = "m", second = "s")
    return(list(kind = "date_part", part = part_char,
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  # as.Date() from string
  if (fn == "as.Date") {
    arg <- expr[[2]]
    # If it's a literal string, evaluate it as an R Date and convert to days since epoch
    if (is.character(arg)) {
      d <- as.Date(arg)
      return(list(kind = "lit_double", value = as.double(d)))
    }
    return(list(kind = "as_date",
                operand = serialize_expr(arg, env, cols)))
  }

  # as.POSIXct() - evaluate literal to seconds since epoch
  if (fn == "as.POSIXct") {
    arg <- expr[[2]]
    if (is.character(arg)) {
      # Get timezone if provided
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
    # For column conversion, treat as cast to double (already double internally)
    return(list(kind = "cast", to = "double",
                operand = serialize_expr(arg, env, cols)))
  }

  # Levenshtein distance functions
  if (fn == "levenshtein") {
    args <- as.list(expr[-1])
    res <- list(kind = "levenshtein",
                operand = serialize_expr(args[[1]], env, cols),
                pattern = serialize_expr(args[[2]], env, cols))
    # Optional max_dist parameter (3rd arg or named)
    nm <- names(args)
    if (length(args) >= 3) {
      md <- if (!is.null(nm) && "max_dist" %in% nm) args[["max_dist"]] else args[[3]]
      res$max_dist <- eval(md, env)
    }
    return(res)
  }
  if (fn == "levenshtein_norm") {
    args <- as.list(expr[-1])
    return(list(kind = "levenshtein_norm",
                operand = serialize_expr(args[[1]], env, cols),
                pattern = serialize_expr(args[[2]], env, cols)))
  }

  # Damerau-Levenshtein distance functions
  if (fn == "dl_dist") {
    args <- as.list(expr[-1])
    res <- list(kind = "dl_dist",
                operand = serialize_expr(args[[1]], env, cols),
                pattern = serialize_expr(args[[2]], env, cols))
    nm <- names(args)
    if (length(args) >= 3) {
      md <- if (!is.null(nm) && "max_dist" %in% nm) args[["max_dist"]] else args[[3]]
      res$max_dist <- eval(md, env)
    }
    return(res)
  }
  if (fn == "dl_dist_norm") {
    args <- as.list(expr[-1])
    return(list(kind = "dl_dist_norm",
                operand = serialize_expr(args[[1]], env, cols),
                pattern = serialize_expr(args[[2]], env, cols)))
  }

  # Jaro-Winkler similarity
  if (fn == "jaro_winkler") {
    args <- as.list(expr[-1])
    return(list(kind = "jaro_winkler",
                operand = serialize_expr(args[[1]], env, cols),
                pattern = serialize_expr(args[[2]], env, cols)))
  }

  # %in% operator
  if (fn == "%in%") {
    set_val <- eval(expr[[3]], env)
    return(list(kind = "in",
                operand = serialize_expr(expr[[2]], env, cols),
                set = set_val))
  }

  # between(x, left, right) -> x >= left & x <= right
  if (fn == "between") {
    x <- serialize_expr(expr[[2]], env, cols)
    left <- serialize_expr(expr[[3]], env, cols)
    right <- serialize_expr(expr[[4]], env, cols)
    return(list(kind = "bool", op = "&",
                left = list(kind = "cmp", op = ">=", left = x, right = left),
                right = list(kind = "cmp", op = "<=", left = x, right = right)))
  }

  # resolve(fk_col, pk_col, value_col) — FK lookup within same table
  if (fn == "resolve") {
    if (length(expr) != 4)
      stop("resolve() requires exactly 3 arguments: fk_col, pk_col, value_col")
    return(list(kind = "resolve",
                fk = serialize_expr(expr[[2]], env, cols),
                pk = serialize_expr(expr[[3]], env, cols),
                val = serialize_expr(expr[[4]], env, cols)))
  }

  # propagate(parent_fk, pk_col, seed_expr) — iterative tree propagation
  if (fn == "propagate") {
    if (length(expr) != 4)
      stop("propagate() requires exactly 3 arguments: parent_fk, pk_col, seed_expr")
    return(list(kind = "propagate",
                parent_fk = serialize_expr(expr[[2]], env, cols),
                pk = serialize_expr(expr[[3]], env, cols),
                seed = serialize_expr(expr[[4]], env, cols)))
  }

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
