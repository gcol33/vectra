# NSE expression capture -> serialized list for C bridge

serialize_expr <- function(expr, env = parent.frame()) {
  if (is.name(expr)) {
    name <- as.character(expr)
    # Check if it's a known R constant
    if (name == "TRUE") return(list(kind = "lit_logical", value = TRUE))
    if (name == "FALSE") return(list(kind = "lit_logical", value = FALSE))
    if (name == "NA") return(list(kind = "lit_na"))
    if (name == "NA_real_") return(list(kind = "lit_na"))
    if (name == "NA_integer_") return(list(kind = "lit_na"))
    if (name == "NA_character_") return(list(kind = "lit_na"))
    # Otherwise it's a column reference
    return(list(kind = "col_ref", name = name))
  }

  if (is.numeric(expr) || is.logical(expr) || is.character(expr)) {
    val <- expr
    if (is.logical(val)) return(list(kind = "lit_logical", value = val))
    if (is.integer(val)) return(list(kind = "lit_integer", value = val))
    if (is.double(val)) return(list(kind = "lit_double", value = val))
    if (is.character(val)) return(list(kind = "lit_string", value = val))
  }

  if (!is.call(expr))
    stop(sprintf("unsupported expression type: %s", typeof(expr)))

  fn <- as.character(expr[[1]])

  # Arithmetic operators
  if (fn %in% c("+", "-", "*", "/", "%%")) {
    if (length(expr) == 2 && fn == "-") {
      # Unary minus
      return(list(kind = "negate",
                  operand = serialize_expr(expr[[2]], env)))
    }
    op <- if (fn == "%%") "%" else fn
    return(list(kind = "arith", op = op,
                left = serialize_expr(expr[[2]], env),
                right = serialize_expr(expr[[3]], env)))
  }

  # Comparison operators
  if (fn %in% c("==", "!=", "<", "<=", ">", ">=")) {
    return(list(kind = "cmp", op = fn,
                left = serialize_expr(expr[[2]], env),
                right = serialize_expr(expr[[3]], env)))
  }

  # Boolean operators
  if (fn == "&" || fn == "&&") {
    return(list(kind = "bool", op = "&",
                left = serialize_expr(expr[[2]], env),
                right = serialize_expr(expr[[3]], env)))
  }
  if (fn == "|" || fn == "||") {
    return(list(kind = "bool", op = "|",
                left = serialize_expr(expr[[2]], env),
                right = serialize_expr(expr[[3]], env)))
  }
  if (fn == "!") {
    return(list(kind = "bool", op = "!",
                operand = serialize_expr(expr[[2]], env)))
  }

  # is.na
  if (fn == "is.na") {
    return(list(kind = "is_na",
                operand = serialize_expr(expr[[2]], env)))
  }

  # Parentheses
  if (fn == "(") {
    return(serialize_expr(expr[[2]], env))
  }

  # String functions
  if (fn == "nchar") {
    return(list(kind = "nchar",
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "substr" || fn == "substring") {
    return(list(kind = "substr",
                operand = serialize_expr(expr[[2]], env),
                start = serialize_expr(expr[[3]], env),
                stop = serialize_expr(expr[[4]], env)))
  }
  if (fn == "grepl") {
    pattern <- expr[[2]]
    x <- expr[[3]]
    # grepl(pattern, x) — pattern must be a literal string
    if (!is.character(pattern))
      stop("grepl: pattern must be a string literal")
    return(list(kind = "grepl",
                pattern = as.character(pattern),
                operand = serialize_expr(x, env)))
  }

  # Math functions
  if (fn %in% c("abs", "sqrt", "log", "exp", "floor", "ceiling", "round")) {
    fn_char <- switch(fn, abs = "a", sqrt = "s", log = "l", exp = "e",
                      floor = "f", ceiling = "c", round = "r")
    return(list(kind = "math_unary", fn = fn_char,
                operand = serialize_expr(expr[[2]], env)))
  }

  # if_else(cond, true, false)
  if (fn == "if_else" || fn == "ifelse") {
    return(list(kind = "if_else",
                cond = serialize_expr(expr[[2]], env),
                then_expr = serialize_expr(expr[[3]], env),
                else_expr = serialize_expr(expr[[4]], env)))
  }

  # Type casting
  if (fn %in% c("as.numeric", "as.double")) {
    return(list(kind = "cast", to = "double",
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "as.integer") {
    return(list(kind = "cast", to = "int64",
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "as.character") {
    return(list(kind = "cast", to = "string",
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "as.logical") {
    return(list(kind = "cast", to = "bool",
                operand = serialize_expr(expr[[2]], env)))
  }

  # String functions
  if (fn == "tolower") {
    return(list(kind = "tolower",
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "toupper") {
    return(list(kind = "toupper",
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "trimws") {
    return(list(kind = "trimws",
                operand = serialize_expr(expr[[2]], env)))
  }

  # Additional math functions
  if (fn %in% c("log2", "log10", "sign", "trunc")) {
    fn_char <- switch(fn, log2 = "2", log10 = "t", sign = "g", trunc = "u")
    return(list(kind = "math_unary", fn = fn_char,
                operand = serialize_expr(expr[[2]], env)))
  }

  # paste0(a, b) — two-argument string concatenation
  if (fn == "paste0") {
    if (length(expr) != 3)
      stop("paste0 in vectra supports exactly 2 arguments")
    return(list(kind = "paste0",
                left = serialize_expr(expr[[2]], env),
                right = serialize_expr(expr[[3]], env)))
  }

  # startsWith / endsWith
  if (fn == "startsWith") {
    prefix <- expr[[3]]
    if (!is.character(prefix)) stop("startsWith: prefix must be a string literal")
    return(list(kind = "startsWith", prefix = as.character(prefix),
                operand = serialize_expr(expr[[2]], env)))
  }
  if (fn == "endsWith") {
    suffix <- expr[[3]]
    if (!is.character(suffix)) stop("endsWith: suffix must be a string literal")
    return(list(kind = "endsWith", suffix = as.character(suffix),
                operand = serialize_expr(expr[[2]], env)))
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
                operand = serialize_expr(x, env)))
  }

  # pmin / pmax
  if (fn == "pmin" || fn == "pmax") {
    return(list(kind = fn,
                left = serialize_expr(expr[[2]], env),
                right = serialize_expr(expr[[3]], env)))
  }

  # %in% operator
  if (fn == "%in%") {
    set_val <- eval(expr[[3]], env)
    return(list(kind = "in",
                operand = serialize_expr(expr[[2]], env),
                set = set_val))
  }

  # between(x, left, right) -> x >= left & x <= right
  if (fn == "between") {
    x <- serialize_expr(expr[[2]], env)
    left <- serialize_expr(expr[[3]], env)
    right <- serialize_expr(expr[[4]], env)
    return(list(kind = "bool", op = "&",
                left = list(kind = "cmp", op = ">=", left = x, right = left),
                right = list(kind = "cmp", op = "<=", left = x, right = right)))
  }

  stop(sprintf("unsupported function in expression: %s", fn))
}

# Combine multiple filter expressions with &
combine_predicates <- function(exprs, env) {
  if (length(exprs) == 0) stop("no filter expressions provided")
  result <- serialize_expr(exprs[[1]], env)
  for (i in seq_along(exprs)[-1]) {
    result <- list(kind = "bool", op = "&",
                   left = result,
                   right = serialize_expr(exprs[[i]], env))
  }
  result
}
