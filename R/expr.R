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
