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
    call_args <- as.list(expr)[-1]
    nms <- names(call_args)
    if (!is.null(nms) && any(nms == "na.rm"))
      stop(sprintf("%s(na.rm=) is not supported", fn))
    data_args <- if (is.null(nms)) call_args else call_args[!nzchar(nms)]
    if (length(data_args) < 2)
      stop(sprintf("%s() needs at least two arguments", fn))
    # Left-fold N arguments into nested binary pmin/pmax.
    acc <- serialize_expr(data_args[[1]], env, cols)
    for (k in 2:length(data_args)) {
      acc <- list(kind = fn, left = acc,
                  right = serialize_expr(data_args[[k]], env, cols))
    }
    return(acc)
  }

  # round(x, digits): the engine has only single-argument round, so express
  # round(x, n) as round(x * 10^n) / 10^n via existing primitives.
  if (fn == "round" && length(expr) >= 3) {
    sx <- serialize_expr(expr[[2]], env, cols)
    digits_expr <- expr[[3]]
    dv <- tryCatch(eval(digits_expr, env), error = function(e) NULL)
    if (!is.null(dv) && is.numeric(dv) && length(dv) == 1L && !is.na(dv)) {
      factor_node <- list(kind = "lit_double", value = 10^dv)
    } else {
      # 10^d = exp(d * log(10)) for a non-constant number of digits
      factor_node <- list(kind = "math_unary", fn = "e",
        operand = list(kind = "arith", op = "*",
          left = serialize_expr(digits_expr, env, cols),
          right = list(kind = "lit_double", value = log(10))))
    }
    scaled  <- list(kind = "arith", op = "*", left = sx, right = factor_node)
    rounded <- list(kind = "math_unary", fn = "r", operand = scaled)
    return(list(kind = "arith", op = "/", left = rounded, right = factor_node))
  }

  # log(x, base): express as log(x) / log(base).
  if (fn == "log" && length(expr) >= 3) {
    return(list(kind = "arith", op = "/",
      left  = list(kind = "math_unary", fn = "l",
                   operand = serialize_expr(expr[[2]], env, cols)),
      right = list(kind = "math_unary", fn = "l",
                   operand = serialize_expr(expr[[3]], env, cols))))
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
    fixed <- FALSE  # base R default: pattern is a regex unless fixed = TRUE
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
      if (!is.na(ci))
        stop("paste(collapse=) is not supported: paste is row-wise here, ",
             "so it cannot reduce a column to a single string")
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
    fixed <- FALSE  # base R default: pattern is a regex unless fixed = TRUE
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
  sc <- serialize_expr(expr[[2]], env, cols)
  sy <- serialize_expr(expr[[3]], env, cols)
  sn <- serialize_expr(expr[[4]], env, cols)

  # dplyr's if_else() has a 4th argument, `missing`, used where the condition
  # is NA. Express it via case_when so NA conditions take the missing value.
  if (fn == "if_else") {
    args <- as.list(expr)[-1]
    an <- names(args)
    missing_expr <- NULL
    if (!is.null(an) && !is.na(match("missing", an))) {
      missing_expr <- args[[match("missing", an)]]
    } else if (length(args) >= 4L && (is.null(an) || an[4] == "")) {
      missing_expr <- args[[4]]
    }
    if (!is.null(missing_expr)) {
      return(list(kind = "case_when",
        cases = list(
          list(cond = list(kind = "is_na", operand = sc),
               val  = serialize_expr(missing_expr, env, cols)),
          list(cond = sc, val = sy)),
        default = sn))
    }
  }

  list(kind = "if_else", cond = sc, then_expr = sy, else_expr = sn)
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

# Geometry functions: libgeos ops over a hex-WKB geometry column, run inside the
# C engine off the column with no per-batch sf round-trip. Each name maps to a
# single-character op code the C side dispatches on (see src/expr_geom.c).
#   unary      geom -> double / bool / string / geometry
#   parameter  geom, numeric -> geometry         (buffer, simplify)
#   binary     geom, geom    -> double / bool     (distance, predicates)
.GEOM_UNARY <- c(
  st_area = "A", st_length = "L", st_perimeter = "L",
  st_x = "X", st_y = "Y", st_npoints = "n", st_ngeometries = "g",
  st_is_valid = "v", st_is_empty = "m", st_is_simple = "s",
  st_geometry_type = "t",
  st_centroid = "c", st_point_on_surface = "o", st_boundary = "b",
  st_envelope = "e", st_convex_hull = "h", st_make_valid = "M")
.GEOM_PARAM <- c(st_buffer = "B", st_simplify = "S")
.GEOM_BINARY <- c(
  st_distance = "D", st_intersects = "i", st_within = "w", st_contains = "C",
  st_overlaps = "O", st_touches = "T", st_crosses = "R", st_equals = "Q",
  st_disjoint = "J", st_covers = "K", st_covered_by = "V")

# Resolve a binary op's second geometry: a geometry column reference, a constant
# sf/sfc object (converted once to hex-WKB), or a hex-WKB string literal.
.serialize_geom_arg <- function(a, env, cols) {
  if (is.name(a) && !is.null(cols) && as.character(a) %in% cols)
    return(serialize_expr(a, env, cols))
  val <- tryCatch(eval(a, env), error = function(e) NULL)
  if (inherits(val, "sf") || inherits(val, "sfc")) {
    if (!requireNamespace("sf", quietly = TRUE))
      stop("a constant sf/sfc geometry argument needs the sf package installed")
    g <- sf::st_geometry(val)
    if (length(g) > 1) g <- sf::st_union(g)
    hex <- sf::st_as_binary(g, hex = TRUE)
    return(list(kind = "lit_string", value = as.character(hex)[1]))
  }
  if (is.character(val) && length(val) == 1)
    return(list(kind = "lit_string", value = val))
  stop("second geometry argument must be a geometry column, an sf/sfc object, ",
       "or a hex-WKB string")
}

.serialize_geom <- function(fn, expr, env, cols) {
  if (fn %in% names(.GEOM_UNARY)) {
    return(list(kind = "geom", fn = unname(.GEOM_UNARY[fn]),
                operand = serialize_expr(expr[[2]], env, cols)))
  }
  if (fn %in% names(.GEOM_PARAM)) {
    return(list(kind = "geom", fn = unname(.GEOM_PARAM[fn]),
                operand = serialize_expr(expr[[2]], env, cols),
                param = serialize_expr(expr[[3]], env, cols)))
  }
  # binary: distance / topological predicate
  list(kind = "geom", fn = unname(.GEOM_BINARY[fn]),
       operand = serialize_expr(expr[[2]], env, cols),
       other = .serialize_geom_arg(expr[[3]], env, cols))
}

# Embedding distance ops over a hex float32 embedding column (see expr_vec.c).
# cosine and l2 are distances (smaller = nearer); dot is an inner product.
.VEC_DIST <- c(cosine = "c", l2 = "e", dot = "d")

# Resolve the query side of a distance op: an embedding column reference, or a
# constant numeric query vector (encoded once to a hex float32 blob), or a
# pre-encoded hex embedding string.
.serialize_vec_query <- function(a, env, cols) {
  if (is.name(a) && !is.null(cols) && as.character(a) %in% cols)
    return(serialize_expr(a, env, cols))
  val <- tryCatch(eval(a, env), error = function(e) NULL)
  if (is.numeric(val))
    return(list(kind = "lit_string", value = .embedding_hex(as.double(val))))
  if (is.character(val) && length(val) == 1)
    return(list(kind = "lit_string", value = val))
  stop("the query argument to a distance op must be an embedding column or a ",
       "numeric query vector")
}

.serialize_vecdist <- function(fn, expr, env, cols) {
  list(kind = "vec_dist", fn = unname(.VEC_DIST[fn]),
       operand = serialize_expr(expr[[2]], env, cols),
       query = .serialize_vec_query(expr[[3]], env, cols))
}

# Biological-sequence ops over an ASCII sequence string column (see
# src/expr_seq.c). Each name maps to a single-character op code the C side
# dispatches on.
#   measures     seq -> int64 / double        seq_length, seq_gc
#   transforms   seq -> seq                    seq_revcomp, seq_complement,
#                                              seq_reverse, seq_transcribe
#   translate    seq (+ table) -> protein      seq_translate
#   subseq       seq, start, width -> seq      seq_subseq
#   distance     seq, ref (+ method) -> int64  seq_dist
.SEQ_UNARY <- c(
  seq_length = "L", seq_gc = "G",
  seq_revcomp = "r", seq_complement = "c", seq_reverse = "v",
  seq_transcribe = "t")

# seq_dist method name -> single-char code.
.SEQ_DIST_METHOD <- c(
  levenshtein = "l", lv = "l", edit = "l",
  dl = "d", damerau = "d", osa = "d",
  hamming = "h")

# Resolve the second sequence of seq_dist: a sequence column reference, or a
# constant character scalar (compared against every row).
.serialize_seq_ref <- function(a, env, cols) {
  if (is.name(a) && !is.null(cols) && as.character(a) %in% cols)
    return(serialize_expr(a, env, cols))
  val <- tryCatch(eval(a, env), error = function(e) NULL)
  if (is.character(val) && length(val) == 1)
    return(list(kind = "lit_string", value = val))
  stop("the reference argument to seq_dist() must be a sequence column or a ",
       "single character string")
}

.serialize_seq <- function(fn, expr, env, cols) {
  if (fn %in% names(.SEQ_UNARY)) {
    return(list(kind = "seq", fn = unname(.SEQ_UNARY[fn]),
                operand = serialize_expr(expr[[2]], env, cols)))
  }

  if (fn == "seq_translate") {
    args <- as.list(expr)[-1]
    nms <- names(args)
    table <- 1L
    if (length(args) >= 2) {
      ta <- if (!is.null(nms) && "table" %in% nms) args[["table"]] else args[[2]]
      table <- as.integer(eval(ta, env))
    }
    if (!identical(table, 1L))
      stop("seq_translate() currently supports only the standard genetic ",
           "code (table = 1)")
    return(list(kind = "seq", fn = "p",
                operand = serialize_expr(args[[1]], env, cols),
                table = table))
  }

  if (fn == "seq_subseq") {
    args <- as.list(expr)[-1]
    nms <- names(args)
    seq_a  <- if (!is.null(nms) && "x" %in% nms) args[["x"]] else args[[1]]
    start_a <- if (!is.null(nms) && "start" %in% nms) args[["start"]] else args[[2]]
    width_a <- if (!is.null(nms) && "width" %in% nms) args[["width"]] else args[[3]]
    return(list(kind = "seq", fn = "s",
                operand = serialize_expr(seq_a, env, cols),
                start = serialize_expr(start_a, env, cols),
                width = serialize_expr(width_a, env, cols)))
  }

  # seq_dist(x, ref, method = "levenshtein")
  args <- as.list(expr)[-1]
  nms <- names(args)
  method <- "levenshtein"
  if (!is.null(nms) && "method" %in% nms) {
    method <- as.character(eval(args[["method"]], env))
    args <- args[setdiff(seq_along(args), match("method", nms))]
  } else if (length(args) >= 3) {
    method <- as.character(eval(args[[3]], env))
    args <- args[1:2]
  }
  code <- .SEQ_DIST_METHOD[[tolower(method)]]
  if (is.null(code)) stop(sprintf("unknown seq_dist method '%s'", method))
  list(kind = "seq", fn = "d",
       operand = serialize_expr(args[[1]], env, cols),
       ref = .serialize_seq_ref(args[[2]], env, cols),
       method = code)
}

# floor_time(t, unit): truncate a Date/POSIXct column to a calendar grid.
# unit is a string like "hour", "15 min", "day", "3 months".
.FLOOR_UNITS <- c(
  sec = "s", second = "s", seconds = "s", secs = "s",
  min = "n", minute = "n", minutes = "n", mins = "n",
  hour = "h", hours = "h",
  day = "d", days = "d",
  week = "w", weeks = "w",
  month = "M", months = "M",
  quarter = "q", quarters = "q",
  year = "y", years = "y")

.parse_time_unit <- function(u) {
  u <- trimws(tolower(u))
  m <- regmatches(u, regexec("^([0-9]*)\\s*([a-z]+)$", u))[[1]]
  if (length(m) != 3L) stop(sprintf("cannot parse time unit '%s'", u))
  n <- if (nzchar(m[2])) as.integer(m[2]) else 1L
  code <- .FLOOR_UNITS[[m[3]]]
  if (is.null(code)) stop(sprintf("unknown time unit '%s'", m[3]))
  list(n = n, code = code)
}

.serialize_floortime <- function(fn, expr, env, cols) {
  u <- eval(expr[[3]], env)
  if (!is.character(u) || length(u) != 1L)
    stop("floor_time() unit must be a single string, e.g. \"1 hour\"")
  pu <- .parse_time_unit(u)
  list(kind = "floor_time", unit = pu$code, n = pu$n,
       operand = serialize_expr(expr[[2]], env, cols))
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
  register(c(names(.GEOM_UNARY), names(.GEOM_PARAM),
             names(.GEOM_BINARY)),                       .serialize_geom)
  register(c("cosine", "l2", "dot"),                     .serialize_vecdist)
  register(c(names(.SEQ_UNARY), "seq_translate",
             "seq_subseq", "seq_dist"),                   .serialize_seq)
  register(c("floor_time"),                              .serialize_floortime)
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
