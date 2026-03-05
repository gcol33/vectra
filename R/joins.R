# Join key parsing: handles by = "col", by = c("a" = "b"), or NULL (natural)
parse_join_keys <- function(x, y, by) {
  x_schema <- .Call(C_node_schema, x$.node)
  y_schema <- .Call(C_node_schema, y$.node)

  if (is.null(by)) {
    # Natural join: common column names
    common <- intersect(x_schema$name, y_schema$name)
    if (length(common) == 0)
      stop("no common columns found for natural join; specify 'by'")
    message(sprintf("Joining by: %s", paste(common, collapse = ", ")))
    return(list(left = common, right = common))
  }

  if (is.null(names(by))) {
    # by = c("a", "b") -> same names on both sides
    for (nm in by) {
      if (!nm %in% x_schema$name) stop(sprintf("column '%s' not found in x", nm))
      if (!nm %in% y_schema$name) stop(sprintf("column '%s' not found in y", nm))
    }
    return(list(left = by, right = by))
  }

  # by = c("a" = "b") -> left_key = "a", right_key = "b"
  left_keys <- names(by)
  right_keys <- unname(by)
  # Handle unnamed entries (same name on both sides)
  for (i in seq_along(left_keys)) {
    if (left_keys[i] == "") left_keys[i] <- right_keys[i]
  }
  for (nm in left_keys) {
    if (!nm %in% x_schema$name) stop(sprintf("column '%s' not found in x", nm))
  }
  for (nm in right_keys) {
    if (!nm %in% y_schema$name) stop(sprintf("column '%s' not found in y", nm))
  }
  list(left = left_keys, right = right_keys)
}

#' Join two vectra tables
#'
#' @param x A `vectra_node` object (left table).
#' @param y A `vectra_node` object (right table).
#' @param by A character vector of column names to join by, or a named vector
#'   like `c("a" = "b")`. `NULL` for natural join (common columns).
#' @param suffix A character vector of length 2 for disambiguating non-key
#'   columns with the same name (default `c(".x", ".y")`).
#' @param ... Ignored.
#'
#' @return A `vectra_node` with the joined result.
#'
#' @examples
#' f1 <- tempfile(fileext = ".vtr")
#' f2 <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
#' write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
#' left_join(tbl(f1), tbl(f2), by = "id") |> collect()
#' unlink(c(f1, f2))
#'
#' @export
left_join <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  UseMethod("left_join")
}

#' @export
left_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .Call(C_join_node, x$.node, y$.node,
                    "left", keys$left, keys$right, suffix[1], suffix[2])
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}

#' @rdname left_join
#' @export
inner_join <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  UseMethod("inner_join")
}

#' @export
inner_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .Call(C_join_node, x$.node, y$.node,
                    "inner", keys$left, keys$right, suffix[1], suffix[2])
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}

#' @rdname left_join
#' @export
right_join <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  UseMethod("right_join")
}

#' @export
right_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  # right_join(x, y) = left_join(y, x) with swapped keys and reordered columns
  keys <- parse_join_keys(x, y, by)

  # Get schemas before C_join_node clears the external pointers
  x_schema <- .Call(C_node_schema, x$.node)
  y_schema <- .Call(C_node_schema, y$.node)

  # Swap: build on left (x), probe with right (y)
  new_xptr <- .Call(C_join_node, y$.node, x$.node,
                    "left", keys$right, keys$left, suffix[2], suffix[1])
  result_node <- structure(list(.node = new_xptr, .path = NULL),
                           class = "vectra_node")
  schema <- .Call(C_node_schema, result_node$.node)

  # Current output: y_cols + x_non_key_cols
  # Desired output: x_cols + y_non_key_cols (with key values from y for matched)
  # This is complex to reorder at the node level. Use a project node.
  cur_names <- schema$name

  # x columns come after y columns in current output
  # Build the desired column order
  x_names <- x_schema$name
  y_names <- y_schema$name
  y_key_set <- keys$right

  # Desired: all x columns, then y non-key columns
  desired <- character(0)
  expr_lists <- list()

  # x key columns: use values from y side (key col in current output)
  for (xn in x_names) {
    ki <- match(xn, keys$left)
    if (!is.na(ki)) {
      # This x key maps to y key: find the y key col name in current output
      yk <- keys$right[ki]
      desired <- c(desired, xn)
      if (xn != yk) {
        expr_lists <- c(expr_lists, list(list(kind = "col_ref", name = yk)))
      } else {
        expr_lists <- c(expr_lists, list(NULL))
      }
    } else {
      # Non-key x column: exists in current output (possibly suffixed)
      match_name <- xn
      if (!match_name %in% cur_names) {
        # Try with suffix
        match_name <- paste0(xn, suffix[1])
      }
      desired <- c(desired, xn)
      if (match_name != xn) {
        expr_lists <- c(expr_lists, list(list(kind = "col_ref", name = match_name)))
      } else {
        expr_lists <- c(expr_lists, list(NULL))
      }
    }
  }
  # y non-key columns
  for (yn in y_names) {
    if (yn %in% y_key_set) next
    match_name <- yn
    if (!match_name %in% cur_names) {
      match_name <- paste0(yn, suffix[2])
    }
    desired <- c(desired, yn)
    if (match_name != yn) {
      expr_lists <- c(expr_lists, list(list(kind = "col_ref", name = match_name)))
    } else {
      expr_lists <- c(expr_lists, list(NULL))
    }
  }

  new_xptr2 <- .Call(C_project_node, result_node$.node, desired, expr_lists)
  structure(list(.node = new_xptr2, .path = NULL), class = "vectra_node")
}

#' @rdname left_join
#' @export
full_join <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  UseMethod("full_join")
}

#' @export
full_join.vectra_node <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...) {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .Call(C_join_node, x$.node, y$.node,
                    "full", keys$left, keys$right, suffix[1], suffix[2])
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}

#' @rdname left_join
#' @export
semi_join <- function(x, y, by = NULL, ...) {
  UseMethod("semi_join")
}

#' @export
semi_join.vectra_node <- function(x, y, by = NULL, ...) {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .Call(C_join_node, x$.node, y$.node,
                    "semi", keys$left, keys$right, ".x", ".y")
  structure(list(.node = new_xptr, .path = x$.path), class = "vectra_node")
}

#' @rdname left_join
#' @export
anti_join <- function(x, y, by = NULL, ...) {
  UseMethod("anti_join")
}

#' @export
anti_join.vectra_node <- function(x, y, by = NULL, ...) {
  keys <- parse_join_keys(x, y, by)
  new_xptr <- .Call(C_join_node, x$.node, y$.node,
                    "anti", keys$left, keys$right, ".x", ".y")
  structure(list(.node = new_xptr, .path = x$.path), class = "vectra_node")
}
