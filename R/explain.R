#' Print the execution plan for a vectra query
#'
#' Shows the node types, column schemas, and structure of the lazy query plan.
#'
#' @param x A `vectra_node` object.
#' @param ... Ignored.
#'
#' @return Invisible `x`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> filter(cyl > 4) |> select(mpg, cyl) |> explain()
#' unlink(f)
#'
#' @export
explain <- function(x, ...) {
  UseMethod("explain")
}

#' @export
explain.vectra_node <- function(x, ...) {
  plan_lines <- .Call(C_node_plan, x$.node)
  schema <- .Call(C_node_schema, x$.node)

  cat("vectra execution plan\n")
  if (!is.null(x$.groups)) {
    cat(sprintf("Groups: %s\n", paste(x$.groups, collapse = ", ")))
  }
  cat("\n")
  for (line in plan_lines) cat(line, "\n")
  cat("\n")
  cat(sprintf("Output columns (%d):\n", length(schema$name)))
  for (i in seq_along(schema$name)) {
    cat(sprintf("  %s <%s>\n", schema$name[i], schema$type[i]))
  }
  invisible(x)
}
