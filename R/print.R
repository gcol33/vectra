#' Print a vectra query node
#'
#' @param x A `vectra_node` object.
#' @param ... Ignored.
#'
#' @return Invisible `x`.
#'
#' @export
print.vectra_node <- function(x, ...) {
  schema <- .Call(C_node_schema, x$.node)
  cat("vectra query node\n")
  cat(sprintf("Columns (%d):\n", length(schema$name)))
  for (i in seq_along(schema$name)) {
    cat(sprintf("  %s <%s>\n", schema$name[i], schema$type[i]))
  }
  invisible(x)
}
