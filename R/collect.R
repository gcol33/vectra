#' Execute a lazy query and return a data.frame
#'
#' Pulls all batches from the execution plan and materializes the result as an
#' R data.frame.
#'
#' @param x A `vectra_node` object.
#' @param ... Ignored.
#'
#' @return A data.frame with the query results.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' result <- tbl(f) |> collect()
#' head(result)
#' unlink(f)
#'
#' @export
collect <- function(x, ...) {
  UseMethod("collect")
}

#' @export
collect.vectra_node <- function(x, ...) {
  .Call(C_collect, x$.node)
}
