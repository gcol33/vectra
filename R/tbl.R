#' Create a lazy table reference from a .vtr file
#'
#' Opens a vectra1 file and returns a lazy query node. No data is read until
#' [collect()] is called.
#'
#' @param path Path to a `.vtr` file.
#'
#' @return A `vectra_node` object representing a lazy scan of the file.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' node <- tbl(f)
#' print(node)
#' unlink(f)
#'
#' @export
tbl <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_scan_node, path)
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a CSV file
#'
#' Opens a CSV file for lazy, streaming query execution. Column types are
#' inferred from the first 1000 rows. No data is read until [collect()] is
#' called.
#'
#' @param path Path to a `.csv` file.
#' @param batch_size Number of rows per batch (default 65536).
#'
#' @return A `vectra_node` object representing a lazy scan of the CSV file.
#'
#' @examples
#' f <- tempfile(fileext = ".csv")
#' write.csv(mtcars, f, row.names = FALSE)
#' node <- tbl_csv(f)
#' print(node)
#' unlink(f)
#'
#' @export
tbl_csv <- function(path, batch_size = 65536L) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_csv_scan_node, path, as.double(batch_size))
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}
