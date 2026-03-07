#' Write query results or a data.frame to a CSV file
#'
#' For `vectra_node` inputs, data is streamed batch-by-batch to disk without
#' materializing the full result in memory. For `data.frame` inputs, the data
#' is written directly.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the output CSV file.
#' @param ... Reserved for future use.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars[1:5, ], f)
#' csv <- tempfile(fileext = ".csv")
#' tbl(f) |> write_csv(csv)
#' unlink(c(f, csv))
#'
#' @export
write_csv <- function(x, path, ...) {
  UseMethod("write_csv")
}

#' @export
write_csv.vectra_node <- function(x, path, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_csv, x$.node, path)
  invisible(NULL)
}

#' @export
write_csv.data.frame <- function(x, path, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  tmp <- tempfile(fileext = ".vtr")
  on.exit(unlink(tmp))
  write_vtr(x, tmp)
  node <- tbl(tmp)
  .Call(C_write_csv, node$.node, path)
  invisible(NULL)
}

#' Write query results or a data.frame to a SQLite table
#'
#' For `vectra_node` inputs, data is streamed batch-by-batch to disk without
#' materializing the full result in memory. For `data.frame` inputs, the data
#' is written directly.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the SQLite database.
#' @param table Name of the table to create/write into.
#' @param ... Reserved for future use.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' db <- tempfile(fileext = ".sqlite")
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars[1:5, ], f)
#' tbl(f) |> write_sqlite(db, "cars")
#' unlink(c(f, db))
#'
#' @export
write_sqlite <- function(x, path, table, ...) {
  UseMethod("write_sqlite")
}

#' @export
write_sqlite.vectra_node <- function(x, path, table, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!is.character(table) || length(table) != 1)
    stop("table must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_sqlite, x$.node, path, table)
  invisible(NULL)
}

#' @export
write_sqlite.data.frame <- function(x, path, table, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!is.character(table) || length(table) != 1)
    stop("table must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  tmp <- tempfile(fileext = ".vtr")
  on.exit(unlink(tmp))
  write_vtr(x, tmp)
  node <- tbl(tmp)
  .Call(C_write_sqlite, node$.node, path, table)
  invisible(NULL)
}

#' Write query results to a GeoTIFF file
#'
#' The data must contain `x` and `y` columns (pixel center coordinates) and
#' one or more numeric band columns. Grid dimensions and geotransform are
#' inferred from the x/y coordinate arrays. Missing pixels are written as NaN.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the output GeoTIFF file.
#' @param compress Logical; use DEFLATE compression? Default `FALSE`.
#' @param ... Reserved for future use.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' \dontrun{
#' tbl_tiff("climate.tif") |>
#'   filter(band1 > 25) |>
#'   write_tiff("filtered.tif")
#' }
#'
#' @export
write_tiff <- function(x, path, compress = FALSE, ...) {
  UseMethod("write_tiff")
}

#' @export
write_tiff.vectra_node <- function(x, path, compress = FALSE, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_tiff, x$.node, path, as.logical(compress))
  invisible(NULL)
}

#' @export
write_tiff.data.frame <- function(x, path, compress = FALSE, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  tmp <- tempfile(fileext = ".vtr")
  on.exit(unlink(tmp))
  write_vtr(x, tmp)
  node <- tbl(tmp)
  .Call(C_write_tiff, node$.node, path, as.logical(compress))
  invisible(NULL)
}

#' Write data to a .vtr file
#'
#' For `vectra_node` inputs (lazy queries from any format: CSV, SQLite, TIFF,
#' or another .vtr), data is streamed batch-by-batch to disk without
#' materializing the full result in memory. Each batch becomes one row group.
#' The output file is written atomically (via temp file + rename) so readers
#' never see a partial file.
#'
#' For `data.frame` inputs, the data is written directly from memory.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the output .vtr file.
#' @param ... Additional arguments passed to methods.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' # From a data.frame
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#'
#' # Streaming format conversion (CSV -> VTR)
#' csv <- tempfile(fileext = ".csv")
#' write.csv(mtcars, csv, row.names = FALSE)
#' f2 <- tempfile(fileext = ".vtr")
#' tbl_csv(csv) |> write_vtr(f2)
#'
#' unlink(c(f, f2, csv))
#'
#' @export
write_vtr <- function(x, path, ...) {
  UseMethod("write_vtr")
}

#' @export
write_vtr.vectra_node <- function(x, path, ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_vtr_node, x$.node, path)
  invisible(NULL)
}

#' @export
write_vtr.data.frame <- function(x, path, batch_size = nrow(x), ...) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_vtr, x, path, as.integer(batch_size))
  invisible(NULL)
}
