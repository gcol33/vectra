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

#' Write a data.frame to a .vtr file
#'
#' Serializes an R data.frame into the vectra1 on-disk format.
#'
#' @param df A data.frame to write. Supported column types: integer, double,
#'   logical, character, and bit64::integer64.
#' @param path File path for the output .vtr file.
#' @param batch_size Number of rows per row group. Defaults to all rows in a
#'   single row group.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' unlink(f)
#'
#' @export
write_vtr <- function(df, path, batch_size = nrow(df)) {
  if (!is.data.frame(df))
    stop(sprintf("df must be a data.frame, got %s", class(df)[1]))
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_vtr, df, path, as.integer(batch_size))
  invisible(NULL)
}
