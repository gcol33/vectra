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
