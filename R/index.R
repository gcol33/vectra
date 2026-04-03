#' Create a hash index on a .vtr file column
#'
#' Builds a persistent hash index stored as a `.vtri` sidecar file alongside
#' the `.vtr` file. The index maps key hashes to row group indices, enabling
#' O(1) row group identification for equality predicates (`filter(col == value)`).
#'
#' For composite indexes on multiple columns, pass a character vector.
#' Composite indexes accelerate AND-combined equality predicates
#' (e.g., `filter(col1 == "a", col2 == "b")`).
#'
#' The index is automatically loaded by [tbl()] when present. It composes with
#' zone-map pruning and binary search on sorted columns.
#'
#' @param path Path to a `.vtr` file.
#' @param column Character vector. Name(s) of column(s) to index.
#' @param ci Logical. Build a case-insensitive index? Default `FALSE`.
#'
#' @return Invisible `NULL`. The index is written as a `.vtri` sidecar file.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE), f)
#' create_index(f, "id")
#' tbl(f) |> filter(id == "m") |> collect()
#' unlink(c(f, paste0(f, ".id.vtri")))
#'
#' @export
create_index <- function(path, column, ci = FALSE) {
  check_scalar_string(path)
  if (!is.character(column) || length(column) < 1)
    stop("column must be a character vector of length >= 1")
  path <- normalizePath(path, mustWork = TRUE)
  .Call(C_create_index, path, column, as.logical(ci))
  invisible(NULL)
}

#' Check if a hash index exists for a .vtr column
#'
#' @param path Path to a `.vtr` file.
#' @param column Character vector. Name(s) of column(s).
#'
#' @return Logical scalar: `TRUE` if a `.vtri` index file exists.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE), f)
#' has_index(f, "id")   # FALSE
#' create_index(f, "id")
#' has_index(f, "id")   # TRUE
#' unlink(c(f, paste0(f, ".id.vtri")))
#'
#' @export
has_index <- function(path, column) {
  check_scalar_string(path)
  if (!is.character(column) || length(column) < 1)
    stop("column must be a character vector of length >= 1")
  path <- normalizePath(path, mustWork = TRUE)
  .Call(C_has_index, path, column)
}
