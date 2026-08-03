#' Create a hash index on a .vtr file column
#'
#' Builds a persistent hash index stored as a `.vtri` sidecar file alongside
#' the `.vtr` file. The index maps key hashes to row group indices, so an
#' equality predicate (`filter(col == value)`) names the row groups that may
#' hold a key without reading any column data.
#'
#' For composite indexes on multiple columns, pass a character vector.
#' Composite indexes accelerate AND-combined equality predicates
#' (e.g., `filter(col1 == "a", col2 == "b")`). The columns may be named in any
#' order.
#'
#' A query opens the index for the column it filters on, so a store can carry
#' an index on each of several columns and a query pays only for the one it
#' uses. `explain()` reports the index a scan will probe. The index composes
#' with zone-map pruning and binary search on sorted columns.
#'
#' The index holds one entry per distinct key per row group rather than one per
#' row, so an index over a column with few distinct values stays small however
#' many rows the store holds -- which is what keeps a lookup off the size of the
#' store.
#'
#' [append_vtr()] leaves the existing row groups where they are, so an index
#' stays valid across an append: it takes in the row groups just appended and
#' keeps the rest, reading only the new data rather than the whole store.
#'
#' An index left behind by any other change of the store is reported as absent
#' by `has_index()` and ignored by queries rather than pruning row groups that
#' may now hold matching rows. The same goes for one that cannot be read at all:
#' an index only ever saves a scan work, so an unusable one costs speed and
#' never rows. Indexes written by vectra 0.11.7 and earlier are superseded and
#' read as absent; call `create_index()` again to rebuild them.
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

#' Check whether a .vtr column has a usable hash index
#'
#' `TRUE` when the `.vtri` sidecar is present, readable, in the current format,
#' and built against the store as it now stands. An index that no longer matches
#' the store, or that cannot be read, reads as `FALSE`, because queries ignore
#' it and fall back to a scan; [create_index()] rebuilds it.
#'
#' @param path Path to a `.vtr` file.
#' @param column Character vector. Name(s) of column(s), in any order.
#'
#' @return Logical scalar: `TRUE` if the index exists and can be used.
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

# The .vtri sidecars sitting beside a store. Matched by prefix rather than by
# glob, so a path holding glob metacharacters still resolves.
.index_files <- function(path) {
  dir <- dirname(path)
  base <- basename(path)
  files <- list.files(dir, all.files = FALSE, no.. = TRUE)
  keep <- startsWith(files, paste0(base, ".")) & endsWith(files, ".vtri")
  file.path(dir, files[keep])
}

# What each of a store's indexes covers, read from the sidecar headers, so an
# index can be rebuilt from the columns it was built on.
.index_specs <- function(path) {
  specs <- lapply(.index_files(path), function(f) .Call(C_index_spec, path, f))
  specs[!vapply(specs, is.null, logical(1))]
}

# Rebuild every index a store carries, from the whole store.
.rebuild_indexes <- function(path) {
  for (spec in .index_specs(path))
    create_index(path, spec$columns, ci = spec$ci)
  invisible(NULL)
}

# Bring every index a store carries up to date after a row append.
#
# An append leaves the existing row groups where they are, so the row groups an
# index already names still hold the keys it maps to them; the only thing it
# does not cover is the row groups just appended. Each index therefore takes
# those in and keeps the rest, reading only the appended data -- which is what
# keeps an indexed store's append off the size of the store. An index that
# cannot be extended (unreadable, or built against a store this one is not an
# extension of) is rebuilt in full instead.
.extend_indexes <- function(path) {
  for (f in .index_files(path)) {
    if (isTRUE(.Call(C_extend_index, path, f))) next
    spec <- .Call(C_index_spec, path, f)
    if (!is.null(spec)) create_index(path, spec$columns, ci = spec$ci)
  }
  invisible(NULL)
}
