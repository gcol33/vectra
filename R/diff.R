#' Compute the logical diff between two .vtr files
#'
#' Reads both files into memory via [collect()] and computes a set-level
#' diff keyed on `key_col`. Returns a list with two elements:
#'
#' - `added`: a `data.frame` of rows present in `new_path` but not `old_path`
#'   (matched on `key_col`).
#' - `deleted`: a vector of key values present in `old_path` but not `new_path`.
#'
#' This is a **logical diff** (key-based set difference), not a binary file diff.
#' Rows with the same key that have changed values are not reported as
#' modified — use `added` and `deleted` together to detect updates (a key that
#' appears in both means a row was replaced).
#'
#' @param old_path Path to the older `.vtr` file.
#' @param new_path Path to the newer `.vtr` file.
#' @param key_col  Name of the column to use as the row key (must exist in
#'   both files with the same type).
#'
#' @return A named list with elements `added` (a `data.frame`) and `deleted`
#'   (a vector of key values).
#'
#' @examples
#' f1 <- tempfile(fileext = ".vtr")
#' f2 <- tempfile(fileext = ".vtr")
#' df1 <- data.frame(id = 1:5, val = letters[1:5], stringsAsFactors = FALSE)
#' df2 <- data.frame(id = c(3L, 4L, 5L, 6L, 7L),
#'                   val = c("C", "d", "e", "f", "g"),
#'                   stringsAsFactors = FALSE)
#' write_vtr(df1, f1)
#' write_vtr(df2, f2)
#'
#' d <- diff_vtr(f1, f2, "id")
#' # Rows 1 and 2 deleted; rows 6 and 7 added
#' stopifnot(all(d$deleted %in% c(1, 2)))
#' stopifnot(all(d$added$id %in% c(6, 7)))
#'
#' unlink(c(f1, f2))
#'
#' @export
diff_vtr <- function(old_path, new_path, key_col) {
  if (!is.character(old_path) || length(old_path) != 1)
    stop("old_path must be a single character string")
  if (!is.character(new_path) || length(new_path) != 1)
    stop("new_path must be a single character string")
  if (!is.character(key_col) || length(key_col) != 1)
    stop("key_col must be a single character string")

  old_path <- normalizePath(old_path, mustWork = TRUE)
  new_path <- normalizePath(new_path, mustWork = TRUE)

  old_df <- tbl(old_path) |> collect()
  new_df <- tbl(new_path) |> collect()

  if (!key_col %in% names(old_df))
    stop(sprintf("key_col '%s' not found in old_path", key_col))
  if (!key_col %in% names(new_df))
    stop(sprintf("key_col '%s' not found in new_path", key_col))

  old_keys <- old_df[[key_col]]
  new_keys <- new_df[[key_col]]

  deleted <- old_keys[!old_keys %in% new_keys]
  added   <- new_df[!new_keys %in% old_keys, , drop = FALSE]

  list(added = added, deleted = deleted)
}
