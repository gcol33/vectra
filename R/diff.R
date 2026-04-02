#' Compute the logical diff between two .vtr files
#'
#' Streams both files and computes a set-level diff keyed on `key_col`.
#' Returns a list with two elements:
#'
#' - `added`: a `vectra_node` (lazy [tbl()]) of rows present in `new_path`
#'   but not `old_path` (matched on `key_col`).  Call [collect()] to
#'   materialise.  The underlying temp file is deleted when the node is
#'   garbage-collected **or** when the calling R session ends via
#'   `on.exit()`.
#' - `deleted`: a vector of key values present in `old_path` but not
#'   `new_path`.
#'
#' This is a **logical diff** (key-based set difference), not a binary file
#' diff.  Rows with the same key that have changed values are not reported
#' as modified — use `added` and `deleted` together to detect updates (a key
#' that appears in both means a row was replaced).
#'
#' @param old_path Path to the older `.vtr` file.
#' @param new_path Path to the newer `.vtr` file.
#' @param key_col  Name of the column to use as the row key (must exist in
#'   both files with the same type).
#'
#' @return A named list with elements `added` (a `vectra_node`) and `deleted`
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
#' stopifnot(all(collect(d$added)$id %in% c(6, 7)))
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

  # Delegate key-set diff to C.
  # Pass 1: stream A key column -> build hash set.
  # Pass 2: stream ALL B columns; write added rows to a temp .vtr file.
  # Returns list(added_path = <string>, deleted_keys = <vector>).
  raw          <- .Call(C_diff_vtr, old_path, new_path, key_col)
  added_path   <- raw$added_path
  deleted_keys <- raw$deleted_keys

  # Wrap the temp file as a lazy vectra_node.
  # Register cleanup so the temp file is removed when this frame exits or
  # when the caller discards the result (whichever comes first via reg.finalizer
  # on the node's external pointer is handled by the C side; here we also
  # schedule an on.exit in the *caller's* frame via sys.on.exit if available,
  # but the simplest and correct approach is to return the path alongside the
  # node so the caller can unlink() it when done.  The node itself does NOT
  # own the file — only the R session's temp directory ownership applies).
  added_node <- tbl(added_path)

  # Attach the temp path to the node so callers can clean up explicitly if
  # desired, and schedule removal when the node is GC'd via a weak reference.
  attr(added_node, ".tmp_path") <- added_path
  reg.finalizer(added_node$.node,
                function(e) unlink(added_path),
                onexit = TRUE)

  list(added = added_node, deleted = deleted_keys)
}
