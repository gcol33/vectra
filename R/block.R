#' Materialize a vectra node into a reusable in-memory block
#'
#' Consumes a vectra node (pulling all batches) and stores the result as a
#' persistent columnar block in memory.  Unlike nodes, blocks can be probed
#' repeatedly via [block_lookup()] without re-scanning.
#'
#' @param .data A `vectra_node` (consumed; cannot be used after this call).
#' @return A `vectra_block` object (external pointer to C-level ColumnBlock).
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".vtr")
#' df <- data.frame(taxonID = 1:3,
#'                  canonicalName = c("Quercus robur", "Pinus sylvestris",
#'                                    "Fagus sylvatica"))
#' write_vtr(df, f)
#' blk <- materialize(tbl(f) |> select(taxonID, canonicalName))
#' hits <- block_lookup(blk, "canonicalName",
#'                      c("Quercus robur", "Pinus sylvestris"))
#' unlink(f)
#' }
#'
#' @export
materialize <- function(.data) {
  UseMethod("materialize")
}

#' @export
materialize.vectra_node <- function(.data) {
  xptr <- .Call(C_block_materialize, .data$.node)
  schema <- tryCatch(.Call(C_node_schema, .data$.node), error = function(e) NULL)
  structure(list(.block = xptr, .path = .data$.path),
            class = "vectra_block")
}

#' @export
print.vectra_block <- function(x, ...) {
  cat("vectra_block [materialized in memory]\n")
  invisible(x)
}


#' Probe a materialized block by column value
#'
#' Performs a hash lookup on a string column of a materialized block.
#' Returns all rows where the column value matches one of the query keys.
#' Hash indices are built lazily on first use and cached for subsequent calls.
#'
#' @param block A `vectra_block` from [materialize()].
#' @param column Character scalar. Name of the string column to match against.
#' @param keys Character vector. Query values to look up.
#' @param ci Logical. Case-insensitive matching (default `FALSE`).
#' @return A data.frame with column `query_idx` (1-based position in `keys`)
#'   plus all columns from the block, for each (query, block_row) match pair.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".vtr")
#' df <- data.frame(taxonID = 1:2,
#'                  canonicalName = c("Quercus robur", "Pinus sylvestris"))
#' write_vtr(df, f)
#' blk <- materialize(tbl(f))
#' hits <- block_lookup(blk, "canonicalName", c("Quercus robur"))
#' ci_hits <- block_lookup(blk, "canonicalName", c("quercus robur"), ci = TRUE)
#' unlink(f)
#' }
#'
#' @export
block_lookup <- function(block, column, keys, ci = FALSE) {
  if (!inherits(block, "vectra_block"))
    stop("block must be a vectra_block from materialize()", call. = FALSE)
  .Call(C_block_lookup, block$.block, column, keys, ci)
}


#' Fuzzy-match query keys against a materialized block
#'
#' Computes string distances between query keys and a string column in a
#' materialized block. Optionally uses exact-match blocking on a second column
#' (e.g., genus) to reduce the search space.
#'
#' @param block A `vectra_block` from [materialize()].
#' @param column Character scalar. Name of the string column to fuzzy-match against.
#' @param keys Character vector. Query strings to match.
#' @param method Character. Distance method: `"dl"` (Damerau-Levenshtein, default),
#'   `"levenshtein"`, or `"jw"` (Jaro-Winkler).
#' @param max_dist Numeric. Maximum normalized distance (default 0.2).
#' @param block_col Optional character scalar. Column name for exact-match blocking
#'   (e.g., genus). When provided, only rows where `block_col` matches the
#'   corresponding `block_keys` value are compared.
#' @param block_keys Optional character vector (same length as `keys`). Exact-match
#'   values for blocking. Required when `block_col` is provided.
#' @param n_threads Integer. Number of OpenMP threads (default 4L).
#' @return A data.frame with columns `query_idx` (1-based position in `keys`),
#'   `fuzzy_dist` (normalized distance), plus all columns from the block.
#'
#' @export
block_fuzzy_lookup <- function(block, column, keys, method = "dl",
                               max_dist = 0.2, block_col = NULL,
                               block_keys = NULL, n_threads = 4L) {
  if (!inherits(block, "vectra_block"))
    stop("block must be a vectra_block from materialize()", call. = FALSE)
  method_int <- match(method, c("dl", "levenshtein", "jw")) - 1L
  if (is.na(method_int))
    stop("method must be 'dl', 'levenshtein', or 'jw'", call. = FALSE)
  .Call(C_block_fuzzy_lookup, block$.block, column, keys,
        as.integer(method_int), as.double(max_dist),
        block_col, block_keys, as.integer(n_threads))
}
