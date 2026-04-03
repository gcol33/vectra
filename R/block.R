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
#' \dontrun{
#' blk <- materialize(tbl("backbone.vtr") |> select(taxonID, canonicalName))
#' hits <- block_lookup(blk, "canonicalName", c("Quercus robur", "Pinus sylvestris"))
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
#' \dontrun{
#' hits <- block_lookup(blk, "canonicalName", c("Quercus robur"))
#' ci_hits <- block_lookup(blk, "canonicalName", c("quercus robur"), ci = TRUE)
#' }
#'
#' @export
block_lookup <- function(block, column, keys, ci = FALSE) {
  if (!inherits(block, "vectra_block"))
    stop("block must be a vectra_block from materialize()", call. = FALSE)
  .Call(C_block_lookup, block$.block, column, keys, ci)
}
