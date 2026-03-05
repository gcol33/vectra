#' Bind rows or columns from multiple vectra tables
#'
#' @param ... `vectra_node` objects or data.frames to combine.
#' @param .id Optional column name for a source identifier.
#'
#' @return A `vectra_node` (streaming) when all inputs are `vectra_node` with
#'   identical schemas and `.id` is NULL. Otherwise a data.frame.
#'
#' @details
#' When all inputs are `vectra_node` objects with identical column names and
#' types and no `.id` is requested, `bind_rows` creates a streaming
#' `ConcatNode` that iterates children sequentially without materializing.
#'
#' Otherwise, inputs are collected and combined in R. Missing columns are
#' filled with NA.
#'
#' `bind_cols` requires the same number of rows in each input.
#'
#' @export
bind_rows <- function(..., .id = NULL) {
  dots <- list(...)

  # Check if we can use streaming ConcatNode
  all_nodes <- all(vapply(dots, inherits, logical(1), "vectra_node"))
  if (all_nodes && is.null(.id) && length(dots) >= 2) {
    schemas <- lapply(dots, function(x) .Call(C_node_schema, x$.node))
    ref <- schemas[[1]]
    all_match <- all(vapply(schemas[-1], function(s) {
      identical(s$name, ref$name) && identical(s$type, ref$type)
    }, logical(1)))

    if (all_match) {
      xptrs <- lapply(dots, function(x) x$.node)
      new_xptr <- .Call(C_concat_node, xptrs)
      return(structure(list(.node = new_xptr, .path = NULL),
                       class = "vectra_node"))
    }
  }

  # Fallback: collect and combine in R
  dfs <- lapply(dots, function(x) {
    if (inherits(x, "vectra_node")) collect(x) else x
  })

  all_names <- unique(unlist(lapply(dfs, names)))

  aligned <- lapply(dfs, function(df) {
    missing <- setdiff(all_names, names(df))
    for (nm in missing) df[[nm]] <- NA
    df[all_names]
  })

  result <- do.call(rbind, aligned)
  rownames(result) <- NULL

  if (!is.null(.id)) {
    src <- rep(seq_along(aligned), vapply(aligned, nrow, integer(1)))
    result <- cbind(stats::setNames(data.frame(src), .id), result)
  }

  result
}

#' @rdname bind_rows
#' @export
bind_cols <- function(...) {
  dots <- list(...)
  dfs <- lapply(dots, function(x) {
    if (inherits(x, "vectra_node")) collect(x) else x
  })

  nrows <- vapply(dfs, nrow, integer(1))
  if (length(unique(nrows)) > 1)
    stop(sprintf("all inputs must have the same number of rows (got %s)",
                 paste(unique(nrows), collapse = ", ")))

  do.call(cbind, dfs)
}
