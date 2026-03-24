#' Summarise with variable-length output per group
#'
#' Like [summarise()] but allows expressions that return more than one row
#' per group. Currently implemented via `collect()` fallback.
#'
#' @param .data A `vectra_node` object.
#' @param ... Named expressions.
#'
#' @return A data.frame (not a lazy node).
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(g = c("a", "a", "b"), x = c(1, 2, 3)), f)
#' tbl(f) |> group_by(g) |> reframe(range_x = range(x))
#' unlink(f)
#'
#' @export
reframe <- function(.data, ...) {
  UseMethod("reframe")
}

#' @export
reframe.vectra_node <- function(.data, ...) {
  df <- collect(.data)
  groups <- .data$.groups
  dots <- eval(substitute(alist(...)))
  # Expand across() calls
  dot_names_check <- names(dots)
  has_across <- any(vapply(dots, function(e) is.call(e) && identical(e[[1]], as.name("across")), logical(1)))
  if (has_across) {
    schema_names <- names(df)
    proxy <- df[0, , drop = FALSE]
    dots <- expand_across(dots, schema_names, parent.frame(), proxy)
  }
  dot_names <- names(dots)

  if (is.null(groups) || length(groups) == 0) {
    # Ungrouped: evaluate expressions on whole data
    env <- list2env(df, parent = parent.frame())
    results <- lapply(seq_along(dots), function(i) {
      eval(dots[[i]], envir = env)
    })
    names(results) <- dot_names
    as.data.frame(results, stringsAsFactors = FALSE)
  } else {
    # Grouped: split-apply-combine
    split_idx <- interaction(df[groups], drop = TRUE)
    pieces <- split(df, split_idx, drop = TRUE)

    result_list <- lapply(pieces, function(chunk) {
      env <- list2env(chunk, parent = parent.frame())
      results <- lapply(seq_along(dots), function(i) {
        eval(dots[[i]], envir = env)
      })
      names(results) <- dot_names
      n <- max(vapply(results, length, integer(1)))
      # Key columns
      key_df <- chunk[rep(1, n), groups, drop = FALSE]
      val_df <- as.data.frame(results, stringsAsFactors = FALSE)
      cbind(key_df, val_df)
    })

    result <- do.call(rbind, result_list)
    rownames(result) <- NULL
    result
  }
}
