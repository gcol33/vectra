#' Print a vectra query node
#'
#' @param x A `vectra_node` object.
#' @param ... Ignored.
#'
#' @return Invisible `x`.
#'
#' @export
print.vectra_node <- function(x, ...) {
  schema <- .Call(C_node_schema, x$.node)
  cat("vectra query node\n")
  cat(sprintf("Columns (%d):\n", length(schema$name)))
  for (i in seq_along(schema$name)) {
    cat(sprintf("  %s <%s>\n", schema$name[i], schema$type[i]))
  }
  invisible(x)
}

#' Get a glimpse of a vectra table
#'
#' Shows column names, types, and a preview of the first few values without
#' collecting the full result.
#'
#' @param x A `vectra_node` object.
#' @param width Maximum number of preview rows to fetch (default 5).
#' @param ... Ignored.
#'
#' @return Invisible `x`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' tbl(f) |> glimpse()
#' unlink(f)
#'
#' @export
glimpse <- function(x, width = 5L, ...) {
  UseMethod("glimpse")
}

#' @export
glimpse.vectra_node <- function(x, width = 5L, ...) {
  schema <- .Call(C_node_schema, x$.node)
  n_cols <- length(schema$name)

  type_map <- c("0" = "int64", "1" = "double", "2" = "bool", "3" = "string")
  types <- type_map[as.character(schema$type)]

  cat(sprintf("vectra lazy table [? x %d]\n", n_cols))

  # Fetch preview rows
  preview <- head(x, width)
  if (nrow(preview) == 0) {
    for (i in seq_len(n_cols)) {
      cat(sprintf("$ %-15s <%s>\n", schema$name[i], types[i]))
    }
  } else {
    term_width <- getOption("width", 80L)
    for (i in seq_len(n_cols)) {
      col <- preview[[schema$name[i]]]
      vals <- if (is.character(col)) {
        paste0('"', col, '"')
      } else {
        format(col, trim = TRUE)
      }
      vals_str <- paste(vals, collapse = ", ")
      prefix <- sprintf("$ %-15s <%s> ", schema$name[i], types[i])
      avail <- term_width - nchar(prefix)
      if (nchar(vals_str) > avail) {
        vals_str <- paste0(substr(vals_str, 1, avail - 3), "...")
      }
      cat(prefix, vals_str, "\n", sep = "")
    }
  }

  invisible(x)
}
