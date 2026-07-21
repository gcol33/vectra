#' Dimensions of a lazy query
#'
#' Reports the shape of a `vectra_node` from plan metadata, without running
#' the query. Defining `dim()` is what makes base R's [nrow()] and [ncol()]
#' work on a node, since both read `dim(x)`.
#'
#' The column count always comes from the plan's schema. The row count is
#' available when it can be read from metadata: a `.vtr` table reports the
#' count stored in its row-group index (minus any rows [delete_vtr()] has
#' tombstoned), and the row-preserving verbs carry it through --
#' [select()], [mutate()], [rename()], [arrange()], [relocate()], window
#' functions, [head()], [slice_head()], `slice_min()`/`slice_max()`, and
#' [bind_rows()] over counted inputs.
#'
#' Verbs whose output length depends on the data -- [filter()], the joins,
#' [summarise()], [distinct()] -- report `NA` rows. Counting those means
#' running the query, which on a larger-than-RAM table is a full pass, so
#' `nrow()` reports what it knows rather than starting one. To get the exact
#' count, run the query:
#'
#' ```r
#' tbl(f) |> filter(x > 0) |> count() |> collect()
#' ```
#'
#' A CSV, SQLite, or TIFF source reports `NA` rows too: those formats carry no
#' row count to read.
#'
#' @param x A `vectra_node` object.
#'
#' @return A length-2 vector `c(rows, cols)`, integer unless the row count
#'   exceeds the largest integer R can hold, in which case both are doubles.
#'   `rows` is `NA` when the count is not derivable from metadata.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#'
#' dim(tbl(f))
#' nrow(tbl(f))                       # 32, straight from the row-group index
#' ncol(tbl(f) |> select(mpg, cyl))   # 2
#' nrow(tbl(f) |> head(5))            # 5
#' nrow(tbl(f) |> filter(cyl == 4))   # NA: needs the query to run
#'
#' # exact count for a filtered query
#' tbl(f) |> filter(cyl == 4) |> count() |> collect()
#'
#' unlink(f)
#'
#' @export
dim.vectra_node <- function(x) {
  n_rows <- .Call(C_node_static_rows, x$.node)
  n_cols <- length(.Call(C_node_schema, x$.node)$name)
  if (is.na(n_rows) || n_rows <= .Machine$integer.max)
    c(as.integer(n_rows), n_cols)
  else
    c(n_rows, as.double(n_cols))
}
