#' Execute a lazy query and return the result
#'
#' Pulls all batches from the execution plan and materializes the result in
#' memory. A plain query returns a data.frame. A query whose output still
#' carries a geometry column, which the spatial verbs ([spatial_map()],
#' [spatial_join()], [spatial_clip()], [spatial_overlay()] and the rest) mark on
#' the node, returns an `sf` object, with the hex-WKB column decoded and the
#' node's coordinate reference system set. A string column whose values are
#' hex-encoded WKB is recognised from its content, so a stored geometry column
#' comes back as `sf` without any spatial verb. The geometry marker follows the
#' column through [filter()], [select()], [rename()], [mutate()] and joins, and
#' is dropped once the column is.
#'
#' For a result still larger than RAM, keep it as a node and write it out with
#' [write_vtr()], stream it to a vector file with [sf::st_write()], or reduce it
#' with [collect_chunked()].
#'
#' @param x A `vectra_node` object.
#' @param sf If `TRUE` (default), a spatial result is returned as an `sf`
#'   object. `FALSE` returns the data.frame with the geometry as a hex-WKB
#'   string column. Ignored for a non-spatial query.
#' @param geom Name of a hex-WKB geometry column to decode when the node does
#'   not already carry one, for instance a column read straight from a `.vtr`
#'   file. `NULL` (default) uses the geometry column the node carries.
#' @param crs Coordinate reference system for the `sf` result. `NULL` (default)
#'   uses the one the node carries, or leaves it unknown.
#' @param ... Ignored.
#'
#' @return A data.frame, or an `sf` object for a spatial query.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' result <- tbl(f) |> collect()
#' head(result)
#' unlink(f)
#'
#' @seealso [spatial_map()], [collect_chunked()]
#' @export
collect <- function(x, ...) {
  UseMethod("collect")
}

#' @export
collect.vectra_node <- function(x, sf = TRUE, geom = NULL, crs = NULL, ...) {
  df <- .Call(C_collect, x$.node)
  if (!isTRUE(sf)) return(df)
  marked <- is.null(geom) && !is.null(x$.geom) && x$.geom %in% names(df)
  if (is.null(geom)) geom <- if (marked) x$.geom else .find_wkb_column(df)
  if (is.null(geom)) return(df)
  if (!geom %in% names(df))
    stop(sprintf("geometry column '%s' not found", geom), call. = FALSE)
  if (!requireNamespace("sf", quietly = TRUE)) {
    if (marked)
      message("'sf' is not installed; returning the geometry as a hex-WKB ",
              "column. Install it with install.packages(\"sf\") to get an sf object.")
    return(df)
  }
  .df_to_sf(df, geom, if (is.null(crs)) x$.crs else crs)
}

# The first character column holding hex-encoded plain WKB, or NULL. A value
# opens with a byte-order byte and a four-byte geometry type word (1-7, no Z/M
# or SRID flag bits): "01" then "0t000000" little-endian, "00" then "0000000t"
# big-endian. Every non-NA value among the first few must match and be an
# even-length hex string. The column named "geometry" wins a tie.
.find_wkb_column <- function(df, sample_n = 16L) {
  pat <- "^(010[1-7]000000|0{9}[1-7])[0-9a-fA-F]*$"
  hit <- character(0)
  for (nm in names(df)) {
    v <- df[[nm]]
    if (!is.character(v)) next
    v <- utils::head(v[!is.na(v)], sample_n)
    if (length(v) && all(nchar(v) %% 2L == 0L & grepl(pat, v)))
      hit <- c(hit, nm)
  }
  if (!length(hit)) return(NULL)
  if ("geometry" %in% hit) "geometry" else hit[1L]
}

#' @export
collect.data.frame <- function(x, ...) {
  x
}
