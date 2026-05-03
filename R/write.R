# -- internal helpers ---------------------------------------------------------

#' Convert a data.frame to a temporary vectra_node
#'
#' Writes `df` to a temporary `.vtr` file, opens it as a lazy node, and
#' registers cleanup of the temp file on the *caller's* frame so the file
#' lives until the caller returns.
#'
#' @param df A `data.frame`.
#' @param envir Environment in which to register the `on.exit` cleanup
#'   (typically `parent.frame()`).
#' @return A `vectra_node`.
#' @noRd
df_to_node <- function(df, envir = parent.frame()) {

  tmp <- tempfile(fileext = ".vtr")
  do.call(on.exit, list(substitute(unlink(tmp)), add = TRUE), envir = envir)
  write_vtr(df, tmp)
  tbl(tmp)
}

check_scalar_string <- function(x, name = deparse(substitute(x))) {
  if (!is.character(x) || length(x) != 1)
    stop(sprintf("%s must be a single character string", name))
}

# -- write_csv ----------------------------------------------------------------

#' Write query results or a data.frame to a CSV file
#'
#' For `vectra_node` inputs, data is streamed batch-by-batch to disk without
#' materializing the full result in memory. For `data.frame` inputs, the data
#' is written directly.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the output CSV file.
#' @param ... Reserved for future use.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars[1:5, ], f)
#' csv <- tempfile(fileext = ".csv")
#' tbl(f) |> write_csv(csv)
#' unlink(c(f, csv))
#'
#' @export
write_csv <- function(x, path, ...) {
  UseMethod("write_csv")
}

#' @export
write_csv.vectra_node <- function(x, path, ...) {
  check_scalar_string(path)
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_csv, x$.node, path)
  invisible(NULL)
}

#' @export
write_csv.data.frame <- function(x, path, ...) {
  write_csv.vectra_node(df_to_node(x), path, ...)
}

#' Write query results or a data.frame to a SQLite table
#'
#' For `vectra_node` inputs, data is streamed batch-by-batch to disk without
#' materializing the full result in memory. For `data.frame` inputs, the data
#' is written directly.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the SQLite database.
#' @param table Name of the table to create/write into.
#' @param ... Reserved for future use.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' db <- tempfile(fileext = ".sqlite")
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars[1:5, ], f)
#' tbl(f) |> write_sqlite(db, "cars")
#' unlink(c(f, db))
#'
#' @export
write_sqlite <- function(x, path, table, ...) {
  UseMethod("write_sqlite")
}

#' @export
write_sqlite.vectra_node <- function(x, path, table, ...) {
  check_scalar_string(path)
  check_scalar_string(table)
  path <- normalizePath(path, mustWork = FALSE)
  .Call(C_write_sqlite, x$.node, path, table)
  invisible(NULL)
}

#' @export
write_sqlite.data.frame <- function(x, path, table, ...) {
  write_sqlite.vectra_node(df_to_node(x), path, table, ...)
}

#' Write query results to a GeoTIFF file
#'
#' The data must contain `x` and `y` columns (pixel center coordinates) and
#' one or more numeric band columns. Grid dimensions and geotransform are
#' inferred from the x/y coordinate arrays. Missing pixels are written as NaN
#' (or the type-appropriate nodata value for integer pixel types).
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the output GeoTIFF file.
#' @param compress Logical; use DEFLATE compression? Default `FALSE`.
#' @param pixel_type Character string specifying the output pixel type.
#'   One of `"float64"` (default), `"float32"`, `"int16"`, `"int32"`,
#'   `"uint8"`, or `"uint16"`.
#' @param metadata Optional character string of GDAL_METADATA XML to embed
#'   in the file (tag 42112). Use [tiff_metadata()] to read it back.
#' @param crs Optional CRS to embed as a GeoKey directory (TIFF tag 34735).
#'   Accepts an integer EPSG code, an `"EPSG:xxxx"` string, or a list with
#'   named fields `epsg`, `geographic` (`TRUE`/`FALSE`), and optionally
#'   `citation`. Codes that are not auto-classified as projected/geographic
#'   default to projected; pass `geographic = TRUE` to override.
#'   Use [tiff_crs()] to read it back.
#' @param ... Reserved for future use.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' \donttest{
#' # Write as int16 with DEFLATE compression and an EPSG:4326 GeoKey
#' df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = c(100, 200, 300, 400))
#' f <- tempfile(fileext = ".tif")
#' write_tiff(df, f, compress = TRUE, pixel_type = "int16", crs = 4326L)
#' tiff_crs(f)
#' unlink(f)
#' }
#'
#' @export
write_tiff <- function(x, path, compress = FALSE, pixel_type = "float64",
                       metadata = NULL, crs = NULL, ...) {
  UseMethod("write_tiff")
}

.pixel_type_code <- c(
  float64 = 0L, float32 = 1L, int16 = 2L, int32 = 3L,
  uint8 = 4L, uint16 = 5L
)

# Translate a user-supplied `crs` argument into (epsg_geographic,
# epsg_projected, citation). The C side requires us to commit each EPSG to
# exactly one slot. We classify with a small table covering the common
# geographic codes (4326 WGS84, 4269 NAD83, the EPSG:42xx historical block
# and EPSG:4xxx geographic block); everything else is treated as projected,
# matching the behavior most users want for UTM/Web Mercator/national grids.
.crs_is_geographic_code <- function(epsg) {
  if (is.na(epsg)) return(FALSE)
  # EPSG geographic 2D CRS block is roughly 4001..4999, with a few outliers
  # in 4xxxx that we don't try to enumerate.
  epsg >= 4001L && epsg <= 4999L
}

.parse_crs <- function(crs) {
  if (is.null(crs)) return(list(epsg_g = 0L, epsg_p = 0L, cit = NULL))

  cit <- NULL
  geographic <- NULL
  epsg <- NA_integer_

  if (is.list(crs)) {
    if (!is.null(crs$epsg))      epsg <- as.integer(crs$epsg)
    if (!is.null(crs$citation))  cit  <- as.character(crs$citation)
    if (!is.null(crs$geographic)) geographic <- isTRUE(crs$geographic)
  } else if (is.character(crs) && length(crs) == 1) {
    m <- regmatches(crs, regexec("^EPSG:(\\d+)$", crs, ignore.case = TRUE))[[1]]
    if (length(m) == 2) epsg <- as.integer(m[2])
    else stop("crs string must look like 'EPSG:xxxx', got '", crs, "'")
  } else if (is.numeric(crs) && length(crs) == 1) {
    epsg <- as.integer(crs)
  } else {
    stop("crs must be NULL, an integer EPSG, an 'EPSG:xxxx' string, ",
         "or a list with $epsg")
  }

  if (is.na(epsg) || epsg <= 0)
    stop("crs needs a positive integer EPSG code")

  if (is.null(geographic))
    geographic <- .crs_is_geographic_code(epsg)

  if (geographic)
    list(epsg_g = epsg, epsg_p = 0L, cit = cit)
  else
    list(epsg_g = 0L, epsg_p = epsg, cit = cit)
}

#' @export
write_tiff.vectra_node <- function(x, path, compress = FALSE,
                                   pixel_type = "float64",
                                   metadata = NULL, crs = NULL, ...) {
  check_scalar_string(path)
  path <- normalizePath(path, mustWork = FALSE)

  pt <- .pixel_type_code[pixel_type]
  if (is.na(pt))
    stop(sprintf("pixel_type must be one of [%s], got '%s'",
                 paste(names(.pixel_type_code), collapse = ", "), pixel_type))

  if (!is.null(metadata) && (!is.character(metadata) || length(metadata) != 1))
    stop("metadata must be a single character string or NULL")

  cs <- .parse_crs(crs)

  # Always use the typed path — it handles all pixel types including float64
  .Call(C_write_tiff_typed, x$.node, path, as.logical(compress), pt, metadata,
        cs$epsg_g, cs$epsg_p, cs$cit)
  invisible(NULL)
}

#' @export
write_tiff.data.frame <- function(x, path, compress = FALSE,
                                  pixel_type = "float64",
                                  metadata = NULL, crs = NULL, ...) {
  write_tiff.vectra_node(df_to_node(x), path, compress, pixel_type,
                         metadata, crs, ...)
}

#' Write data to a .vtr file
#'
#' For `vectra_node` inputs (lazy queries from any format: CSV, SQLite, TIFF,
#' or another .vtr), data is streamed batch-by-batch to disk without
#' materializing the full result in memory. Each batch becomes one row group.
#' The output file is written atomically (via temp file + rename) so readers
#' never see a partial file.
#'
#' For `data.frame` inputs, the data is written directly from memory.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path for the output .vtr file.
#' @param compress Compression level: `"fast"` (default, byte-shuffle + greedy
#'   LZ), `"small"` (per-block adaptive — tries greedy LZ, separated-streams
#'   LZ, and LZ + Huffman entropy coding, and writes whichever shrank the
#'   block the most; never worse than `"fast"` on any block, typically
#'   10-25 percent smaller files at the cost of slower encode), or `"none"`.
#' @param batch_size Target number of rows per row group in the output file.
#'   Defaults to 131072 for data.frames (1 MB per double column, cache-friendly
#'   for decompression). For nodes, defaults to `NULL` (one row group per
#'   upstream batch).
#' @param col_types Optional named character vector specifying narrow integer
#'   storage types. Names must match column names; values must be `"int8"`,
#'   `"int16"`, or `"int32"`. Only applies to integer columns.
#'   Example: `col_types = c(age = "int8", year = "int16")`.
#' @param quantize Optional named list for lossy quantization of `double`
#'   columns. Each element is named after a column and is itself a named list
#'   with `scale` (or `precision = 1/scale`), `type` (`"int8"`, `"int16"`,
#'   `"int32"`; default `"int16"`), and optionally `offset` (default 0).
#'   Example: `quantize = list(temp = list(precision = 0.001, type = "int16"))`.
#' @param spatial Optional list for 2D spatial predictor encoding. Either a
#'   global spec applied to all numeric columns (`list(nx = 2000, ny = 2000)`)
#'   or per-column specs (`list(temp = list(nx = 2000, ny = 2000))`).
#'   When provided, a spatial predictor removes smooth 2D trends before
#'   compression, dramatically improving compression of raster data.
#'   Combines with `quantize` for maximum effect.
#' @param ... Additional arguments passed to methods.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' # From a data.frame
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#'
#' # Streaming format conversion (CSV -> VTR)
#' csv <- tempfile(fileext = ".csv")
#' write.csv(mtcars, csv, row.names = FALSE)
#' f2 <- tempfile(fileext = ".vtr")
#' tbl_csv(csv) |> write_vtr(f2)
#'
#' unlink(c(f, f2, csv))
#'
#' @export
write_vtr <- function(x, path, compress = c("fast", "small", "none"), batch_size = NULL,
                      col_types = NULL, quantize = NULL, spatial = NULL,
                      ...) {
  UseMethod("write_vtr")
}

.check_compress <- function(compress) {
  match.arg(compress, c("fast", "small", "none"))
}

#' @export
write_vtr.vectra_node <- function(x, path, compress = c("fast", "small", "none"),
                                  batch_size = NULL, col_types = NULL,
                                  quantize = NULL, spatial = NULL, ...) {
  check_scalar_string(path)
  path <- normalizePath(path, mustWork = FALSE)
  compress <- .check_compress(compress)
  bs <- if (!is.null(batch_size)) as.double(batch_size) else NULL
  .Call(C_write_vtr_node, x$.node, path, bs, compress, col_types, quantize,
        spatial)
  invisible(NULL)
}

#' @export
write_vtr.data.frame <- function(x, path, compress = c("fast", "small", "none"),
                                 batch_size = 131072L, col_types = NULL,
                                 quantize = NULL, spatial = NULL, ...) {
  check_scalar_string(path)
  path <- normalizePath(path, mustWork = FALSE)
  compress <- .check_compress(compress)
  .Call(C_write_vtr, x, path, as.integer(batch_size), compress, col_types,
        quantize, spatial)
  invisible(NULL)
}

#' Append rows to an existing .vtr file
#'
#' Appends one or more new row groups to the end of an existing `.vtr` file
#' without touching or recompressing existing row groups. The schema of `x`
#' must exactly match the schema of the target file (same column names and
#' types, in the same order).
#'
#' The operation is not fully atomic: if the process is interrupted after
#' new row groups are written but before the header is patched, the file
#' will be in a corrupted state. Use `write_vtr()` for safety-critical
#' write-once workloads.
#'
#' @param x A `vectra_node` (lazy query) or a `data.frame`.
#' @param path File path of an existing `.vtr` file to append to.
#' @param ... Additional arguments passed to methods.
#'
#' @return Invisible `NULL`.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars[1:10, ], f)
#' append_vtr(mtcars[11:20, ], f)
#' result <- tbl(f) |> collect()
#' stopifnot(nrow(result) == 20L)
#' unlink(f)
#'
#' @export
append_vtr <- function(x, path, ...) {
  UseMethod("append_vtr")
}

#' @export
append_vtr.vectra_node <- function(x, path, ...) {
  check_scalar_string(path)
  path <- normalizePath(path, mustWork = TRUE)
  # Force GC so any lingering tbl()-owned file handles on `path` are closed
  # before we stream-rewrite the file. On Windows the rename at the end of
  # C_append_vtr otherwise fails with a sharing violation.
  gc(verbose = FALSE)
  .Call(C_append_vtr, x$.node, path)
  invisible(NULL)
}

#' @export
append_vtr.data.frame <- function(x, path, ...) {
  append_vtr.vectra_node(df_to_node(x), path, ...)
}
