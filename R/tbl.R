#' Create a lazy table reference from a .vtr file
#'
#' Opens a vectra1 file and returns a lazy query node. No data is read until
#' [collect()] is called.
#'
#' @param path Path to a `.vtr` file.
#'
#' @return A `vectra_node` object representing a lazy scan of the file.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' node <- tbl(f)
#' print(node)
#' unlink(f)
#'
#' @export
tbl <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_scan_node, path)
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a CSV file
#'
#' Opens a CSV file for lazy, streaming query execution. Column types are
#' inferred from the first 1000 rows. No data is read until [collect()] is
#' called. Gzip-compressed files (`.csv.gz`) are supported transparently.
#'
#' @param path Path to a `.csv` or `.csv.gz` file.
#' @param batch_size Number of rows per batch (default 65536).
#'
#' @return A `vectra_node` object representing a lazy scan of the CSV file.
#'
#' @examples
#' f <- tempfile(fileext = ".csv")
#' write.csv(mtcars, f, row.names = FALSE)
#' node <- tbl_csv(f)
#' print(node)
#' unlink(f)
#'
#' @export
tbl_csv <- function(path, batch_size = .DEFAULT_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_csv_scan_node, path, as.double(batch_size))
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a SQLite database
#'
#' Opens a SQLite database and lazily scans a table. Column types are inferred
#' from declared types in the CREATE TABLE statement. All filtering, grouping,
#' and aggregation is handled by vectra's C engine --- no SQL parsing needed.
#' No data is read until [collect()] is called.
#'
#' @param path Path to a SQLite database file.
#' @param table Name of the table to scan.
#' @param batch_size Number of rows per batch (default 65536).
#'
#' @return A `vectra_node` object representing a lazy scan of the table.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".sqlite")
#' write_sqlite(mtcars, f, "cars")
#' node <- tbl_sqlite(f, "cars")
#' node |> filter(cyl == 6) |> collect()
#' unlink(f)
#' }
#'
#' @export
tbl_sqlite <- function(path, table, batch_size = .DEFAULT_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!is.character(table) || length(table) != 1)
    stop("table must be a single character string")

  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_sql_scan_node, path, table, as.double(batch_size))
  structure(list(.node = xptr, .path = path, .table = table),
            class = "vectra_node")
}

#' Create a lazy table reference from an Excel (.xlsx) file
#'
#' Reads a sheet from an Excel workbook into a vectra node for lazy query
#' execution. The sheet is read into memory via
#' \code{\link[openxlsx2:read_xlsx]{openxlsx2::read_xlsx()}} and then converted
#' to vectra's internal format. Requires the \pkg{openxlsx2} package.
#'
#' @param path Path to an `.xlsx` file.
#' @param sheet Sheet to read: either a name (character) or 1-based index
#'   (integer). Default `1L` (first sheet).
#' @param batch_size Number of rows per batch (default 65536).
#'
#' @return A `vectra_node` object representing a lazy scan of the sheet.
#'
#' @examples
#' \donttest{
#' if (requireNamespace("openxlsx2", quietly = TRUE)) {
#'   f <- tempfile(fileext = ".xlsx")
#'   openxlsx2::write_xlsx(mtcars, f)
#'   node <- tbl_xlsx(f)
#'   node |> filter(cyl == 6) |> collect()
#'   unlink(f)
#' }
#' }
#'
#' @export
tbl_xlsx <- function(path, sheet = 1L, batch_size = .DEFAULT_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!(is.character(sheet) || is.numeric(sheet)) || length(sheet) != 1)
    stop("sheet must be a single character string or integer")

  if (!requireNamespace("openxlsx2", quietly = TRUE))
    stop("Package 'openxlsx2' is required for tbl_xlsx(). ",
         "Install it with: install.packages('openxlsx2')")

  path <- normalizePath(path, mustWork = TRUE)
  df <- openxlsx2::read_xlsx(path, sheet = sheet)
  df_to_node(df)
}

#' Create a lazy table reference from a GeoTIFF raster
#'
#' Opens a GeoTIFF file and returns a lazy query node. Each pixel becomes a row
#' with columns `x`, `y`, `band1`, `band2`, etc. Coordinates are pixel centers
#' derived from the affine geotransform. NoData values become `NA`.
#'
#' Use `filter(x >= ..., y <= ...)` for extent-based cropping and
#' `filter(band1 > ...)` for value-based cropping. Results can be converted
#' back to a raster with `terra::rast(df, type = "xyz")`.
#'
#' @param path Path to a GeoTIFF file.
#' @param batch_size Number of raster rows per batch (default 256).
#'
#' @return A `vectra_node` object representing a lazy scan of the raster.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = as.double(rep(1:4, 3)),
#'                  y = as.double(rep(1:3, each = 4)),
#'                  band1 = as.double(1:12))
#' write_tiff(df, f)
#' node <- tbl_tiff(f)
#' node |> filter(band1 > 6) |> collect()
#' unlink(f)
#' }
#'
#' @export
tbl_tiff <- function(path, batch_size = .TIFF_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_tiff_scan_node, path, as.double(batch_size))
  meta <- .Call(C_tiff_scan_meta, xptr)
  structure(list(.node = xptr, .path = path, .tiff_meta = meta),
            class = "vectra_node")
}

#' Extract raster values at point coordinates
#'
#' Samples band values from a GeoTIFF at specific (x, y) locations using the
#' file's affine geotransform. Only the strips containing query points are read,
#' making this efficient for sparse point sets on large rasters.
#'
#' Points that fall outside the raster extent return `NA` for all bands.
#' Pixel assignment uses nearest-pixel rounding (i.e., the point is assigned to
#' the pixel whose center is closest).
#'
#' @param path Path to a GeoTIFF file.
#' @param x Numeric vector of x coordinates, or a data.frame / matrix with
#'   columns named `x` and `y`.
#' @param y Numeric vector of y coordinates (ignored if `x` is a data.frame).
#'
#' @return A data.frame with columns `x`, `y`, `band1`, `band2`, etc.
#'   One row per input point, in the same order as the input.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = as.double(rep(1:4, 3)),
#'                  y = as.double(rep(1:3, each = 4)),
#'                  band1 = as.double(1:12))
#' write_tiff(df, f)
#'
#' # Sample at specific locations via data.frame
#' pts <- data.frame(x = c(2, 3), y = c(1, 2))
#' tiff_extract_points(f, pts)
#'
#' # Or pass x and y separately
#' tiff_extract_points(f, x = c(2, 3), y = c(1, 2))
#' unlink(f)
#' }
#'
#' @export
tiff_extract_points <- function(path, x, y = NULL) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)

  if (is.data.frame(x) || is.matrix(x)) {
    if (!all(c("x", "y") %in% colnames(x)))
      stop("when x is a data.frame/matrix it must have columns 'x' and 'y'")
    y <- as.double(x[, "y"])
    x <- as.double(x[, "x"])
  } else {
    if (is.null(y))
      stop("y is required when x is a numeric vector")
    x <- as.double(x)
    y <- as.double(y)
  }

  if (length(x) != length(y))
    stop(sprintf("x and y must have the same length (got %d and %d)",
                 length(x), length(y)))

  .Call(C_tiff_extract_points, path, x, y)
}

#' Read GDAL_METADATA from a GeoTIFF
#'
#' Returns the GDAL_METADATA XML string (TIFF tag 42112) embedded in a
#' GeoTIFF file. Returns `NA` if the tag is not present.
#'
#' @param path Path to a GeoTIFF file.
#' @return A single character string containing the XML, or `NA_character_`.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = as.double(1:4))
#' write_tiff(df, f, metadata = "<GDALMetadata></GDALMetadata>")
#' tiff_metadata(f)
#' unlink(f)
#' }
#'
#' @export
tiff_metadata <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  .Call(C_tiff_read_metadata, path)
}
