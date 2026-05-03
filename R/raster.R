# raster.R — VECR raster API (Phase 1).
#
# Four primary entry points:
#   vec_write_raster   write a matrix or 3D array to a .vec raster
#   vec_open_raster    open a .vec raster, return a metadata + handle list
#   vec_read_window    decode a window of a chosen band into a matrix
#   vec_extract_points sample band values at (x, y) points
#
# The C side accepts the storage dtype as a string ("f32", "i16", ...) and
# casts R doubles into that dtype on write. On read, every dtype is widened
# to R double; nodata pixels become NA_real_.

#' Write a raster matrix or 3D array to a .vec raster file
#'
#' Writes a row-major raster (one band) or a band-major 3D array (multi-band)
#' to the VECR raster format. Each tile is encoded as a self-describing tdc
#' block (PRED_2D + BYTE_SHUFFLE + LZ).
#'
#' @param x A numeric matrix `c(rows, cols)` for a single band, or a numeric
#'   3D array `c(rows, cols, bands)` for multi-band.
#' @param path Output file path.
#' @param dtype Storage dtype, one of `"f64"`, `"f32"`, `"i8"`, `"u8"`,
#'   `"i16"`, `"u16"`, `"i32"`, `"u32"`, `"i64"`, `"u64"`. Defaults to `"f32"`
#'   for floating-point input — `"f64"` doubles file size with no information
#'   gain for typical climate rasters.
#' @param tile_size Square tile edge in pixels. Default 512.
#' @param extent Numeric vector `c(xmin, ymin, xmax, ymax)`. Used together with
#'   the raster dimensions to derive the geotransform. Either `extent` or
#'   `gt` must be supplied for georeferenced output.
#' @param gt Numeric(6) GDAL-style geotransform. Overrides `extent` if both
#'   are given.
#' @param epsg EPSG code (integer) or 0L for none.
#' @param nodata Nodata value, or `NA_real_` to skip recording one.
#' @param band_names Optional character vector of length equal to the number
#'   of bands.
#' @param compression Compression effort, one of `"fast"` (single spec, fast
#'   encode), `"balanced"` (probe two entropy coders, ~2x encode time), or
#'   `"max"` (probe six candidate specs per tile, slowest encode but
#'   smallest file). Decode cost is unchanged across levels because each
#'   tile records its own codec spec. Default `"fast"`.
#' @return Invisible `NULL`.
#' @export
vec_write_raster <- function(x, path,
                             dtype       = "f32",
                             tile_size   = 512L,
                             extent      = NULL,
                             gt          = NULL,
                             epsg        = 0L,
                             nodata      = NA_real_,
                             band_names  = NULL,
                             compression = c("fast", "balanced", "max")) {

  if (!is.character(path) || length(path) != 1L)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)

  if (!is.numeric(x))
    stop("x must be numeric")

  d <- dim(x)
  if (is.null(d) || length(d) < 2L || length(d) > 3L)
    stop("x must be a numeric matrix or 3D array (rows, cols[, bands])")

  rows <- d[1L]
  cols <- d[2L]
  n_bands <- if (length(d) == 3L) d[3L] else 1L

  width  <- as.integer(cols)
  height <- as.integer(rows)

  ## Geotransform derivation. R matrices are arranged with row 1 at the top
  ## of the y-axis (north), matching GeoTIFF convention.
  if (is.null(gt)) {
    if (is.null(extent)) {
      gt <- c(0, 1, 0, 0, 0, 1)  # identity
    } else {
      if (!is.numeric(extent) || length(extent) != 4L)
        stop("extent must be c(xmin, ymin, xmax, ymax)")
      xmin <- extent[1L]; ymin <- extent[2L]
      xmax <- extent[3L]; ymax <- extent[4L]
      xres <- (xmax - xmin) / cols
      yres <- (ymax - ymin) / rows
      gt <- c(xmin, xres, 0, ymax, 0, -yres)
    }
  } else {
    if (!is.numeric(gt) || length(gt) != 6L)
      stop("gt must be a numeric vector of length 6")
  }

  if (!is.null(band_names)) {
    if (!is.character(band_names) || length(band_names) != n_bands)
      stop(sprintf("band_names must be character of length %d", n_bands))
  }

  ## R matrices are column-major; the C side expects band-major then
  ## row-major within each band. Convert via aperm for 3D arrays; for
  ## matrices, t() suffices.
  if (length(d) == 2L) {
    data_vec <- as.vector(t(x))                 # row-major, single band
  } else {
    ## permute so the result is c(cols, rows, bands), then as.vector gives
    ## band-major + within each band cols-vary-fastest = row-major.
    data_vec <- as.vector(aperm(x, c(2L, 1L, 3L)))
  }
  storage.mode(data_vec) <- "double"

  compression <- match.arg(compression)
  comp_code <- switch(compression,
                      fast     = 0L,
                      balanced = 1L,
                      max      = 2L)

  .Call(C_vec_write_raster,
        path,
        data_vec,
        c(width, height, as.integer(n_bands)),
        as.character(dtype),
        as.integer(tile_size),
        as.numeric(gt),
        as.integer(epsg),
        as.numeric(nodata),
        band_names,
        comp_code)

  invisible(NULL)
}

#' Open a .vec raster
#'
#' Lazy open: parses the header and tile index but does not decode any
#' tiles. Returns a list with metadata and an external pointer handle.
#' The pointer is auto-finalized when garbage collected; call
#' `vec_close_raster()` to release earlier.
#'
#' @param path Path to a `.vec` raster file.
#' @return A `vectra_raster` list with elements:
#'   `ptr`, `width`, `height`, `n_bands`, `tile_size`, `dtype`, `gt`,
#'   `epsg`, `nodata`, `band_names`.
#' @export
vec_open_raster <- function(path) {
  if (!is.character(path) || length(path) != 1L)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  out <- .Call(C_vec_open_raster, path)
  structure(out, class = "vectra_raster")
}

#' Close a .vec raster handle
#'
#' Idempotent. The handle is also auto-released by R's garbage collector.
#' @param r A `vectra_raster` returned by `vec_open_raster()`.
#' @return Invisible `NULL`.
#' @export
vec_close_raster <- function(r) {
  if (!inherits(r, "vectra_raster"))
    stop("r must be a vectra_raster")
  .Call(C_vec_close_raster, r$ptr)
  invisible(NULL)
}

#' Read a window of pixels from a .vec raster
#'
#' Decodes only the tiles overlapping the requested window. Pixels outside
#' the raster extent come back as `NA`.
#'
#' @param r A `vectra_raster` from `vec_open_raster()`.
#' @param band Band index (1-based). Default 1.
#' @param level Overview level — 0 = full resolution, 1 = half, 2 =
#'   quarter, etc. Must be < `r$n_levels` (which is 1 unless
#'   `vec_build_overviews()` has been run on the file).
#' @param cols 1-based column range `c(col_min, col_max)`. Inclusive.
#'   Coordinates are in the chosen level's pixel grid (so at level 1 the
#'   raster is half as wide). Default `c(1, level_width)`.
#' @param rows 1-based row range `c(row_min, row_max)`. Inclusive.
#'   Default `c(1, level_height)`.
#' @return A numeric matrix with `nrow = row_max - row_min + 1` and
#'   `ncol = col_max - col_min + 1`. Nodata pixels become `NA`.
#' @export
vec_read_window <- function(r,
                            band = 1L,
                            level = 0L,
                            cols = NULL,
                            rows = NULL) {
  if (!inherits(r, "vectra_raster"))
    stop("r must be a vectra_raster")
  level <- as.integer(level)
  level_width  <- max(1L, as.integer(ceiling(r$width  / 2^level)))
  level_height <- max(1L, as.integer(ceiling(r$height / 2^level)))
  if (is.null(cols)) cols <- c(1L, level_width)
  if (is.null(rows)) rows <- c(1L, level_height)
  if (length(cols) != 2L || length(rows) != 2L)
    stop("cols and rows must each be length 2")

  .Call(C_vec_read_window,
        r$ptr,
        as.integer(band),
        as.integer(level),
        as.integer(cols[1L]), as.integer(rows[1L]),
        as.integer(cols[2L]), as.integer(rows[2L]))
}

#' Extract band values at (x, y) points from a .vec raster
#'
#' @param r A `vectra_raster` from `vec_open_raster()`.
#' @param x Numeric vector of x coordinates in CRS units.
#' @param y Numeric vector of y coordinates, same length as `x`.
#' @return A `data.frame` with columns `x`, `y`, then one column per band
#'   (named after `r$band_names` if recorded, otherwise `band1`, `band2`,
#'   ...). NA marks pixels outside the raster or matching nodata.
#' @export
vec_extract_points <- function(r, x, y) {
  if (!inherits(r, "vectra_raster"))
    stop("r must be a vectra_raster")
  if (length(x) != length(y))
    stop("x and y must have the same length")
  .Call(C_vec_extract_points,
        r$ptr,
        as.numeric(x), as.numeric(y))
}

#' Build overview pyramids for a .vec raster
#'
#' Appends `n_levels - 1` reduced-resolution copies of the raster to the
#' file. Each level is computed by 2x downsampling the previous level
#' with the chosen kernel. Reading via `vec_read_window(level = L)`
#' picks tiles at level L; the file's `n_levels` is updated in place.
#'
#' @param path Path to a `.vec` raster file. The file is modified in place.
#' @param levels Total levels including level 0 (so `levels = 5` adds
#'   four overviews: levels 1..4). Must be in `[2, 16]`.
#' @param resampling One of `"nearest"`, `"average"`, `"bilinear"`,
#'   `"mode"`, `"gauss"`. `"average"` is the right choice for continuous
#'   rasters; `"mode"` for categorical/land-cover.
#' @param compression Compression effort for the new tiles. Defaults to
#'   `"fast"` because overview tiles are usually one-shot writes.
#' @return Invisible `NULL`.
#' @export
vec_build_overviews <- function(path,
                                levels,
                                resampling  = c("average", "nearest",
                                                "bilinear", "mode", "gauss"),
                                compression = c("fast", "balanced", "max")) {
  if (!is.character(path) || length(path) != 1L)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  if (!is.numeric(levels) || length(levels) != 1L || levels < 2L)
    stop("levels must be a single integer >= 2")

  resampling <- match.arg(resampling)
  compression <- match.arg(compression)
  comp_code <- switch(compression, fast = 0L, balanced = 1L, max = 2L)

  .Call(C_vec_build_overviews,
        path,
        as.integer(levels),
        resampling,
        comp_code)
  invisible(NULL)
}

#' Export a .vec raster to GeoTIFF
#'
#' Writes the level-0 pixels of a `.vec` raster to a GeoTIFF file. The
#' TIFF inherits dtype, geotransform, EPSG, and nodata from the source.
#' Strip layout with optional DEFLATE compression — LZW, tiled, and
#' BigTIFF support land in a follow-up commit.
#'
#' @param r Either a path to a `.vec` raster or a `vectra_raster` returned
#'   by `vec_open_raster()`. If a handle is passed it is left open.
#' @param path Output `.tif` path.
#' @param compression One of `"none"` or `"deflate"` (default).
#' @return Invisible `NULL`.
#' @export
vec_to_tiff <- function(r, path,
                        compression = c("deflate", "none")) {
  if (!is.character(path) || length(path) != 1L)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = FALSE)

  vec_path <- if (inherits(r, "vectra_raster")) {
    ## Reach into the externalptr's resolved file path. The reader doesn't
    ## currently expose its source path, so require a string for now.
    stop("vec_to_tiff currently requires the source path; pass it directly")
  } else if (is.character(r) && length(r) == 1L) {
    normalizePath(r, mustWork = TRUE)
  } else {
    stop("r must be a .vec path or a vectra_raster")
  }

  compression <- match.arg(compression)
  use_deflate <- if (compression == "deflate") 1L else 0L

  .Call(C_vec_to_tiff, vec_path, path, use_deflate)
  invisible(NULL)
}

#' @export
print.vectra_raster <- function(x, ...) {
  cat("<vectra_raster>\n")
  cat(sprintf("  dimensions  : %d cols x %d rows x %d bands\n",
              x$width, x$height, x$n_bands))
  cat(sprintf("  dtype       : %s\n", x$dtype))
  cat(sprintf("  tile_size   : %d\n", x$tile_size))
  cat(sprintf("  geotransform: [%.6g, %.6g, %.6g, %.6g, %.6g, %.6g]\n",
              x$gt[1], x$gt[2], x$gt[3], x$gt[4], x$gt[5], x$gt[6]))
  if (!is.null(x$n_levels) && x$n_levels > 1L)
    cat(sprintf("  n_levels    : %d\n", x$n_levels))
  if (x$epsg > 0L) cat(sprintf("  epsg        : %d\n", x$epsg))
  if (!is.na(x$nodata)) cat(sprintf("  nodata      : %g\n", x$nodata))
  if (!is.null(x$band_names))
    cat(sprintf("  band_names  : %s\n", paste(x$band_names, collapse = ", ")))
  invisible(x)
}
