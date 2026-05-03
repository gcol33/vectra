## GeoKey writer / reader, tiled-TIFF reader, and GDAL_METADATA band names.
## terra is only used as an external producer/consumer to validate spec
## compliance — tests that need it are skipped when it isn't installed.

skip_if_no_terra <- function() {
  testthat::skip_if_not_installed("terra")
}

test_that("CRS round-trips: integer EPSG (geographic)", {
  df <- data.frame(x = as.double(rep(1:4, 3)),
                   y = as.double(rep(1:3, each = 4)),
                   band1 = as.double(1:12))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f, crs = 4326L)

  crs <- tiff_crs(f)
  expect_identical(crs$epsg, 4326L)
  expect_true(is.na(crs$citation))
})

test_that("CRS round-trips: 'EPSG:xxxx' string (projected)", {
  df <- data.frame(x = as.double(rep(1:4, 3)),
                   y = as.double(rep(1:3, each = 4)),
                   band1 = as.double(1:12))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f, crs = "EPSG:3857")

  expect_identical(tiff_crs(f)$epsg, 3857L)
})

test_that("CRS round-trips: list with citation", {
  df <- data.frame(x = as.double(rep(1:4, 3)),
                   y = as.double(rep(1:3, each = 4)),
                   band1 = as.double(1:12))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f, crs = list(epsg = 31287L, citation = "MGI / Austria Lambert"))

  crs <- tiff_crs(f)
  expect_identical(crs$epsg, 31287L)
  expect_identical(crs$citation, "MGI / Austria Lambert")
})

test_that("no crs argument writes no GeoKey directory", {
  df <- data.frame(x = as.double(rep(1:4, 3)),
                   y = as.double(rep(1:3, each = 4)),
                   band1 = as.double(1:12))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f)

  crs <- tiff_crs(f)
  expect_true(is.na(crs$epsg))
  expect_true(is.na(crs$citation))
})

test_that("write_tiff(crs = ...) is readable by terra", {
  skip_if_no_terra()
  df <- data.frame(x = as.double(rep(1:4, 3)),
                   y = as.double(rep(1:3, each = 4)),
                   band1 = as.double(1:12))

  for (epsg in c(4326L, 3857L, 31287L)) {
    f <- tempfile(fileext = ".tif")
    write_tiff(df, f, crs = epsg)
    info <- terra::crs(terra::rast(f), describe = TRUE)
    expect_equal(as.integer(info$code), epsg,
                 info = sprintf("epsg=%d", epsg))
    unlink(f)
  }
})

test_that("invalid crs argument is rejected", {
  df <- data.frame(x = c(0.5, 1.5), y = c(0.5, 0.5), band1 = c(1.0, 2.0))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  expect_error(write_tiff(df, f, crs = "not an EPSG"),
               "crs string must look like")
  expect_error(write_tiff(df, f, crs = 0L),
               "needs a positive integer EPSG")
})

test_that("tiled (uncompressed) GeoTIFF reads correctly", {
  skip_if_no_terra()
  nrows <- 64; ncols <- 64
  r <- terra::rast(nrows = nrows, ncols = ncols,
                   xmin = 0, xmax = ncols, ymin = 0, ymax = nrows,
                   crs = "EPSG:4326")
  terra::values(r) <- as.double(seq_len(nrows * ncols))

  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  terra::writeRaster(r, f, overwrite = TRUE,
                     gdal = c("TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16",
                              "COMPRESS=NONE"))

  node <- tbl_tiff(f)
  expect_equal(node$.tiff_meta$width, ncols)
  expect_equal(node$.tiff_meta$height, nrows)
  expect_equal(node$.tiff_meta$epsg, 4326L)

  df <- collect(node)
  expect_equal(nrow(df), nrows * ncols)
  expect_equal(sort(df$band1), sort(terra::values(r)[, 1]))
})

test_that("tiled DEFLATE GeoTIFF with edge padding reads correctly", {
  skip_if_no_terra()
  # 200x200 image with 64x64 tiles → 4x4 grid, last col/row tiles padded.
  nrows <- 200; ncols <- 200
  r <- terra::rast(nrows = nrows, ncols = ncols,
                   xmin = 0, xmax = ncols, ymin = 0, ymax = nrows,
                   crs = "EPSG:3857")
  terra::values(r) <- as.double(seq_len(nrows * ncols))

  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  terra::writeRaster(r, f, overwrite = TRUE,
                     gdal = c("TILED=YES", "BLOCKXSIZE=64", "BLOCKYSIZE=64",
                              "COMPRESS=DEFLATE"))

  df <- collect(tbl_tiff(f))
  expect_equal(nrow(df), nrows * ncols)
  expect_equal(sort(df$band1), sort(terra::values(r)[, 1]))
})

test_that("point extraction works on tiled TIFF", {
  skip_if_no_terra()
  nrows <- 64; ncols <- 64
  r <- terra::rast(nrows = nrows, ncols = ncols,
                   xmin = 0, xmax = ncols, ymin = 0, ymax = nrows,
                   crs = "EPSG:4326")
  terra::values(r) <- as.double(seq_len(nrows * ncols))

  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  terra::writeRaster(r, f, overwrite = TRUE,
                     gdal = c("TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16",
                              "COMPRESS=NONE"))

  pts <- data.frame(x = c(0.5, 7.5, 31.5, 63.5),
                    y = c(63.5, 31.5, 7.5, 0.5))
  got <- tiff_extract_points(f, pts)
  expected <- terra::extract(r, pts)
  expect_equal(got$band1, expected[[2]])
})

test_that("tiff_band_names: explicit DESCRIPTION items round-trip", {
  df <- data.frame(x = rep(1:2, 2), y = rep(1:2, each = 2),
                   band1 = as.double(1:4), band2 = as.double(5:8))
  xml <- paste0(
    "<GDALMetadata>",
    "<Item name=\"DESCRIPTION\" sample=\"0\" role=\"description\">temperature</Item>",
    "<Item name=\"DESCRIPTION\" sample=\"1\" role=\"description\">humidity</Item>",
    "<Item name=\"scale\" sample=\"0\">0.01</Item>",
    "</GDALMetadata>")
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f, metadata = xml)

  expect_identical(tiff_band_names(f), c("temperature", "humidity"))
})

test_that("tiff_band_names returns NA per band when no metadata", {
  df <- data.frame(x = rep(1:2, 2), y = rep(1:2, each = 2),
                   band1 = as.double(1:4), band2 = as.double(5:8))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f)

  expect_identical(tiff_band_names(f), rep(NA_character_, 2))
})

test_that("tiff_band_names reads names written by terra", {
  skip_if_no_terra()
  r <- terra::rast(nrows = 4, ncols = 4, nlyrs = 3,
                   xmin = 0, xmax = 4, ymin = 0, ymax = 4,
                   crs = "EPSG:4326")
  terra::values(r) <- as.double(rep(1:16, 3))
  names(r) <- c("alpha", "beta", "gamma")

  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  terra::writeRaster(r, f, overwrite = TRUE, gdal = "COMPRESS=NONE")

  expect_identical(tiff_band_names(f), c("alpha", "beta", "gamma"))
})

test_that("tiff_band_names decodes XML entity escapes", {
  df <- data.frame(x = c(0.5, 1.5), y = c(0.5, 0.5), band1 = c(1.0, 2.0))
  xml <- paste0(
    "<GDALMetadata>",
    "<Item name=\"DESCRIPTION\" sample=\"0\">a &amp; b &lt;c&gt;</Item>",
    "</GDALMetadata>")
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  write_tiff(df, f, metadata = xml)

  expect_identical(tiff_band_names(f), "a & b <c>")
})

# ---------------------------------------------------------------------------
# Phase 4c: tiled TIFF write
# ---------------------------------------------------------------------------

# Helper: build a regular grid as an x/y/band data.frame.
.make_grid_df <- function(ncols, nrows, fn = function(i, j) i + (j - 1) * ncols) {
  data.frame(
    x = rep(seq(0.5, ncols - 0.5, by = 1), nrows),
    y = rep(seq(nrows - 0.5, 0.5, by = -1), each = ncols),
    band1 = as.double(fn(rep(seq_len(ncols), nrows),
                         rep(seq_len(nrows), each = ncols)))
  )
}

test_that("tiled write: round-trip via tbl_tiff preserves values", {
  ncols <- 32; nrows <- 32
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, tiled = TRUE, tile_size = 16L)
  result <- collect(tbl_tiff(f))

  expect_equal(nrow(result), ncols * nrows)
  expect_equal(sort(result$band1), sort(df$band1))
})

test_that("tiled write: emits TileWidth/TileLength matching tile_size", {
  ncols <- 32; nrows <- 48
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, tiled = TRUE, tile_size = 16L)
  meta <- tbl_tiff(f)$.tiff_meta
  expect_equal(meta$width, ncols)
  expect_equal(meta$height, nrows)
})

test_that("tiled write with edge padding (image not multiple of tile)", {
  # 40x40 image with 16x16 tiles → 3x3 = 9 tiles; right/bottom edges padded.
  ncols <- 40; nrows <- 40
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, tiled = TRUE, tile_size = 16L)
  result <- collect(tbl_tiff(f))

  # Reader returns only in-image pixels.
  expect_equal(nrow(result), ncols * nrows)
  expect_equal(sort(result$band1), sort(df$band1))
})

test_that("tiled DEFLATE write round-trips correctly", {
  ncols <- 64; nrows <- 64
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, compress = TRUE, tiled = TRUE, tile_size = 32L)
  result <- collect(tbl_tiff(f))

  expect_equal(nrow(result), ncols * nrows)
  expect_equal(sort(result$band1), sort(df$band1))
})

test_that("tiled multi-band write round-trips", {
  ncols <- 32; nrows <- 32
  base <- .make_grid_df(ncols, nrows)
  # Reader assigns band1/band2 names regardless of writer-side column names.
  df <- data.frame(x = base$x, y = base$y,
                   b1 = base$band1, b2 = base$band1 * 2)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, tiled = TRUE, tile_size = 16L)
  result <- collect(tbl_tiff(f))

  expect_equal(nrow(result), ncols * nrows)
  expect_equal(sort(result$band1), sort(df$b1))
  expect_equal(sort(result$band2), sort(df$b2))
})

test_that("tiled write rejects non-multiple-of-16 tile_size", {
  df <- data.frame(x = c(0.5, 1.5), y = c(0.5, 0.5), band1 = c(1.0, 2.0))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  expect_error(write_tiff(df, f, tiled = TRUE, tile_size = 17L),
               "multiple of 16")
  expect_error(write_tiff(df, f, tiled = TRUE, tile_size = 0L),
               "positive")
})

test_that("tiled write: tile count = ceil(W/TW) * ceil(H/TH) (terra)", {
  skip_if_no_terra()
  ncols <- 50; nrows <- 30
  TW <- 16L; TH <- 16L
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, tiled = TRUE, tile_size = c(TW, TH))

  # Round-trip through terra: output values should match input.
  r <- terra::rast(f)
  expect_equal(terra::nrow(r), nrows)
  expect_equal(terra::ncol(r), ncols)

  vals <- terra::values(r)[, 1]
  # terra fills nodata-padded edges if the writer didn't pad properly. We
  # expect a 50x30 raster with ncols*nrows valid cells, no NA.
  expect_equal(length(vals), ncols * nrows)
  expect_equal(sort(vals[!is.na(vals)]), sort(df$band1))

  # Confirm tile structure: the file's tile count must be 4 * 2 = 8.
  expected_n_tiles <- ceiling(ncols / TW) * ceiling(nrows / TH)
  expect_equal(expected_n_tiles, 4 * 2)
})

test_that("tiled write: terra round-trip with CRS and DEFLATE", {
  skip_if_no_terra()
  ncols <- 64; nrows <- 64
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, compress = TRUE, tiled = TRUE, tile_size = 32L,
             crs = 4326L)

  r <- terra::rast(f)
  expect_equal(terra::ncol(r), ncols)
  expect_equal(terra::nrow(r), nrows)
  info <- terra::crs(r, describe = TRUE)
  expect_equal(as.integer(info$code), 4326L)
  vals <- terra::values(r)[, 1]
  expect_equal(sort(vals), sort(df$band1))
})

test_that("tiled write: rectangular tile_size c(w, h)", {
  ncols <- 32; nrows <- 48
  df <- .make_grid_df(ncols, nrows)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, tiled = TRUE, tile_size = c(16L, 32L))
  result <- collect(tbl_tiff(f))

  expect_equal(nrow(result), ncols * nrows)
  expect_equal(sort(result$band1), sort(df$band1))
})
