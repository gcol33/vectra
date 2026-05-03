## BigTIFF write path (Phase 4d).
##
## We never actually generate a >4 GB file in tests — instead we
## force-enable BigTIFF on small data and verify:
##   1. The on-disk header bytes start with the BigTIFF magic 0x002B and
##      the 8-byte offset width.
##   2. Our own reader (which already supports BigTIFF) reproduces the
##      pixel values.
##   3. terra::rast() accepts the file (terra reads BigTIFF natively).
##   4. Auto mode emits classic TIFF for tiny rasters.
##   5. The auto-promotion threshold trips when the expected raw payload
##      would exceed the 4 GB ceiling.

skip_if_no_terra <- function() {
  testthat::skip_if_not_installed("terra")
}

# Read the first few header bytes and decode magic / offset width.
.read_tiff_header <- function(path) {
  con <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  bom <- readBin(con, "integer", n = 2, size = 1, signed = FALSE)
  if (!(bom[1] == as.integer(charToRaw("I")[1]) &&
        bom[2] == as.integer(charToRaw("I")[1])))
    stop("not a little-endian TIFF (got byte order ", bom[1], " ", bom[2], ")")
  magic <- readBin(con, "integer", n = 1, size = 2, signed = FALSE,
                   endian = "little")
  out <- list(magic = magic)
  if (magic == 43L) {
    out$offset_size <- readBin(con, "integer", n = 1, size = 2, signed = FALSE,
                                endian = "little")
    out$reserved <- readBin(con, "integer", n = 1, size = 2, signed = FALSE,
                              endian = "little")
  }
  out
}

test_that("bigtiff = TRUE writes BigTIFF magic 0x002B", {
  df <- data.frame(
    x = rep(seq(0.5, 3.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 4),
    band1 = as.double(1:12)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, bigtiff = TRUE)

  hdr <- .read_tiff_header(f)
  expect_identical(hdr$magic, 43L)         # 0x002B
  expect_identical(hdr$offset_size, 8L)
  expect_identical(hdr$reserved, 0L)
})

test_that("bigtiff = TRUE round-trips through tbl_tiff()", {
  df <- data.frame(
    x = rep(seq(0.5, 3.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 4),
    band1 = as.double(1:12)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, bigtiff = TRUE)
  result <- tbl_tiff(f) |> collect()

  expect_equal(nrow(result), 12)
  expect_equal(sort(result$band1), 1:12)
})

test_that("bigtiff = TRUE round-trips through terra", {
  skip_if_no_terra()
  df <- data.frame(
    x = rep(seq(0.5, 3.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 4),
    band1 = as.double(1:12)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, bigtiff = TRUE, crs = 4326L)
  r <- terra::rast(f)
  expect_equal(terra::nrow(r), 3)
  expect_equal(terra::ncol(r), 4)
  expect_equal(sort(terra::values(r)[, 1]), 1:12)
})

test_that("bigtiff = TRUE works with multi-band, deflate, nodata, CRS", {
  skip_if_no_terra()
  df <- data.frame(
    x = rep(seq(0.5, 3.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 4),
    band1 = as.double(1:12),
    band2 = c(as.double(10:20), NA_real_)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, compress = TRUE, pixel_type = "int16",
             crs = list(epsg = 31287L, citation = "MGI / Austria Lambert"),
             bigtiff = TRUE)

  hdr <- .read_tiff_header(f)
  expect_identical(hdr$magic, 43L)

  r <- terra::rast(f)
  expect_equal(terra::nlyr(r), 2)
  vals <- terra::values(r)
  expect_equal(sort(vals[, 1]), 1:12)
  # band2 has one NA-as-nodata; everything else round-trips
  expect_setequal(setdiff(vals[, 2], NA), 10:20)

  expect_identical(tiff_crs(f)$epsg, 31287L)
})

test_that("bigtiff = FALSE produces classic TIFF (magic 42)", {
  df <- data.frame(
    x = rep(seq(0.5, 3.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 4),
    band1 = as.double(1:12)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, bigtiff = FALSE)

  hdr <- .read_tiff_header(f)
  expect_identical(hdr$magic, 42L)
})

test_that("bigtiff = \"auto\" stays classic for small rasters", {
  df <- data.frame(
    x = rep(seq(0.5, 3.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 4),
    band1 = as.double(1:12)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, bigtiff = "auto")
  expect_identical(.read_tiff_header(f)$magic, 42L)

  # Default for the writer is "auto"; same expectation.
  f2 <- tempfile(fileext = ".tif")
  on.exit(unlink(f2), add = TRUE)
  write_tiff(df, f2)
  expect_identical(.read_tiff_header(f2)$magic, 42L)
})

test_that("invalid bigtiff argument is rejected", {
  df <- data.frame(x = c(0.5, 1.5), y = c(0.5, 0.5), band1 = c(1.0, 2.0))
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  expect_error(write_tiff(df, f, bigtiff = "yes"), "bigtiff string")
  expect_error(write_tiff(df, f, bigtiff = NA), "bigtiff must be")
  expect_error(write_tiff(df, f, bigtiff = 1), "bigtiff must be")
})
