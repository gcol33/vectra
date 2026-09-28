# A band column from tbl_tiff, which emits pixels in storage (row-major) order.
.tiff_band <- function(path, band = 1L) {
  collect(tbl_tiff(path))[[paste0("band", band)]]
}

.vec_lzw <- function(x, dtype, ...) {
  vec_path  <- tempfile(fileext = ".vec")
  tiff_path <- tempfile(fileext = ".tif")
  vec_write_raster(x, vec_path, dtype = dtype, ...)
  vec_to_tiff(vec_path, tiff_path, compression = "lzw")
  unlink(vec_path)
  tiff_path
}

test_that("tbl_tiff reads an LZW file written by vec_to_tiff", {
  set.seed(1)
  m <- matrix(runif(200), 10, 20)
  t <- .vec_lzw(m, "f64")
  on.exit(unlink(t))
  expect_equal(.tiff_band(t), as.numeric(t(m)))
})

test_that("tbl_tiff undoes vectra's LZW + Predictor 2 for every integer width", {
  m <- matrix(seq_len(40 * 30), 30, 40)
  for (dt in c("u8", "i16", "u16", "i32")) {
    x <- if (dt == "u8") m %% 250L else m
    t <- .vec_lzw(x, dt)
    expect_equal(.tiff_band(t), as.numeric(t(x)), info = dt)
    unlink(t)
  }
})

test_that("tbl_tiff reads multi-band LZW + Predictor 2", {
  arr <- array(0L, dim = c(20, 25, 3))
  for (b in 1:3) arr[, , b] <- matrix(seq_len(20 * 25) * b, 20, 25)
  t <- .vec_lzw(arr, "i16")
  on.exit(unlink(t))
  for (b in 1:3)
    expect_equal(.tiff_band(t, b), as.numeric(t(arr[, , b])), info = paste("band", b))
})

test_that("LZW decoding survives code-width growth and dictionary resets", {
  set.seed(7)
  # High-entropy doubles force many distinct codes: widths 9..12 and
  # several ClearCode resets inside one strip.
  m <- matrix(rnorm(300 * 200), 300, 200)
  t <- .vec_lzw(m, "f64")
  on.exit(unlink(t))
  expect_equal(.tiff_band(t), as.numeric(t(m)))
})

test_that("tbl_tiff matches terra on GDAL-written LZW / predictor files", {
  skip_if_not_installed("terra")
  set.seed(3)
  nr <- 37; nc <- 53
  vals_f <- matrix(cumsum(rnorm(nr * nc)), nr, nc)
  vals_i <- matrix(as.integer(round(cumsum(rnorm(nr * nc, sd = 5)))), nr, nc)
  cases <- list(
    list(v = vals_f, dt = "FLT4S", p = 1), list(v = vals_f, dt = "FLT4S", p = 2),
    list(v = vals_f, dt = "FLT4S", p = 3), list(v = vals_f, dt = "FLT8S", p = 3),
    list(v = vals_i, dt = "INT2S", p = 2), list(v = vals_i, dt = "INT4S", p = 2),
    list(v = abs(vals_i) %% 200L, dt = "INT1U", p = 2),
    list(v = abs(vals_i), dt = "INT2U", p = 2)
  )
  for (cs in cases) {
    for (extra in list(character(), c("TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16"),
                       "ENDIANNESS=BIG")) {
      r <- terra::rast(nrows = nr, ncols = nc, xmin = 0, xmax = nc,
                       ymin = 0, ymax = nr, crs = "EPSG:4326")
      terra::values(r) <- as.numeric(t(cs$v))
      f <- tempfile(fileext = ".tif")
      terra::writeRaster(r, f, datatype = cs$dt, overwrite = TRUE,
                         gdal = c("COMPRESS=LZW", paste0("PREDICTOR=", cs$p),
                                  extra))
      info <- paste(cs$dt, "predictor", cs$p, paste(extra, collapse = " "))
      expect_equal(.tiff_band(f), as.numeric(terra::values(terra::rast(f))),
                   info = info)
      unlink(f)
    }
  }
})

test_that("tbl_tiff matches terra on multi-band GDAL LZW + predictor 3", {
  skip_if_not_installed("terra")
  set.seed(4)
  r <- terra::rast(nrows = 19, ncols = 23, nlyrs = 3, xmin = 0, xmax = 23,
                   ymin = 0, ymax = 19)
  terra::values(r) <- matrix(rnorm(19 * 23 * 3), ncol = 3)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))
  terra::writeRaster(r, f, datatype = "FLT4S", overwrite = TRUE,
                     gdal = c("COMPRESS=LZW", "PREDICTOR=3", "INTERLEAVE=PIXEL"))
  ref <- terra::values(terra::rast(f))
  for (b in 1:3)
    expect_equal(.tiff_band(f, b), as.numeric(ref[, b]), info = paste("band", b))
})

test_that("a corrupt LZW strip errors instead of returning pixels", {
  set.seed(9)
  m <- matrix(rnorm(64 * 64), 64, 64)
  t <- .vec_lzw(m, "f64")
  on.exit(unlink(t))
  raw <- readBin(t, "raw", file.info(t)$size)

  # Overwrite the strip payload (right after the 8-byte header) with 0xFF:
  # every 9-bit code is then 511, a code no dictionary has defined yet.
  bad <- raw
  bad[9:4000] <- as.raw(0xFF)
  f1 <- tempfile(fileext = ".tif")
  writeBin(bad, f1)
  expect_error(collect(tbl_tiff(f1)))

  # A stream that decodes to fewer bytes than the strip geometry.
  bad2 <- raw
  bad2[9:11] <- as.raw(c(0x80, 0x40, 0x40))   # ClearCode, then 9-bit EoiCode
  f2 <- tempfile(fileext = ".tif")
  writeBin(bad2, f2)
  expect_error(collect(tbl_tiff(f2)))

  # Old-style (LSB-first) LZW is refused.
  bad3 <- raw
  bad3[9:10] <- as.raw(c(0x00, 0x01))
  f3 <- tempfile(fileext = ".tif")
  writeBin(bad3, f3)
  expect_error(collect(tbl_tiff(f3)))
  unlink(c(f1, f2, f3))
})
