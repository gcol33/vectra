# Recovery and streaming-invariance tests for rast_calc().
#
# The reference is the same expression evaluated on the whole resident matrices.
# The streamed strip pass must reproduce it exactly and stay equal across tile
# sizes (a streamed path must equal the resident path).

wr <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

test_that("rast_calc recovers NDVI computed on resident matrices", {
  set.seed(1)
  nir <- matrix(runif(20 * 20, 10, 90), 20, 20)
  red <- matrix(runif(20 * 20, 5, 50), 20, 20)
  fn <- wr(nir, tile = 8L); fr <- wr(red, tile = 8L)
  on.exit(unlink(c(fn, fr)))

  got <- rast_calc(list(nir = fn, red = fr), (nir - red) / (nir + red))
  ref <- (nir - red) / (nir + red)
  expect_equal(unclass(got), unclass(ref), tolerance = 1e-9, ignore_attr = TRUE)
})

test_that("rast_calc is invariant to tile size (streaming invariance)", {
  set.seed(2)
  a <- matrix(rnorm(48 * 48), 48, 48)
  fa_small <- wr(a, tile = 8L); fa_big <- wr(a, tile = 48L)
  on.exit(unlink(c(fa_small, fa_big)))

  small <- rast_calc(list(a = fa_small), a * 3 - 2)
  big   <- rast_calc(list(a = fa_big),   a * 3 - 2)
  expect_equal(small, big, tolerance = 1e-12)
})

test_that("rast_calc reclassify matches a resident cut()", {
  set.seed(3)
  dem <- matrix(runif(16 * 16, 0, 100), 16, 16)
  fd <- wr(dem, tile = 4L)
  on.exit(unlink(fd))
  brks <- c(-Inf, 25, 50, 75, Inf)
  got <- rast_calc(list(dem = fd), as.integer(cut(dem, brks)))
  ref <- matrix(as.integer(cut(as.vector(dem), brks)), 16, 16)
  expect_equal(unclass(got), unclass(ref), ignore_attr = TRUE)
})

test_that("rast_calc round-trips through a .vec sink", {
  m <- matrix(1:36, 6, 6, byrow = TRUE)
  fm <- wr(m, tile = 3L)
  out <- tempfile(fileext = ".vec")
  on.exit(unlink(c(fm, out)))
  h <- rast_calc(list(m = fm), m + 100, path = out)
  expect_s3_class(h, "vectra_raster")
  back <- vec_read_window(h, band = 1L)
  vec_close_raster(h)
  expect_equal(back, m + 100, ignore_attr = TRUE)
})

test_that("rast_calc rejects misaligned rasters and bad expressions", {
  a <- wr(matrix(1, 4, 4)); b <- wr(matrix(1, 5, 5))
  on.exit(unlink(c(a, b)))
  expect_error(rast_calc(list(a = a, b = b), a + b), "share dimensions")
  expect_error(rast_calc(list(matrix(1)), 1), "named list")
  expect_error(rast_calc(list(a = a), c(1, 2, 3)), "one value per cell")
})
