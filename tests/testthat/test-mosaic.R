# Recovery and streaming-invariance tests for mosaic().
#
# Ground truth is a hand-assembled union grid and terra::mosaic where available.
# The streamed strip pass must reproduce it and stay equal across tile sizes.

wr <- function(m, ext, tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

test_that("mosaic builds the union grid and resolves overlap by fun", {
  a <- matrix(1, 4, 4); b <- matrix(2, 4, 4)
  fa <- wr(a, c(0, 0, 4, 4), tile = 2L); fb <- wr(b, c(2, 2, 6, 6), tile = 2L)
  on.exit(unlink(c(fa, fb)))

  mm <- mosaic(list(fa, fb), fun = "mean")
  expect_equal(dim(mm), c(6L, 6L))
  # fa covers x[0,4] y[0,4]; fb covers x[2,6] y[2,6]; union is x[0,6] y[0,6].
  # overlap is the 2x2 block of target rows/cols 3:4 -> mean(1, 2).
  expect_equal(mm[3, 3], 1.5)
  expect_equal(mm[1, 6], 2)    # top-right (x[5,6] y[5,6]) is b only
  expect_equal(mm[6, 1], 1)    # bottom-left (x[0,1] y[0,1]) is a only
  expect_true(is.na(mm[1, 1])) # top-left covered by neither
  expect_true(is.na(mm[6, 6])) # bottom-right covered by neither
})

test_that("first/last/min/max/sum resolve overlap as specified", {
  a <- matrix(1, 4, 4); b <- matrix(2, 4, 4)
  fa <- wr(a, c(0, 0, 4, 4), tile = 2L); fb <- wr(b, c(2, 2, 6, 6), tile = 2L)
  on.exit(unlink(c(fa, fb)))

  expect_equal(mosaic(list(fa, fb), "first")[3, 3], 1)
  expect_equal(mosaic(list(fa, fb), "last")[3, 3], 2)
  expect_equal(mosaic(list(fa, fb), "min")[3, 3], 1)
  expect_equal(mosaic(list(fa, fb), "max")[3, 3], 2)
  expect_equal(mosaic(list(fa, fb), "sum")[3, 3], 3)
})

test_that("mosaic matches terra::mosaic cell-for-cell", {
  skip_if_not_installed("terra")
  set.seed(7)
  a <- matrix(rnorm(8 * 8), 8, 8); b <- matrix(rnorm(8 * 8), 8, 8)
  fa <- wr(a, c(0, 0, 8, 8), tile = 4L); fb <- wr(b, c(4, 4, 12, 12), tile = 4L)
  on.exit(unlink(c(fa, fb)))

  for (fn in c("mean", "sum", "min", "max")) {
    mm <- mosaic(list(fa, fb), fun = fn)
    ta <- terra::rast(a, extent = terra::ext(0, 8, 0, 8))
    tb <- terra::rast(b, extent = terra::ext(4, 12, 4, 12))
    tmo <- terra::mosaic(ta, tb, fun = fn)
    tmv <- matrix(terra::values(tmo), terra::nrow(tmo), terra::ncol(tmo),
                  byrow = TRUE)
    expect_equal(dim(mm), dim(tmv))
    expect_equal(mm, tmv, tolerance = 1e-9, ignore_attr = TRUE,
                 label = paste("mosaic fun =", fn))
  }
})

test_that("mosaic is invariant to tile size (streaming invariance)", {
  set.seed(8)
  a <- matrix(rnorm(12 * 12), 12, 12); b <- matrix(rnorm(12 * 12), 12, 12)
  fa1 <- wr(a, c(0, 0, 12, 12), tile = 3L); fb1 <- wr(b, c(6, 6, 18, 18), tile = 3L)
  fa2 <- wr(a, c(0, 0, 12, 12), tile = 12L); fb2 <- wr(b, c(6, 6, 18, 18), tile = 12L)
  on.exit(unlink(c(fa1, fb1, fa2, fb2)))

  expect_equal(mosaic(list(fa1, fb1), "mean"), mosaic(list(fa2, fb2), "mean"))
})

test_that("mosaic rejects rasters off a common grid", {
  fa <- wr(matrix(1, 4, 4), c(0, 0, 4, 4))
  fb <- wr(matrix(2, 4, 4), c(0.3, 0, 4.3, 4))   # half-cell shift
  on.exit(unlink(c(fa, fb)))
  expect_error(mosaic(list(fa, fb)), "common cell grid")
})
