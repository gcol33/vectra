# Recovery and streaming-invariance tests for proximity().
#
# Ground truth is the brute-force Euclidean distance from every cell centre to
# the nearest feature cell centre, and terra::distance where terra exists.
# proximity must reproduce both and stay equal across tile sizes.

wr <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

# Brute-force distance from each cell centre to the nearest TRUE cell centre.
# `feat` is an H x W logical matrix (row 1 north); xres/yres are the cell sizes.
brute_prox <- function(feat, xres, yres) {
  H <- nrow(feat); W <- ncol(feat)
  fi <- which(feat, arr.ind = TRUE)
  out <- matrix(NA_real_, H, W)
  if (!nrow(fi)) return(out)
  for (i in seq_len(H)) for (j in seq_len(W)) {
    dx <- (j - fi[, 2L]) * xres
    dy <- (i - fi[, 1L]) * yres
    out[i, j] <- sqrt(min(dx * dx + dy * dy))
  }
  out
}

test_that("proximity matches brute-force Euclidean distance", {
  H <- 12L; W <- 12L
  m <- matrix(NA_real_, H, W)
  feats <- rbind(c(2L, 3L), c(7L, 9L), c(11L, 2L))
  for (k in seq_len(nrow(feats))) m[feats[k, 1L], feats[k, 2L]] <- 1
  f <- wr(m, ext = c(0, 0, W, H), tile = 4L)
  on.exit(unlink(f))

  d <- proximity(f)
  truth <- brute_prox(!is.na(m), 1, 1)
  expect_equal(d, truth, ignore_attr = TRUE, tolerance = 1e-6)
})

test_that("proximity scales x and y by resolution (anisotropic)", {
  H <- 10L; W <- 10L
  m <- matrix(NA_real_, H, W); m[5L, 5L] <- 1
  # extent makes 2:1 cells: xres = 2, yres = 1.
  f <- wr(m, ext = c(0, 0, 2 * W, H), tile = 4L)
  on.exit(unlink(f))

  d <- proximity(f)
  truth <- brute_prox(!is.na(m), 2, 1)
  expect_equal(d, truth, ignore_attr = TRUE, tolerance = 1e-6)
})

test_that("target selects which values are features", {
  m <- matrix(0, 6L, 6L); m[2L, 2L] <- 7; m[5L, 5L] <- 7; m[1L, 6L] <- 3
  f <- wr(m, ext = c(0, 0, 6, 6), tile = 3L)
  on.exit(unlink(f))

  d <- proximity(f, target = 7)
  truth <- brute_prox(m == 7, 1, 1)
  expect_equal(d, truth, ignore_attr = TRUE, tolerance = 1e-6)
})

test_that("proximity matches terra::distance cell-for-cell", {
  skip_if_not_installed("terra")
  set.seed(3)
  H <- 15L; W <- 15L
  m <- matrix(NA_real_, H, W)
  m[sample(H * W, 5L)] <- 1
  f <- wr(m, ext = c(0, 0, W, H), tile = 5L)
  on.exit(unlink(f))

  d <- proximity(f)
  # A projected CRS keeps terra::distance planar-Euclidean (lon/lat is geodesic).
  tr <- terra::rast(m, extent = terra::ext(0, W, 0, H), crs = "EPSG:32633")
  tdv <- matrix(terra::values(terra::distance(tr)), H, W, byrow = TRUE)
  expect_equal(d, tdv, ignore_attr = TRUE, tolerance = 1e-6)
})

test_that("proximity is invariant to tile size (streaming invariance)", {
  set.seed(5)
  H <- 16L; W <- 16L
  m <- matrix(NA_real_, H, W)
  m[cbind(sample(H, 4L), sample(W, 4L))] <- 1
  f1 <- wr(m, ext = c(0, 0, W, H), tile = 4L)
  f2 <- wr(m, ext = c(0, 0, W, H), tile = 16L)
  on.exit(unlink(c(f1, f2)))

  expect_equal(proximity(f1), proximity(f2))
})

test_that("proximity streams to a .vec and round-trips", {
  H <- 12L; W <- 12L
  m <- matrix(NA_real_, H, W); m[3L, 4L] <- 1; m[9L, 10L] <- 1
  f <- wr(m, ext = c(0, 0, W, H), tile = 4L)
  out <- tempfile(fileext = ".vec")
  on.exit(unlink(c(f, out)))

  h <- proximity(f, path = out, dtype = "f64")
  expect_s3_class(h, "vectra_raster")
  got <- vec_read_window(h)
  vec_close_raster(h)
  truth <- brute_prox(!is.na(m), 1, 1)
  expect_equal(got, truth, ignore_attr = TRUE, tolerance = 1e-6)
})

test_that("no feature anywhere yields all NA", {
  m <- matrix(NA_real_, 8L, 8L)
  f <- wr(m, ext = c(0, 0, 8, 8), tile = 4L)
  on.exit(unlink(f))
  expect_true(all(is.na(proximity(f))))

  m2 <- matrix(5, 8L, 8L)
  f2 <- wr(m2, ext = c(0, 0, 8, 8), tile = 4L)
  on.exit(unlink(f2), add = TRUE)
  expect_true(all(is.na(proximity(f2, target = 999))))
})
