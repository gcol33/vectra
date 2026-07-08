# mop(): MOP transferability surface (distance surface via feature_knn plus the
# non-analogous-conditions layers) aligned to the projection grid.
#
# Correctness is checked against a resident brute-force reference; the streamed
# .vec path must reproduce the in-memory path and stay invariant across tile
# sizes. An optional parity block cross-checks the distance surface against the
# mop package when it (and terra) are installed.

wr_bands <- function(bands, ext = c(0, 0, ncol(bands[[1]]), nrow(bands[[1]])),
                     tile = 512L, names = NULL) {
  R <- nrow(bands[[1]]); C <- ncol(bands[[1]])
  arr <- array(0, c(R, C, length(bands)))
  for (b in seq_along(bands)) arr[, , b] <- bands[[b]]
  f <- tempfile(fileext = ".vec")
  vec_write_raster(arr, f, dtype = "f64", extent = ext, tile_size = tile,
                   band_names = names)
  f
}

# Brute-force MOP distance surface on resident band matrices.
brute_mop_distance <- function(g_bands, m_bands, keff, transform = NULL) {
  q <- do.call(cbind, lapply(g_bands, as.vector))
  r <- do.call(cbind, lapply(m_bands, as.vector))
  ok_r <- stats::complete.cases(r); r <- r[ok_r, , drop = FALSE]
  if (!is.null(transform)) { q <- q %*% t(transform); r <- r %*% t(transform) }
  vapply(seq_len(nrow(q)), function(i) {
    if (anyNA(q[i, ])) return(NA_real_)
    d <- sqrt(colSums((t(r) - q[i, ])^2))
    mean(sort(d)[seq_len(keff)])
  }, numeric(1))
}

test_that("mop distance surface recovers the brute-force reference", {
  set.seed(1)
  m1 <- matrix(rnorm(20 * 20, 10, 2), 20, 20)
  m2 <- matrix(rnorm(20 * 20, 800, 40), 20, 20)
  g1 <- m1 + 3; g2 <- m2 - 60
  fm <- wr_bands(list(m1, m2), tile = 8L)
  fg <- wr_bands(list(g1, g2), tile = 8L)
  on.exit(unlink(c(fm, fg)))

  out <- mop(fg, fm, percentage = 10)
  keff <- ceiling(0.10 * 400)
  ref <- brute_mop_distance(list(g1, g2), list(m1, m2), keff)
  expect_equal(as.vector(out$mop_distance), ref, tolerance = 1e-9)
})

test_that("mop NAC layers count out-of-range predictors exactly", {
  set.seed(2)
  m1 <- matrix(rnorm(16 * 16, 0, 1), 16, 16)
  m2 <- matrix(rnorm(16 * 16, 0, 1), 16, 16)
  g1 <- m1 + rnorm(256, 0, 2)   # some cells fall outside the calibration range
  g2 <- m2 + rnorm(256, 0, 2)
  fm <- wr_bands(list(m1, m2), tile = 4L)
  fg <- wr_bands(list(g1, g2), tile = 4L)
  on.exit(unlink(c(fm, fg)))

  out <- mop(fg, fm, percentage = 20)
  vmin <- c(min(m1), min(m2)); vmax <- c(max(m1), max(m2))
  gv1 <- as.vector(g1); gv2 <- as.vector(g2)
  tlow  <- (gv1 < vmin[1]) + (gv2 < vmin[2])
  thigh <- (gv1 > vmax[1]) + (gv2 > vmax[2])

  expect_equal(as.vector(out$towards_low),  tlow)
  expect_equal(as.vector(out$towards_high), thigh)
  expect_equal(as.vector(out$mop_simple), tlow + thigh)
  expect_equal(as.vector(out$mop_basic), as.numeric(tlow + thigh > 0))
})

test_that("mop is invariant to tile size and the streamed path matches memory", {
  set.seed(3)
  m1 <- matrix(rnorm(24 * 24, 5, 1), 24, 24)
  m2 <- matrix(rnorm(24 * 24, 5, 1), 24, 24)
  g1 <- m1 + 1; g2 <- m2 - 1
  fm_s <- wr_bands(list(m1, m2), tile = 6L)
  fg_s <- wr_bands(list(g1, g2), tile = 6L)
  fm_b <- wr_bands(list(m1, m2), tile = 24L)
  fg_b <- wr_bands(list(g1, g2), tile = 24L)
  on.exit(unlink(c(fm_s, fg_s, fm_b, fg_b)))

  small <- mop(fg_s, fm_s, percentage = 15)
  big   <- mop(fg_b, fm_b, percentage = 15)
  expect_equal(small$mop_distance, big$mop_distance, tolerance = 1e-12)

  fout <- tempfile(fileext = ".vec"); on.exit(unlink(fout), add = TRUE)
  mop(fg_s, fm_s, percentage = 15, path = fout)
  rout <- vec_open_raster(fout)
  streamed <- as.vector(vec_read_window(rout, band = 1))
  vec_close_raster(rout)
  expect_equal(streamed, as.vector(small$mop_distance), tolerance = 1e-4)
})

test_that("mop mahalanobis matches the whitened brute force", {
  set.seed(4)
  A <- matrix(c(2, 1, 1, 2), 2, 2)
  base <- matrix(rnorm(18 * 18 * 2), 18 * 18, 2) %*% chol(A)
  m1 <- matrix(base[, 1], 18, 18); m2 <- matrix(base[, 2], 18, 18)
  g1 <- m1 + 0.5; g2 <- m2 + 0.5
  fm <- wr_bands(list(m1, m2), tile = 6L)
  fg <- wr_bands(list(g1, g2), tile = 6L)
  on.exit(unlink(c(fm, fg)))

  out <- mop(fg, fm, k = 20, metric = "mahalanobis")
  r <- cbind(as.vector(m1), as.vector(m2))
  Tf <- chol(solve(stats::cov(r)))
  ref <- brute_mop_distance(list(g1, g2), list(m1, m2), 20, transform = Tf)
  expect_equal(as.vector(out$mop_distance), ref, tolerance = 1e-9)
})

test_that("mop rejects mismatched band counts", {
  m <- wr_bands(list(matrix(1, 5, 5), matrix(2, 5, 5)))
  g <- wr_bands(list(matrix(1, 5, 5)))
  on.exit(unlink(c(m, g)))
  expect_error(mop(g, m, percentage = 10), "same number of bands")
})

test_that("mop distance surface agrees with the mop package", {
  skip_if_not_installed("mop")
  skip_if_not_installed("terra")
  set.seed(5)
  m1 <- matrix(rnorm(30 * 30, 10, 2), 30, 30)
  m2 <- matrix(rnorm(30 * 30, 800, 40), 30, 30)
  g1 <- m1 + 2; g2 <- m2 - 30

  # mop() operates on terra SpatRasters (calibration m, projection g).
  to_rast <- function(b1, b2) {
    r <- terra::rast(nlyrs = 2, nrows = nrow(b1), ncols = ncol(b1),
                     xmin = 0, xmax = ncol(b1), ymin = 0, ymax = nrow(b1))
    terra::values(r) <- cbind(as.vector(t(b1)), as.vector(t(b2)))
    names(r) <- c("bio1", "bio12")
    r
  }
  ref <- tryCatch(
    mop::mop(m = to_rast(m1, m2), g = to_rast(g1, g2), type = "basic",
             calculate_distance = TRUE, where_distance = "all",
             distance = "euclidean", scale = FALSE, center = FALSE,
             percentage = 10, rescale_distance = FALSE,
             progress_bar = FALSE),
    error = function(e) skip(paste("mop:: API differs:", conditionMessage(e)))
  )
  ref_d <- as.vector(t(matrix(terra::values(ref$mop_distances),
                              nrow(m1), ncol(m1), byrow = TRUE)))

  fm <- wr_bands(list(m1, m2)); fg <- wr_bands(list(g1, g2))
  on.exit(unlink(c(fm, fg)))
  got <- as.vector(mop(fg, fm, percentage = 10)$mop_distance)

  finite <- is.finite(ref_d) & is.finite(got)
  skip_if(sum(finite) < 100, "too few comparable cells")
  expect_equal(got[finite], ref_d[finite], tolerance = 1e-4)
})
