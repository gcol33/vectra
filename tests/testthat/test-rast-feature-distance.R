# rast_feature_distance(): the out-of-core predictor-space kNN distance surface
# over a projection raster. Correctness is checked against a resident brute-force
# reference; the streamed .vec path must reproduce the in-memory path and stay
# invariant across tile sizes.

wr_bands <- function(bands, ext = c(0, 0, ncol(bands[[1]]), nrow(bands[[1]])),
                     tile = 512L, names = NULL) {
  arr <- array(0, c(nrow(bands[[1]]), ncol(bands[[1]]), length(bands)))
  for (b in seq_along(bands)) arr[, , b] <- bands[[b]]
  f <- tempfile(fileext = ".vec")
  vec_write_raster(arr, f, dtype = "f64", extent = ext, tile_size = tile,
                   band_names = names)
  f
}

# Brute-force distance surface on resident band matrices.
brute_distance <- function(x_bands, ref_bands, keff, transform = NULL) {
  q <- do.call(cbind, lapply(x_bands, as.vector))
  r <- do.call(cbind, lapply(ref_bands, as.vector))
  r <- r[stats::complete.cases(r), , drop = FALSE]
  if (!is.null(transform)) { q <- q %*% t(transform); r <- r %*% t(transform) }
  vapply(seq_len(nrow(q)), function(i) {
    if (anyNA(q[i, ])) return(NA_real_)
    d <- sqrt(colSums((t(r) - q[i, ])^2))
    mean(sort(d)[seq_len(keff)])
  }, numeric(1))
}

test_that("distance surface recovers the brute-force reference", {
  set.seed(1)
  r1 <- matrix(rnorm(20 * 20, 10, 2), 20, 20)
  r2 <- matrix(rnorm(20 * 20, 800, 40), 20, 20)
  x1 <- r1 + 3; x2 <- r2 - 60
  fr <- wr_bands(list(r1, r2), tile = 8L)
  fx <- wr_bands(list(x1, x2), tile = 8L)
  on.exit(unlink(c(fr, fx)))

  got <- rast_feature_distance(fx, fr, percentage = 10)
  keff <- ceiling(0.10 * 400)
  ref <- brute_distance(list(x1, x2), list(r1, r2), keff)
  expect_equal(as.vector(got), ref, tolerance = 1e-9)
})

test_that("k selects an absolute neighbour count", {
  set.seed(2)
  r1 <- matrix(rnorm(16 * 16), 16, 16); r2 <- matrix(rnorm(16 * 16), 16, 16)
  x1 <- r1 + 1; x2 <- r2 - 1
  fr <- wr_bands(list(r1, r2), tile = 4L); fx <- wr_bands(list(x1, x2), tile = 4L)
  on.exit(unlink(c(fr, fx)))

  got <- rast_feature_distance(fx, fr, k = 25)
  ref <- brute_distance(list(x1, x2), list(r1, r2), 25)
  expect_equal(as.vector(got), ref, tolerance = 1e-9)
})

test_that("distance surface is invariant to tile size and streams to disk", {
  set.seed(3)
  r1 <- matrix(rnorm(24 * 24, 5, 1), 24, 24); r2 <- matrix(rnorm(24 * 24, 5, 1), 24, 24)
  x1 <- r1 + 1; x2 <- r2 - 1
  fr_s <- wr_bands(list(r1, r2), tile = 6L); fx_s <- wr_bands(list(x1, x2), tile = 6L)
  fr_b <- wr_bands(list(r1, r2), tile = 24L); fx_b <- wr_bands(list(x1, x2), tile = 24L)
  on.exit(unlink(c(fr_s, fx_s, fr_b, fx_b)))

  small <- rast_feature_distance(fx_s, fr_s, percentage = 15)
  big   <- rast_feature_distance(fx_b, fr_b, percentage = 15)
  expect_equal(small, big, tolerance = 1e-12)

  fout <- tempfile(fileext = ".vec"); on.exit(unlink(fout), add = TRUE)
  rast_feature_distance(fx_s, fr_s, percentage = 15, path = fout)
  rout <- vec_open_raster(fout)
  streamed <- as.vector(vec_read_window(rout, band = 1))
  vec_close_raster(rout)
  expect_equal(streamed, as.vector(small), tolerance = 1e-4)
})

test_that("mahalanobis surface matches the whitened brute force", {
  set.seed(4)
  A <- matrix(c(2, 1, 1, 2), 2, 2)
  base <- matrix(rnorm(18 * 18 * 2), 18 * 18, 2) %*% chol(A)
  r1 <- matrix(base[, 1], 18, 18); r2 <- matrix(base[, 2], 18, 18)
  x1 <- r1 + 0.5; x2 <- r2 + 0.5
  fr <- wr_bands(list(r1, r2), tile = 6L); fx <- wr_bands(list(x1, x2), tile = 6L)
  on.exit(unlink(c(fr, fx)))

  got <- rast_feature_distance(fx, fr, k = 20, metric = "mahalanobis")
  r <- cbind(as.vector(r1), as.vector(r2))
  Tf <- chol(solve(stats::cov(r)))
  ref <- brute_distance(list(x1, x2), list(r1, r2), 20, transform = Tf)
  expect_equal(as.vector(got), ref, tolerance = 1e-9)
})

test_that("vars selects bands by name", {
  set.seed(5)
  r1 <- matrix(rnorm(10 * 10), 10, 10); r2 <- matrix(rnorm(10 * 10), 10, 10)
  x1 <- r1 + 1; x2 <- r2 + 1
  fr <- wr_bands(list(r1, r2), names = c("bio1", "bio12"))
  fx <- wr_bands(list(x1, x2), names = c("bio1", "bio12"))
  on.exit(unlink(c(fr, fx)))

  # one predictor only
  got <- rast_feature_distance(fx, fr, vars = "bio1", k = 5)
  ref <- brute_distance(list(x1), list(r1), 5)
  expect_equal(as.vector(got), ref, tolerance = 1e-9)
})

test_that("mismatched band counts and bad neighbour args are rejected", {
  r <- wr_bands(list(matrix(1, 5, 5), matrix(2, 5, 5)))
  x <- wr_bands(list(matrix(1, 5, 5)))
  on.exit(unlink(c(r, x)))
  expect_error(rast_feature_distance(x, r, percentage = 10),
               "same number of bands")
  x2 <- wr_bands(list(matrix(1, 5, 5)))
  r2 <- wr_bands(list(matrix(1, 5, 5)))
  on.exit(unlink(c(x2, r2)), add = TRUE)
  expect_error(rast_feature_distance(x2, r2), "exactly one")
  expect_error(rast_feature_distance(x2, r2, k = 1, percentage = 5),
               "exactly one")
})
