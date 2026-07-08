# feature_knn(): mean distance to the nearest k (or nearest percentage%) of a
# resident reference cloud, in predictor space. Correctness is checked against a
# brute-force R reference (full distance matrix, sort, mean of the k smallest).

vtr_df <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

# Brute-force reference: mean distance to the k nearest reference rows, with an
# optional whitening transform applied to both sides first (Mahalanobis).
brute_mean_knn <- function(q, ref, k, transform = NULL) {
  if (!is.null(transform)) { q <- q %*% t(transform); ref <- ref %*% t(transform) }
  apply(q, 1L, function(row) {
    d <- sqrt(colSums((t(ref) - row)^2))
    mean(sort(d)[seq_len(k)])
  })
}

test_that("euclidean feature_knn matches the brute-force mean distance", {
  set.seed(1)
  ref <- matrix(rnorm(200 * 3), 200, 3)
  colnames(ref) <- c("a", "b", "c")
  qy <- as.data.frame(matrix(rnorm(40 * 3), 40, 3)); names(qy) <- c("a", "b", "c")
  f <- vtr_df(qy); on.exit(unlink(f))

  got <- collect(feature_knn(tbl(f), ref, k = 8))
  exp <- brute_mean_knn(as.matrix(qy), ref, 8)
  expect_equal(got$knn_distance, exp, tolerance = 1e-9)
  # original columns ride through unchanged, one row in one row out
  expect_equal(nrow(got), nrow(qy))
  expect_equal(got$a, qy$a, tolerance = 1e-12)
})

test_that("percentage selects ceil(percentage% * N) neighbours", {
  set.seed(2)
  ref <- matrix(rnorm(300 * 2), 300, 2); colnames(ref) <- c("x", "y")
  qy <- data.frame(x = rnorm(25), y = rnorm(25))
  f <- vtr_df(qy); on.exit(unlink(f))

  got <- collect(feature_knn(tbl(f), ref, percentage = 5))$knn_distance
  exp <- brute_mean_knn(as.matrix(qy), ref, ceiling(0.05 * 300))  # 15
  expect_equal(got, exp, tolerance = 1e-9)
})

test_that("mahalanobis feature_knn matches whitened brute force", {
  set.seed(3)
  # correlated predictors so the metric actually differs from euclidean
  A <- matrix(c(3, 1.5, 1.5, 2), 2, 2)
  ref <- matrix(rnorm(400 * 2), 400, 2) %*% chol(A); colnames(ref) <- c("x", "y")
  qy <- as.data.frame(matrix(rnorm(30 * 2), 30, 2) %*% chol(A)); names(qy) <- c("x", "y")
  f <- vtr_df(qy); on.exit(unlink(f))

  Tf <- chol(solve(stats::cov(ref)))
  got <- collect(feature_knn(tbl(f), ref, k = 12, metric = "mahalanobis"))$knn_distance
  exp <- brute_mean_knn(as.matrix(qy), ref, 12, transform = Tf)
  expect_equal(got, exp, tolerance = 1e-9)
})

test_that("k is capped at the reference cloud size", {
  set.seed(4)
  ref <- matrix(rnorm(10 * 2), 10, 2); colnames(ref) <- c("x", "y")
  qy <- data.frame(x = rnorm(5), y = rnorm(5))
  f <- vtr_df(qy); on.exit(unlink(f))

  # k = 999 -> all 10 reference rows -> mean distance to every reference point
  got <- collect(feature_knn(tbl(f), ref, k = 999))$knn_distance
  exp <- brute_mean_knn(as.matrix(qy), ref, 10)
  expect_equal(got, exp, tolerance = 1e-9)
})

test_that("NA predictor rows yield NA distance", {
  set.seed(5)
  ref <- matrix(rnorm(50 * 2), 50, 2); colnames(ref) <- c("x", "y")
  qy <- data.frame(x = c(0, NA, 1), y = c(0, 1, NA))
  f <- vtr_df(qy); on.exit(unlink(f))

  got <- collect(feature_knn(tbl(f), ref, k = 5))$knn_distance
  expect_false(is.na(got[1]))
  expect_true(is.na(got[2]))
  expect_true(is.na(got[3]))
})

test_that("streaming across batches equals a single-batch result", {
  set.seed(6)
  ref <- matrix(rnorm(100 * 2), 100, 2); colnames(ref) <- c("x", "y")
  qy <- data.frame(x = rnorm(5000), y = rnorm(5000))
  f <- vtr_df(qy); on.exit(unlink(f))
  # small row groups force many batches through the streamed query path
  fs <- tempfile(fileext = ".vtr"); on.exit(unlink(fs), add = TRUE)
  write_vtr(qy, fs, batch_size = 137L)

  got <- collect(feature_knn(tbl(fs), ref, k = 7))$knn_distance
  exp <- brute_mean_knn(as.matrix(qy), ref, 7)
  expect_equal(got, exp, tolerance = 1e-9)
})

test_that("custom dist_col name is honoured", {
  ref <- matrix(rnorm(20 * 2), 20, 2); colnames(ref) <- c("x", "y")
  f <- vtr_df(data.frame(x = 0, y = 0)); on.exit(unlink(f))
  got <- collect(feature_knn(tbl(f), ref, k = 3, dist_col = "mop"))
  expect_true("mop" %in% names(got))
})

test_that("invalid inputs are rejected", {
  ref <- matrix(rnorm(20 * 2), 20, 2); colnames(ref) <- c("x", "y")
  f <- vtr_df(data.frame(x = 0, y = 0)); on.exit(unlink(f))

  expect_error(feature_knn(data.frame(x = 1), ref, k = 1), "vectra_node")
  expect_error(feature_knn(tbl(f), ref), "exactly one")               # neither k nor pct
  expect_error(feature_knn(tbl(f), ref, k = 1, percentage = 5), "exactly one")
  expect_error(feature_knn(tbl(f), ref, k = 0), "positive")
  expect_error(feature_knn(tbl(f), ref, percentage = 200), "0, 100")
  expect_error(feature_knn(tbl(f), data.frame(z = 1:3), vars = "q", k = 1),
               "missing")
})
