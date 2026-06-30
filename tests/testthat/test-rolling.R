# Recovery tests for time-based rolling window functions, checked against a
# brute-force O(n^2) reference over the trailing window (t - window, t].

# Brute-force trailing rolling aggregate within optional groups.
brute_roll <- function(t, x, window_s, fn, grp = NULL) {
  secs <- as.numeric(t)
  n <- length(secs)
  out <- numeric(n)
  for (i in seq_len(n)) {
    in_win <- secs > secs[i] - window_s & secs <= secs[i]
    if (!is.null(grp)) in_win <- in_win & grp == grp[i]
    nrow_win <- sum(in_win)        # roll_n counts rows in the window
    v <- x[in_win]
    v <- v[!is.na(v)]              # other aggregates skip NA values
    out[i] <- switch(fn,
      sum  = sum(v),
      mean = if (length(v)) mean(v) else NA_real_,
      min  = if (length(v)) min(v)  else NA_real_,
      max  = if (length(v)) max(v)  else NA_real_,
      n    = nrow_win)
  }
  out
}

set.seed(21)

test_that("ungrouped roll_mean/sum/n/min/max match brute force", {
  n <- 80
  t0 <- as.POSIXct("2021-06-01 00:00:00", tz = "UTC")
  t <- t0 + cumsum(sample(60:900, n, replace = TRUE))  # irregular spacing
  x <- round(rnorm(n, 20, 4), 2)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(t = t, x = x, id = seq_len(n)), f)

  res <- tbl(f) |>
    mutate(rm = roll_mean(x, t, "1 hour"),
           rs = roll_sum(x, t, "1 hour"),
           rn = roll_n(t, "1 hour"),
           rmin = roll_min(x, t, "1 hour"),
           rmax = roll_max(x, t, "1 hour")) |>
    collect()
  res <- res[order(res$id), ]

  expect_equal(res$rm,  brute_roll(t, x, 3600, "mean"), tolerance = 1e-9)
  expect_equal(res$rs,  brute_roll(t, x, 3600, "sum"),  tolerance = 1e-9)
  expect_equal(res$rn,  brute_roll(t, x, 3600, "n"))
  expect_equal(res$rmin, brute_roll(t, x, 3600, "min"), tolerance = 1e-9)
  expect_equal(res$rmax, brute_roll(t, x, 3600, "max"), tolerance = 1e-9)
})

test_that("grouped rolling respects partitions", {
  n <- 100
  t0 <- as.POSIXct("2021-06-01 00:00:00", tz = "UTC")
  t <- t0 + cumsum(sample(60:600, n, replace = TRUE))
  x <- round(rnorm(n), 2)
  g <- sample(c("a", "b", "c"), n, replace = TRUE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(t = t, x = x, g = g, id = seq_len(n)), f)

  res <- tbl(f) |>
    group_by(g) |>
    mutate(rm = roll_mean(x, t, "30 min"), rn = roll_n(t, "30 min")) |>
    collect()
  res <- res[order(res$id), ]

  expect_equal(res$rm, brute_roll(t, x, 1800, "mean", grp = g), tolerance = 1e-9)
  expect_equal(res$rn, brute_roll(t, x, 1800, "n", grp = g))
})

test_that("NA values are skipped in the rolling window", {
  t0 <- as.POSIXct("2021-06-01 00:00:00", tz = "UTC")
  t <- t0 + c(0, 60, 120, 180)
  x <- c(1, NA, 3, NA)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(t = t, x = x, id = 1:4), f)

  res <- tbl(f) |>
    mutate(rs = roll_sum(x, t, "1 hour"),
           rmean = roll_mean(x, t, "1 hour"),
           rn = roll_n(t, "1 hour")) |>
    collect()
  res <- res[order(res$id), ]
  expect_equal(res$rs, c(1, 1, 4, 4))      # value NAs skipped in the sum
  expect_equal(res$rn, c(1, 2, 3, 4))      # roll_n counts rows, NA-valued included
  expect_equal(res$rmean, c(1, 1, 2, 2))
})

test_that("calendar window units are rejected", {
  t0 <- as.POSIXct("2021-06-01", tz = "UTC")
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(t = t0 + (0:4) * 86400, x = 1:5), f)
  expect_error(
    tbl(f) |> mutate(r = roll_mean(x, t, "1 month")) |> collect(),
    "fixed unit")
})
