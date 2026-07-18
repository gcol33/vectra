# Regression tests for the round-4 audit fixes (0.11.5).

test_that("streaming roll_min/roll_max do not overflow the deque (long partition, short window)", {
  # Before the fix the min/max monotonic deque used absolute, never-rebased
  # indices while its capacity only grew on value-window overflow, so a long
  # partition with a short window (value window stays compacted) wrote OOB.
  n <- 3000
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  t <- t0 + seq_len(n)                     # 1s apart; "2 sec" window holds ~2 rows
  v <- as.numeric(n - seq_len(n))          # strictly decreasing: deque never tail-pops
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(t = t, v = v, id = seq_len(n)), f, batch_size = 128)
  r <- tbl(f) |> mutate(mx = roll_max(v, t, "2 sec"),
                        mn = roll_min(v, t, "2 sec")) |> collect()
  r <- r[order(r$id), ]
  sec <- as.numeric(t)
  refmx <- vapply(seq_len(n), function(i) max(v[sec > sec[i] - 2 & sec <= sec[i]]), numeric(1))
  refmn <- vapply(seq_len(n), function(i) min(v[sec > sec[i] - 2 & sec <= sec[i]]), numeric(1))
  expect_equal(r$mx, refmx)
  expect_equal(r$mn, refmn)
})

test_that("unary math over a double column is correct (no leak, no double free)", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  x <- c(4, 9, 16, 25)
  write_vtr(data.frame(x = x), f)
  r <- tbl(f) |> mutate(s = sqrt(x), a = abs(-x), l = log(x)) |> collect()
  expect_equal(r$s, sqrt(x))
  expect_equal(r$a, x)
  expect_equal(r$l, log(x))
})

test_that("binary-search pushdown keeps matching rows for fractional thresholds on sorted int columns", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  df <- data.frame(x = rep(0:9, each = 3))          # sorted ints, stored int64
  write_vtr(df, f, batch_size = 3)                  # many small groups -> sorted column
  expect_equal(sort((tbl(f) |> filter(x < 2.9) |> collect())$x), rep(0:2, each = 3))
  expect_equal(sort((tbl(f) |> filter(x <= 2.9) |> collect())$x), rep(0:2, each = 3))
  expect_equal(sort((tbl(f) |> filter(x > 6.1) |> collect())$x), rep(7:9, each = 3))
  expect_equal(sort((tbl(f) |> filter(x >= 7.1) |> collect())$x), rep(8:9, each = 3))
  # negative fractional (truncation-toward-zero rounds the wrong way)
  df2 <- data.frame(x = rep(-9:0, each = 3))
  f2 <- tempfile(fileext = ".vtr"); on.exit(unlink(f2), add = TRUE)
  write_vtr(df2, f2, batch_size = 3)
  expect_equal(sort((tbl(f2) |> filter(x > -2.9) |> collect())$x), rep(-2:0, each = 3))
})

test_that("an interior all-NaN row group does not drop matching rows on a sorted double column", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(y = c(1, 2, NaN, NaN, 3, 4)), f, batch_size = 2)
  got <- (tbl(f) |> filter(y < 5) |> collect())$y
  expect_equal(sort(got[!is.nan(got)]), c(1, 2, 3, 4))
})

test_that("fuzzy_join rejects non-string key/block columns instead of crashing", {
  a <- tempfile(fileext = ".vtr"); b <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(a, b)))
  write_vtr(data.frame(id = 1:3, k = c(10, 20, 30)), a)     # numeric key
  write_vtr(data.frame(id = 1:3, k = c(11, 21, 31)), b)
  expect_error(
    fuzzy_join(tbl(a), tbl(b), by = c("k" = "k"), max_dist = 1) |> collect(),
    "must be string"
  )
})

test_that("datetime parts are correct for pre-1970 dates and near-epoch POSIXct", {
  d <- as.Date(c("1965-03-01", "1970-01-02", "2021-02-15"))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(d = d, id = seq_along(d)), f)
  r <- tbl(f) |> mutate(y = year(d), m = month(d), dd = day(d)) |> collect()
  r <- r[order(r$id), ]
  expect_equal(r$y,  c(1965, 1970, 2021))
  expect_equal(r$m,  c(3, 1, 2))
  expect_equal(r$dd, c(1, 2, 15))

  ts <- as.POSIXct(c("1970-01-01 01:00:00", "2021-06-01 12:30:45",
                     "1969-12-31 23:00:00"), tz = "UTC")
  f2 <- tempfile(fileext = ".vtr"); on.exit(unlink(f2), add = TRUE)
  write_vtr(data.frame(ts = ts, id = seq_along(ts)), f2)
  r2 <- tbl(f2) |> mutate(y = year(ts), h = hour(ts), s = second(ts)) |> collect()
  r2 <- r2[order(r2$id), ]
  expect_equal(r2$y, c(1970, 2021, 1969))   # near-epoch POSIXct no longer read as days
  expect_equal(r2$h, c(1, 12, 23))
  expect_equal(r2$s, c(0, 45, 0))
})

test_that("as.Date rejects invalid dates as NA instead of normalizing", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(s = c("2021-02-30", "2020-02-29", "2021-13-01",
                             "abcd-01-01", "2021-06-15"), id = 1:5), f)
  r <- tbl(f) |> mutate(d = as.Date(s)) |> collect()
  r <- r[order(r$id), ]
  expect_equal(is.na(r$d), c(TRUE, FALSE, TRUE, TRUE, FALSE))
  expect_equal(as.Date(r$d[c(2, 5)], origin = "1970-01-01"),
               as.Date(c("2020-02-29", "2021-06-15")))
})

test_that("GeoTIFF with DEFLATE + horizontal predictor (2) decodes correctly", {
  skip_if_not_installed("terra")
  set.seed(3)
  r <- terra::rast(nrows = 30, ncols = 40,
                   vals = sample(0:60000, 1200), crs = "EPSG:4326")
  terra::ext(r) <- c(0, 40, 0, 30)
  f <- tempfile(fileext = ".tif"); on.exit(unlink(f))
  terra::writeRaster(r, f, overwrite = TRUE, datatype = "INT4U",
                     gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2"))
  v <- tbl_tiff(f) |> collect()
  expect_equal(sort(v$band1), sort(terra::values(terra::rast(f))[, 1]))
})
