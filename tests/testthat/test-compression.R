# -- Phase 1: compression round-trips ------------------------------------------

test_that("compress='fast' round-trips doubles", {
  df <- data.frame(x = rnorm(500), y = runif(500))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$x, df$x)
  expect_equal(result$y, df$y)
})

test_that("compress='small' round-trips doubles", {
  df <- data.frame(x = rnorm(500), y = runif(500))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small")
  result <- tbl(f) |> collect()
  expect_equal(result$x, df$x)
  expect_equal(result$y, df$y)
})

test_that("compress='small' round-trips integers", {
  df <- data.frame(a = 1:1000, b = sample(1e6, 1000))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small")
  result <- tbl(f) |> collect()
  expect_equal(result$a, as.double(df$a))
  expect_equal(result$b, as.double(df$b))
})

test_that("compress='small' round-trips strings", {
  df <- data.frame(s = sample(letters, 500, replace = TRUE),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small")
  result <- tbl(f) |> collect()
  expect_equal(result$s, df$s)
})

test_that("compress='small' round-trips booleans with NA", {
  df <- data.frame(flag = c(TRUE, FALSE, NA, TRUE, FALSE, rep(TRUE, 495)))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small")
  result <- tbl(f) |> collect()
  expect_equal(result$flag, df$flag)
})

test_that("compress='small' round-trips all types together", {
  df <- data.frame(
    i = c(1L, 2L, NA, 4L, seq(5L, 500L)),
    d = c(1.1, NA, 3.3, 4.4, rnorm(496)),
    b = c(TRUE, NA, FALSE, TRUE, sample(c(TRUE, FALSE), 496, replace = TRUE)),
    s = c("a", NA, "c", "d", sample(letters, 496, replace = TRUE)),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small")
  result <- tbl(f) |> collect()
  expect_equal(result$d, df$d)
  expect_equal(result$b, df$b)
  expect_equal(result$s, df$s)
  expect_equal(result$i, as.double(df$i))
})

test_that("small files are no larger than fast files on structured data", {
  df <- data.frame(x = rep(1:100, 50), y = rep(rnorm(100), 50))
  f_fast  <- tempfile(fileext = ".vtr")
  f_ratio <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f_fast, f_ratio)))
  write_vtr(df, f_fast,  compress = "fast")
  write_vtr(df, f_ratio, compress = "small")
  expect_lte(file.size(f_ratio), file.size(f_fast))
})

test_that("compress='small' with multiple row groups", {
  df <- data.frame(x = rnorm(1000), y = 1:1000)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small", batch_size = 200)
  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 1000L)
  expect_equal(result$x, df$x)
  expect_equal(result$y, as.double(df$y))
})

test_that("small with quantize round-trips", {
  set.seed(42)
  df <- data.frame(temp = rnorm(1000, 15, 5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small",
            quantize = list(temp = list(precision = 0.01, type = "int16")))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$temp - df$temp))
  expect_lt(max_err, 0.01 + 1e-10)
})

test_that("small with spatial round-trips", {
  nx <- 50; ny <- 50
  set.seed(1)
  temp <- 10 + 0.05 * rep(1:nx, ny) + 0.03 * rep(1:ny, each = nx) +
          rnorm(nx * ny, 0, 0.5)
  df <- data.frame(temp = temp)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small",
            quantize = list(temp = list(precision = 0.001, type = "int16")),
            spatial = list(nx = nx, ny = ny))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$temp - df$temp))
  expect_lt(max_err, 0.001 + 1e-10)
})

test_that("small with narrow int round-trips", {
  df <- data.frame(a = sample(-100L:100L, 500, replace = TRUE),
                   b = sample(-30000L:30000L, 500, replace = TRUE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "small",
            col_types = c(a = "int8", b = "int16"))
  result <- tbl(f) |> collect()
  expect_equal(result$a, as.double(df$a))
  expect_equal(result$b, as.double(df$b))
})

test_that("compress='none' round-trips doubles", {
  df <- data.frame(x = rnorm(500), y = runif(500))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "none")
  result <- tbl(f) |> collect()
  expect_equal(result$x, df$x)
  expect_equal(result$y, df$y)
})

test_that("compress='fast' round-trips integers", {
  df <- data.frame(a = 1:1000, b = sample(1e6, 1000))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$a, as.double(df$a))
  expect_equal(result$b, as.double(df$b))
})

test_that("compress='fast' round-trips strings", {
  df <- data.frame(s = sample(letters, 500, replace = TRUE),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$s, df$s)
})

test_that("compress='fast' round-trips booleans with NA", {
  df <- data.frame(flag = c(TRUE, FALSE, NA, TRUE, FALSE, rep(TRUE, 495)))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$flag, df$flag)
})

test_that("compress='fast' round-trips all types together", {
  df <- data.frame(
    i = c(1L, 2L, NA, 4L, seq(5L, 500L)),
    d = c(1.1, NA, 3.3, 4.4, rnorm(496)),
    b = c(TRUE, NA, FALSE, TRUE, sample(c(TRUE, FALSE), 496, replace = TRUE)),
    s = c("a", NA, "c", "d", sample(letters, 496, replace = TRUE)),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$d, df$d)
  expect_equal(result$b, df$b)
  expect_equal(result$s, df$s)
  expect_equal(result$i, as.double(df$i))
})

test_that("compressed files are smaller than uncompressed", {
  df <- data.frame(x = rep(1:100, 50), y = rep(rnorm(100), 50))
  f_none <- tempfile(fileext = ".vtr")
  f_fast <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f_none, f_fast)))

  write_vtr(df, f_none, compress = "none")
  write_vtr(df, f_fast, compress = "fast")

  expect_lt(file.size(f_fast), file.size(f_none))
})

test_that("compress='fast' with multiple row groups", {
  df <- data.frame(x = rnorm(1000), y = 1:1000)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast", batch_size = 200)
  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 1000L)
  expect_equal(result$x, df$x)
  expect_equal(result$y, as.double(df$y))
})

test_that("uncompressed file reads after rewrite with compression", {
  df <- data.frame(x = 1:100, y = rnorm(100))
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(df, f1, compress = "none")
  write_vtr(df, f2, compress = "fast")

  r1 <- tbl(f1) |> collect()
  r2 <- tbl(f2) |> collect()
  expect_equal(r1$x, r2$x)
  expect_equal(r1$y, r2$y)
})

# -- Phase 2a: narrow integer types -------------------------------------------

test_that("col_types int8 round-trips", {
  df <- data.frame(val = c(-100L, 0L, 50L, 127L, NA))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, col_types = c(val = "int8"))
  result <- tbl(f) |> collect()
  expect_equal(result$val, as.double(df$val))
})

test_that("col_types int16 round-trips", {
  df <- data.frame(val = c(-30000L, 0L, 15000L, 32767L, NA))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, col_types = c(val = "int16"))
  result <- tbl(f) |> collect()
  expect_equal(result$val, as.double(df$val))
})

test_that("col_types int32 round-trips", {
  df <- data.frame(val = c(-2000000L, 0L, 1000000L, NA))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, col_types = c(val = "int32"))
  result <- tbl(f) |> collect()
  expect_equal(result$val, as.double(df$val))
})

test_that("narrow int with compression round-trips", {
  df <- data.frame(a = sample(-100L:100L, 500, replace = TRUE),
                   b = sample(-30000L:30000L, 500, replace = TRUE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            col_types = c(a = "int8", b = "int16"))
  result <- tbl(f) |> collect()
  expect_equal(result$a, as.double(df$a))
  expect_equal(result$b, as.double(df$b))
})

test_that("narrow int files are smaller", {
  df <- data.frame(val = sample(-100L:100L, 1000, replace = TRUE))
  f64 <- tempfile(fileext = ".vtr")
  f8  <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f64, f8)))
  write_vtr(df, f64, compress = "none")
  write_vtr(df, f8, compress = "none", col_types = c(val = "int8"))
  expect_lt(file.size(f8), file.size(f64))
})

test_that("filter works on narrow int columns", {
  df <- data.frame(id = 1:200, val = sample(-100L:100L, 200, replace = TRUE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, col_types = c(val = "int8"))
  result <- tbl(f) |> filter(val > 0) |> collect()
  expected <- df[df$val > 0, ]
  expect_equal(nrow(result), nrow(expected))
  expect_equal(result$val, as.double(expected$val))
})

test_that("mutate works on narrow int columns", {
  df <- data.frame(val = c(10L, 20L, 30L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, col_types = c(val = "int8"))
  result <- tbl(f) |> mutate(doubled = val * 2) |> collect()
  expect_equal(result$doubled, c(20, 40, 60))
})

# -- Phase 2b: lossy quantization ---------------------------------------------

test_that("quantize round-trips within precision", {
  set.seed(42)
  df <- data.frame(temp = runif(500, -20, 20))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, quantize = list(temp = list(precision = 0.001,
                                                type = "int16")))
  result <- tbl(f) |> collect()
  expect_equal(length(result$temp), 500L)
  max_err <- max(abs(result$temp - df$temp), na.rm = TRUE)
  expect_lt(max_err, 0.001 + 1e-10)
})

test_that("quantize with scale parameter", {
  df <- data.frame(val = c(0.0, 0.5, 1.0, -1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, quantize = list(val = list(scale = 1000, type = "int16")))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$val - df$val))
  expect_lt(max_err, 0.001 + 1e-10)
})

test_that("quantize preserves NA", {
  df <- data.frame(temp = c(1.5, NA, -3.2, NA, 10.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, quantize = list(temp = list(precision = 0.01,
                                                type = "int16")))
  result <- tbl(f) |> collect()
  expect_true(is.na(result$temp[2]))
  expect_true(is.na(result$temp[4]))
  non_na <- c(1, 3, 5)
  max_err <- max(abs(result$temp[non_na] - df$temp[non_na]))
  expect_lt(max_err, 0.01 + 1e-10)
})

test_that("quantize with compression round-trips", {
  set.seed(123)
  df <- data.frame(temp = rnorm(1000, 15, 5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            quantize = list(temp = list(precision = 0.01, type = "int16")))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$temp - df$temp))
  expect_lt(max_err, 0.01 + 1e-10)
})

test_that("quantize to int8", {
  df <- data.frame(val = c(-5.0, 0.0, 5.0, 12.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, quantize = list(val = list(precision = 0.1,
                                               type = "int8")))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$val - df$val))
  expect_lt(max_err, 0.1 + 1e-10)
})

test_that("quantize produces smaller files", {
  set.seed(99)
  df <- data.frame(temp = rnorm(5000, 15, 5))
  f_raw <- tempfile(fileext = ".vtr")
  f_q   <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f_raw, f_q)))
  write_vtr(df, f_raw, compress = "fast")
  write_vtr(df, f_q, compress = "fast",
            quantize = list(temp = list(precision = 0.01, type = "int16")))
  expect_lt(file.size(f_q), file.size(f_raw))
})

# -- Phase 2c: spatial encoding -----------------------------------------------

test_that("spatial round-trips on smooth raster data", {
  nx <- 50; ny <- 50
  x_coord <- rep(1:nx, ny)
  y_coord <- rep(1:ny, each = nx)
  # smooth temperature field: gradient + small noise
  set.seed(1)
  temp <- 10 + 0.05 * x_coord + 0.03 * y_coord + rnorm(nx * ny, 0, 0.5)
  df <- data.frame(temp = temp)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            quantize = list(temp = list(precision = 0.001, type = "int16")),
            spatial = list(nx = nx, ny = ny))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$temp - df$temp))
  expect_lt(max_err, 0.001 + 1e-10)
})

test_that("spatial without quantize round-trips integers", {
  nx <- 20; ny <- 20
  vals <- as.integer(rep(1:20, each = 20) + rep(1:20, 20))
  df <- data.frame(val = vals)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            spatial = list(nx = nx, ny = ny))
  result <- tbl(f) |> collect()
  expect_equal(result$val, as.double(df$val))
})

test_that("spatial produces smaller files on smooth data", {
  nx <- 100; ny <- 100
  set.seed(2)
  temp <- 10 + 0.05 * rep(1:nx, ny) + 0.03 * rep(1:ny, each = nx)
  df <- data.frame(temp = temp)

  f_plain <- tempfile(fileext = ".vtr")
  f_spatial <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f_plain, f_spatial)))

  write_vtr(df, f_plain, compress = "fast",
            quantize = list(temp = list(precision = 0.001, type = "int16")))
  write_vtr(df, f_spatial, compress = "fast",
            quantize = list(temp = list(precision = 0.001, type = "int16")),
            spatial = list(nx = nx, ny = ny))

  expect_lt(file.size(f_spatial), file.size(f_plain))
})

test_that("spatial + quantize + fast compression round-trips", {
  nx <- 30; ny <- 30
  set.seed(3)
  temp <- 15 + 0.1 * rep(1:nx, ny) + rnorm(nx * ny, 0, 0.2)
  df <- data.frame(temp = temp)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            quantize = list(temp = list(precision = 0.001, type = "int16")),
            spatial = list(nx = nx, ny = ny))
  result <- tbl(f) |> collect()
  max_err <- max(abs(result$temp - df$temp))
  expect_lt(max_err, 0.001 + 1e-10)
})

test_that("spatial requires nx*ny == n_rows", {
  nx <- 40; ny <- 40
  df <- data.frame(temp = rnorm(100))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  expect_error(
    write_vtr(df, f, compress = "fast",
              spatial = list(nx = nx, ny = ny)),
    "nx\\*ny"
  )
})

# -- DIFF encoding (auto-selected for slowly varying data) --------------------

test_that("slowly varying integer data round-trips (triggers DIFF encoding)", {
  # Monotonically increasing with small increments - should trigger DIFF
  df <- data.frame(ts = cumsum(sample(1:5, 1000, replace = TRUE)))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$ts, as.double(df$ts))
})

test_that("slowly varying doubles round-trip (may trigger DIFF)", {
  set.seed(5)
  df <- data.frame(val = cumsum(rnorm(1000, 0, 0.01)))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast")
  result <- tbl(f) |> collect()
  expect_equal(result$val, df$val)
})

# -- Combined pipeline: narrow + compress + filter ----------------------------

test_that("full pipeline: write compressed narrow int, then filter + collect", {
  df <- data.frame(
    id = 1:1000,
    category = sample(c(-1L, 0L, 1L), 1000, replace = TRUE),
    value = rnorm(1000)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            col_types = c(category = "int8"))
  result <- tbl(f) |> filter(category == 1) |> collect()
  expected <- df[df$category == 1L, ]
  expect_equal(nrow(result), nrow(expected))
  expect_equal(result$value, expected$value)
})

test_that("full pipeline: quantize + compress + summarise", {
  set.seed(10)
  df <- data.frame(
    grp = rep(c("A", "B"), each = 500),
    val = c(rnorm(500, 10, 2), rnorm(500, 20, 2))
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, compress = "fast",
            quantize = list(val = list(precision = 0.01, type = "int16")))

  result <- tbl(f) |>
    group_by(grp) |>
    summarise(avg = mean(val)) |>
    collect()

  # Means should be close to 10 and 20 (within quantization + sampling error)
  result <- result[order(result$grp), ]
  expect_equal(nrow(result), 2L)
  expect_lt(abs(result$avg[1] - 10), 0.5)
  expect_lt(abs(result$avg[2] - 20), 0.5)
})
