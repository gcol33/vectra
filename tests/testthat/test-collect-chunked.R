# Streaming consumption: collect_chunked() fold and chunk_feeder() generator.
# These verbs must agree exactly with the materializing path (collect()) and
# with in-memory model fits, while only ever holding one batch at a time.

test_that("collect_chunked row count matches collect()", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  n <- collect_chunked(tbl(f), function(acc, chunk) acc + nrow(chunk),
                       .init = 0L)
  expect_equal(n, nrow(mtcars))
})

test_that("collect_chunked actually streams in multiple batches", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  df <- data.frame(x = as.double(1:100), g = rep(1:4, 25))
  write.csv(df, f, row.names = FALSE)

  # Small batch_size forces several chunks; the cursor must keep its place
  # across separate C_node_next_batch calls.
  n_batches <- collect_chunked(tbl_csv(f, batch_size = 10),
                               function(acc, chunk) acc + 1L, .init = 0L)
  expect_gt(n_batches, 1L)

  total <- collect_chunked(tbl_csv(f, batch_size = 10),
                           function(acc, chunk) acc + sum(chunk$x), .init = 0)
  expect_equal(total, sum(df$x))
})

test_that("collect_chunked column sums match collect() across batches", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  set.seed(1)
  df <- data.frame(a = rnorm(250), b = rnorm(250))
  write.csv(df, f, row.names = FALSE)

  streamed <- collect_chunked(
    tbl_csv(f, batch_size = 32),
    function(acc, chunk) {
      acc$a <- acc$a + sum(chunk$a)
      acc$b <- acc$b + sum(chunk$b)
      acc
    },
    .init = list(a = 0, b = 0)
  )
  full <- tbl_csv(f) |> collect()
  expect_equal(streamed$a, sum(full$a))
  expect_equal(streamed$b, sum(full$b))
})

test_that("collect_chunked handles filtered (selection-vector) batches", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  df <- data.frame(x = as.double(1:200))
  write.csv(df, f, row.names = FALSE)

  kept <- collect_chunked(
    tbl_csv(f, batch_size = 16) |> filter(x > 150),
    function(acc, chunk) acc + nrow(chunk), .init = 0L
  )
  expect_equal(kept, sum(df$x > 150))
})

test_that("collect_chunked returns .init unchanged when no rows match", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  out <- collect_chunked(
    tbl(f) |> filter(mpg > 1e6),
    function(acc, chunk) acc + nrow(chunk), .init = 0L
  )
  expect_identical(out, 0L)
})

test_that("collect_chunked sufficient statistics reproduce lm() exactly", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  set.seed(42)
  df <- data.frame(wt = rnorm(500, 3, 1), hp = rnorm(500, 150, 40))
  df$mpg <- 30 - 5 * df$wt - 0.05 * df$hp + rnorm(500, 0, 1)
  write.csv(df, f, row.names = FALSE)

  acc <- collect_chunked(
    tbl_csv(f, batch_size = 64) |> select(mpg, wt, hp),
    function(acc, chunk) {
      X <- cbind(1, chunk$wt, chunk$hp)
      y <- chunk$mpg
      list(XtX = acc$XtX + crossprod(X), Xty = acc$Xty + crossprod(X, y))
    },
    .init = list(XtX = matrix(0, 3, 3), Xty = matrix(0, 3, 1))
  )
  beta_stream <- drop(solve(acc$XtX, acc$Xty))
  beta_lm <- unname(coef(lm(mpg ~ wt + hp, data = df)))
  expect_equal(beta_stream, beta_lm, tolerance = 1e-8)
})

test_that("collect_chunked validates its arguments", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  expect_error(collect_chunked(mtcars, function(a, c) a), "vectra_node")
  expect_error(collect_chunked(tbl(f), "not a function"), "function")
})

test_that("collect_chunked preserves string, factor, and Date columns", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(
    s = rep(c("alpha", "beta", "gamma"), length.out = 120),
    g = factor(rep(c("lo", "hi"), length.out = 120), levels = c("lo", "hi")),
    d = as.Date("2020-01-01") + 0:119,
    v = as.double(1:120),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 25)

  # Rebuild each column by concatenating the per-batch chunks, then compare to
  # the materializing path. Exercises batch_to_dataframe's string fill and the
  # annotation (factor/Date) reconstruction one batch at a time.
  parts <- collect_chunked(tbl(f), function(acc, chunk) c(acc, list(chunk)),
                           .init = list())
  expect_gt(length(parts), 1L)
  rebuilt <- do.call(rbind, parts)
  full <- tbl(f) |> collect()

  expect_equal(rebuilt$s, full$s)
  expect_identical(levels(rebuilt$g), levels(full$g))
  expect_equal(as.character(rebuilt$g), as.character(full$g))
  expect_s3_class(rebuilt$d, "Date")
  expect_equal(rebuilt$d, full$d)
  expect_equal(rebuilt$v, full$v)
})

test_that("chunk_feeder replays the full stream on every reset", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  df <- data.frame(x = as.double(1:120))
  write.csv(df, f, row.names = FALSE)

  src <- function() tbl_csv(f, batch_size = 25)
  feed <- chunk_feeder(src)

  drain <- function() {
    feed(reset = TRUE)
    rows <- 0L
    while (!is.null(chunk <- feed(reset = FALSE))) rows <- rows + nrow(chunk)
    rows
  }

  # Two independent passes must each see every row.
  expect_equal(drain(), nrow(df))
  expect_equal(drain(), nrow(df))
})

test_that("chunk_feeder validates its argument", {
  expect_error(chunk_feeder(42), "function")
})

test_that("chunk_feeder drives an out-of-core Gaussian GLM equal to lm()", {
  skip_if_not_installed("biglm")
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  set.seed(7)
  df <- data.frame(wt = rnorm(800, 3, 1), hp = rnorm(800, 150, 40))
  df$mpg <- 30 - 5 * df$wt - 0.05 * df$hp + rnorm(800, 0, 1)
  write.csv(df, f, row.names = FALSE)

  src <- function() tbl_csv(f, batch_size = 128) |> select(mpg, wt, hp)
  fit <- biglm::bigglm(mpg ~ wt + hp, data = chunk_feeder(src),
                       family = gaussian())
  expect_equal(unname(coef(fit)),
               unname(coef(lm(mpg ~ wt + hp, data = df))),
               tolerance = 1e-6)
})

test_that("chunk_feeder drives an out-of-core logistic GLM equal to glm()", {
  skip_if_not_installed("biglm")
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  set.seed(11)
  n <- 2000
  bio1 <- rnorm(n)
  bio12 <- rnorm(n)
  eta <- -0.3 + 1.2 * bio1 - 0.8 * bio12
  presence <- rbinom(n, 1, plogis(eta))
  df <- data.frame(presence = as.double(presence), bio1 = bio1, bio12 = bio12)
  write.csv(df, f, row.names = FALSE)

  src <- function() tbl_csv(f, batch_size = 256) |>
    select(presence, bio1, bio12)
  fit <- biglm::bigglm(presence ~ bio1 + bio12, data = chunk_feeder(src),
                       family = binomial(), maxit = 25)

  ref <- glm(presence ~ bio1 + bio12, data = df, family = binomial())
  expect_equal(unname(coef(fit)), unname(coef(ref)), tolerance = 1e-3)
})
