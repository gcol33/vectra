# --- any / all ---

test_that("any aggregation works", {
  df <- data.frame(g = c("a", "a", "b", "b"),
                   x = c(TRUE, FALSE, FALSE, FALSE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(a = any(x)) |> collect()
  expect_equal(result$a[result$g == "a"], 1)
  expect_equal(result$a[result$g == "b"], 0)
})

test_that("all aggregation works", {
  df <- data.frame(g = c("a", "a", "b", "b"),
                   x = c(TRUE, TRUE, TRUE, FALSE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(a = all(x)) |> collect()
  expect_equal(result$a[result$g == "a"], 1)
  expect_equal(result$a[result$g == "b"], 0)
})

# --- median ---

test_that("median aggregation works", {
  df <- data.frame(g = c("a", "a", "a", "b", "b"),
                   x = c(1.0, 3.0, 2.0, 10.0, 20.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(m = median(x)) |> collect()
  expect_equal(result$m[result$g == "a"], 2)
  expect_equal(result$m[result$g == "b"], 15)
})

test_that("median with na.rm = TRUE", {
  df <- data.frame(g = c("a", "a", "a"), x = c(1.0, NA, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(m = median(x, na.rm = TRUE)) |> collect()
  expect_equal(result$m, 2)
})

# --- n_distinct ---

test_that("n_distinct counts unique values", {
  df <- data.frame(g = c("a", "a", "a", "b", "b"),
                   x = c(1.0, 1.0, 2.0, 3.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(nd = n_distinct(x)) |> collect()
  expect_equal(result$nd[result$g == "a"], 2)
  expect_equal(result$nd[result$g == "b"], 1)
})

# --- slice ---

test_that("slice selects rows by position", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0, 50.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice(2, 4)
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(20, 40))
})

test_that("slice with negative indices removes rows", {
  df <- data.frame(x = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice(-2)
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(10, 30))
})

# --- cross_join ---

test_that("cross_join produces Cartesian product", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(a = c(1, 2)), f1)
  write_vtr(data.frame(b = c("x", "y", "z"), stringsAsFactors = FALSE), f2)
  result <- cross_join(tbl(f1), tbl(f2))
  expect_equal(nrow(result), 6)
  expect_true("a" %in% names(result))
  expect_true("b" %in% names(result))
})

# --- ntile ---

test_that("ntile divides into buckets", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(bucket = ntile(3)) |> collect()
  expect_equal(result$bucket, c(1, 1, 2, 2, 3, 3))
})

# --- percent_rank ---

test_that("percent_rank computes relative rank", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(pr = percent_rank(x)) |> collect()
  expect_equal(result$pr, c(0, 0.25, 0.5, 0.75, 1.0))
})

# --- cume_dist ---

test_that("cume_dist computes cumulative distribution", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cd = cume_dist(x)) |> collect()
  expect_equal(result$cd, c(0.2, 0.4, 0.6, 0.8, 1.0))
})
