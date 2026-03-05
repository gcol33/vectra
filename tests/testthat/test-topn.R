# --- TopN heap-based optimization tests ---

test_that("slice_min basic correctness", {
  df <- data.frame(x = c(5.0, 1.0, 3.0, 2.0, 4.0),
                   y = c("e", "a", "c", "b", "d"),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 3) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(1, 2, 3))
  expect_equal(result$y, c("a", "b", "c"))
})

test_that("slice_max basic correctness", {
  df <- data.frame(x = c(5.0, 1.0, 3.0, 2.0, 4.0),
                   y = c("e", "a", "c", "b", "d"),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_max(order_by = x, n = 3) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(5, 4, 3))
  expect_equal(result$y, c("e", "d", "c"))
})

test_that("topn with n > nrow returns all rows sorted", {
  df <- data.frame(x = c(3.0, 1.0, 2.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 10) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(1, 2, 3))
})

test_that("topn with n = 1", {
  df <- data.frame(x = c(5.0, 1.0, 3.0, 2.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result_min <- tbl(f) |> slice_min(order_by = x, n = 1) |> collect()
  expect_equal(result_min$x, 1)
  result_max <- tbl(f) |> slice_max(order_by = x, n = 1) |> collect()
  expect_equal(result_max$x, 5)
})

test_that("topn with n = nrow returns all rows sorted", {
  df <- data.frame(x = c(3.0, 1.0, 2.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 3) |> collect()
  expect_equal(result$x, c(1, 2, 3))
})

test_that("topn with NAs sorts NAs last", {
  df <- data.frame(x = c(5.0, NA, 3.0, NA, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 2) |> collect()
  expect_equal(result$x, c(1, 3))
})

test_that("topn with all NAs returns NAs", {
  df <- data.frame(x = c(NA_real_, NA_real_, NA_real_))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 2) |> collect()
  expect_equal(nrow(result), 2)
  expect_true(all(is.na(result$x)))
})

test_that("topn on integer column", {
  df <- data.frame(x = c(50L, 10L, 30L, 20L, 40L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 3) |> collect()
  expect_equal(result$x, c(10, 20, 30))
})

test_that("topn on string column", {
  df <- data.frame(x = c("cherry", "apple", "banana", "date", "elderberry"),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 3) |> collect()
  expect_equal(result$x, c("apple", "banana", "cherry"))
})

test_that("topn on boolean column", {
  df <- data.frame(x = c(TRUE, FALSE, TRUE, FALSE, TRUE),
                   y = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 2) |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(FALSE, FALSE))
})

test_that("topn preserves all columns", {
  df <- data.frame(a = c(3.0, 1.0, 2.0), b = c("x", "y", "z"),
                   c = c(TRUE, FALSE, TRUE), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = a, n = 2) |> collect()
  expect_equal(result$a, c(1, 2))
  expect_equal(result$b, c("y", "z"))
  expect_equal(result$c, c(FALSE, TRUE))
})

test_that("topn after filter", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 3) |> slice_min(order_by = x, n = 2) |> collect()
  expect_equal(result$x, c(4, 5))
})

test_that("topn after mutate", {
  df <- data.frame(x = c(3.0, 1.0, 2.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = x * 2) |> slice_min(order_by = y, n = 2) |> collect()
  expect_equal(result$x, c(1, 2))
  expect_equal(result$y, c(2, 4))
})

test_that("topn with duplicate values", {
  df <- data.frame(x = c(1.0, 2.0, 2.0, 3.0, 3.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 4) |> collect()
  expect_equal(nrow(result), 4)
  expect_equal(result$x, c(1, 2, 2, 3))
})

test_that("topn single row dataset", {
  df <- data.frame(x = 42.0)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 1) |> collect()
  expect_equal(result$x, 42)
})

test_that("topn large dataset correctness", {
  set.seed(42)
  n <- 10000
  df <- data.frame(x = runif(n), y = sample(letters, n, replace = TRUE),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  k <- 50
  result <- tbl(f) |> slice_min(order_by = x, n = k) |> collect()
  expected <- head(df[order(df$x), ], k)
  expect_equal(result$x, expected$x)
})
