test_that("filter with simple comparison", {
  df <- data.frame(x = c(1L, 2L, 3L, 4L, 5L), y = c(10.0, 20.0, 30.0, 40.0, 50.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 3) |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(4, 5))
  expect_equal(result$y, c(40, 50))
})

test_that("filter with equality", {
  df <- data.frame(x = c(1L, 2L, 3L), s = c("a", "b", "c"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x == 2) |> collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$s, "b")
})

test_that("filter with AND (multiple args)", {
  df <- data.frame(x = 1:10, y = rep(c(TRUE, FALSE), 5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 3, x < 8) |> collect()
  expect_equal(result$x, c(4, 5, 6, 7))
})

test_that("filter with boolean operators", {
  df <- data.frame(x = 1:6)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x < 3 | x > 4) |> collect()
  expect_equal(result$x, c(1, 2, 5, 6))
})

test_that("filter with NOT", {
  df <- data.frame(x = c(TRUE, FALSE, TRUE, FALSE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(!x) |> collect()
  expect_equal(result$x, c(FALSE, FALSE))
})

test_that("filter with is.na", {
  df <- data.frame(x = c(1L, NA, 3L, NA, 5L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(!is.na(x)) |> collect()
  expect_equal(result$x, c(1, 3, 5))
})

test_that("filter with NAs in predicate (treated as FALSE)", {
  df <- data.frame(x = c(1L, NA, 3L), y = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 1) |> collect()
  # NA > 1 is NA, treated as FALSE
  expect_equal(nrow(result), 1)
  expect_equal(result$x, 3)
})
