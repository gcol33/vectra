test_that("round-trip preserves double columns", {
  df <- data.frame(x = c(1.5, 2.7, NA, 4.0), y = c(NA, 10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(result$x, df$x)
  expect_equal(result$y, df$y)
})

test_that("round-trip preserves integer columns as double", {
  df <- data.frame(a = c(1L, 2L, NA, 4L), b = c(10L, NA, 30L, 40L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  # int64 stored internally, returned as double by default
  expect_equal(result$a, c(1, 2, NA, 4))
  expect_equal(result$b, c(10, NA, 30, 40))
})

test_that("round-trip preserves logical columns", {
  df <- data.frame(flag = c(TRUE, FALSE, NA, TRUE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(result$flag, df$flag)
})

test_that("round-trip preserves string columns", {
  df <- data.frame(s = c("hello", "world", NA, ""), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(result$s, df$s)
})

test_that("round-trip with all types together", {
  df <- data.frame(
    i = c(1L, 2L, NA),
    d = c(1.1, NA, 3.3),
    b = c(TRUE, NA, FALSE),
    s = c("a", NA, "c"),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(result$d, df$d)
  expect_equal(result$b, df$b)
  expect_equal(result$s, df$s)
  expect_equal(result$i, as.double(df$i))
})

test_that("multiple row groups round-trip correctly", {
  df <- data.frame(x = 1:100, y = as.double(101:200))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 30)
  result <- tbl(f) |> collect()
  expect_equal(result$x, as.double(df$x))
  expect_equal(result$y, df$y)
})

test_that("empty-string column round-trips", {
  df <- data.frame(s = c("", "", ""), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(result$s, df$s)
})

test_that("single-row data.frame round-trips", {
  df <- data.frame(x = 42L, y = "test", stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(result$x, 42)
  expect_equal(result$y, "test")
})

test_that("print method works", {
  df <- data.frame(x = 1:3, y = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  node <- tbl(f)
  expect_output(print(node), "vectra query node")
  expect_output(print(node), "int64")
  expect_output(print(node), "double")
})
