test_that("mutate adds a new column", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = x * 2) |> collect()
  expect_equal(result$x, c(1, 2, 3))
  expect_equal(result$y, c(2, 4, 6))
})

test_that("mutate with arithmetic expressions", {
  df <- data.frame(a = c(10.0, 20.0), b = c(3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(c = a + b, d = a - b) |> collect()
  expect_equal(result$c, c(13, 24))
  expect_equal(result$d, c(7, 16))
})

test_that("mutate sequential: later expr references earlier", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = x + 1, z = y * 2) |> collect()
  expect_equal(result$y, c(2, 3, 4))
  expect_equal(result$z, c(4, 6, 8))
})

test_that("mutate replaces existing column", {
  df <- data.frame(x = c(1.0, 2.0, 3.0), y = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(x = x * 100) |> collect()
  expect_equal(result$x, c(100, 200, 300))
  expect_equal(result$y, c(10, 20, 30))
})

test_that("mutate with NA propagation", {
  df <- data.frame(x = c(1.0, NA, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = x + 1) |> collect()
  expect_equal(result$y, c(2, NA, 4))
})

test_that("filter then select pipeline", {
  df <- data.frame(x = 1:10, y = as.double(11:20),
                   z = c(rep("a", 5), rep("b", 5)),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 5) |> select(x, z) |> collect()
  expect_equal(names(result), c("x", "z"))
  expect_equal(nrow(result), 5)
  expect_equal(result$z, rep("b", 5))
})
