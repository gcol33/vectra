test_that("select picks specified columns", {
  df <- data.frame(a = 1:3, b = 4:6, c = 7:9)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(a, c) |> collect()
  expect_equal(names(result), c("a", "c"))
  expect_equal(result$a, c(1, 2, 3))
  expect_equal(result$c, c(7, 8, 9))
})

test_that("select preserves column order", {
  df <- data.frame(x = 1:3, y = 4:6, z = 7:9)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(z, x) |> collect()
  expect_equal(names(result), c("z", "x"))
})
