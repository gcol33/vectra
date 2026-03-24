# --- count ---

test_that("count with sort = TRUE sorts descending", {
  df <- data.frame(g = c("a", "a", "a", "b", "b", "c"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> count(g, sort = TRUE) |> collect()
  expect_equal(result$n, c(3, 2, 1))
  expect_equal(result$g, c("a", "b", "c"))
})

test_that("count with wt sums the weight column", {
  df <- data.frame(g = c("a", "a", "b"), w = c(10.0, 20.0, 5.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> count(g, wt = w) |> collect()
  expect_equal(result$n[result$g == "a"], 30)
  expect_equal(result$n[result$g == "b"], 5)
})

test_that("count with custom name and sort = TRUE works", {
  df <- data.frame(g = c("x", "x", "y"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> count(g, sort = TRUE, name = "cnt") |> collect()
  expect_equal(result$cnt, c(2, 1))
  expect_true("cnt" %in% names(result))
})

test_that("count with no groups counts total rows", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> count() |> collect()
  expect_equal(result$n, 3)
})

# --- tally ---

test_that("tally with sort = TRUE sorts descending", {
  df <- data.frame(g = c("a", "a", "a", "b", "b", "c"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> tally(sort = TRUE) |> collect()
  expect_equal(result$n, c(3, 2, 1))
})

test_that("tally with wt sums the weight column", {
  df <- data.frame(g = c("a", "a", "b"), w = c(10.0, 20.0, 5.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> tally(wt = w) |> collect()
  expect_equal(result$n[result$g == "a"], 30)
  expect_equal(result$n[result$g == "b"], 5)
})
