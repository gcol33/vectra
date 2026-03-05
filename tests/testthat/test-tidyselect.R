# --- tidyselect helpers in select ---

test_that("select with starts_with", {
  df <- data.frame(mpg = 1, cyl = 2, disp = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(starts_with("m")) |> collect()
  expect_equal(names(result), "mpg")
})

test_that("select with ends_with", {
  df <- data.frame(mpg = 1, cyl = 2, disp = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(ends_with("g")) |> collect()
  expect_equal(names(result), "mpg")
})

test_that("select with contains", {
  df <- data.frame(mpg = 1, cyl = 2, disp = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(contains("is")) |> collect()
  expect_equal(names(result), "disp")
})

test_that("select with everything()", {
  df <- data.frame(a = 1, b = 2, c = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(everything()) |> collect()
  expect_equal(names(result), c("a", "b", "c"))
})

test_that("select with negation (deselect)", {
  df <- data.frame(a = 1, b = 2, c = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(-b) |> collect()
  expect_equal(names(result), c("a", "c"))
})

test_that("select with renaming", {
  df <- data.frame(a = 1, b = 2)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(alpha = a, b) |> collect()
  expect_equal(names(result), c("alpha", "b"))
  expect_equal(result$alpha, 1)
})

test_that("select with last_col()", {
  df <- data.frame(a = 1, b = 2, c = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(last_col()) |> collect()
  expect_equal(names(result), "c")
})

# --- across in summarise ---

test_that("across in summarise applies function to selected columns", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0),
                   y = c(10.0, 20.0, 30.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(across(c(x, y), mean)) |>
    collect()
  expect_equal(result$x[result$g == "a"], 1.5)
  expect_equal(result$y[result$g == "a"], 15)
})

test_that("across with named function list", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(across(x, list(total = sum, avg = mean))) |>
    collect()
  expect_true("x_total" %in% names(result))
  expect_true("x_avg" %in% names(result))
  expect_equal(result$x_total[result$g == "a"], 3)
  expect_equal(result$x_avg[result$g == "a"], 1.5)
})
