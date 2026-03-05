# --- write_csv tests ---

test_that("write_csv from node round-trips all types", {
  df <- data.frame(a = c(1L, 2L, 3L), b = c(1.5, 2.5, 3.5),
                   c = c(TRUE, FALSE, TRUE),
                   d = c("x", "y", "z"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_equal(result$a, c(1, 2, 3))
  expect_equal(result$b, c(1.5, 2.5, 3.5))
  expect_equal(result$c, c(TRUE, FALSE, TRUE))
  expect_equal(result$d, c("x", "y", "z"))
})

test_that("write_csv handles NAs", {
  df <- data.frame(a = c(1L, NA, 3L), b = c(NA, 2.5, NA),
                   c = c(TRUE, NA, FALSE),
                   d = c(NA, "y", NA), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_true(is.na(result$a[2]))
  expect_true(is.na(result$b[1]))
  expect_true(is.na(result$c[2]))
  expect_true(is.na(result$d[1]))
  expect_equal(result$a[1], 1)
  expect_equal(result$d[2], "y")
})

test_that("write_csv quotes strings with commas", {
  df <- data.frame(x = c("a,b", "c\"d", "normal"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_equal(result$x, c("a,b", "c\"d", "normal"))
})

test_that("write_csv quotes strings with newlines", {
  df <- data.frame(x = c("line1\nline2", "ok"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_equal(result$x, c("line1\nline2", "ok"))
})

test_that("write_csv from data.frame works", {
  df <- data.frame(a = c(1.0, 2.0), b = c("x", "y"), stringsAsFactors = FALSE)
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(csv))
  write_csv(df, csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_equal(result$a, c(1, 2))
  expect_equal(result$b, c("x", "y"))
})

test_that("write_csv streams after filter", {
  df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> filter(x > 7) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_equal(nrow(result), 3)
  expect_equal(result$y, c("h", "i", "j"))
})

test_that("write_csv streams after mutate", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> mutate(y = x * 10) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  expect_equal(result$y, c(10, 20, 30))
})

test_that("write_csv streams after summarise", {
  df <- data.frame(g = c("a", "a", "b"), v = c(1.0, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> group_by(g) |> summarise(s = sum(v)) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE)
  result <- result[order(result$g), ]
  expect_equal(result$g, c("a", "b"))
  expect_equal(result$s, c(3, 3))
})

test_that("write_csv empty result", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> filter(x > 100) |> write_csv(csv)
  lines <- readLines(csv)
  expect_equal(length(lines), 1)  # header only
  expect_equal(lines[1], "x")
})

test_that("write_csv column names with special chars are quoted", {
  df <- data.frame(1.0, 2.0)
  names(df) <- c("a,b", "c d")
  f <- tempfile(fileext = ".vtr")
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, csv)))
  write_vtr(df, f)
  tbl(f) |> write_csv(csv)
  result <- read.csv(csv, stringsAsFactors = FALSE, check.names = FALSE)
  expect_true("a,b" %in% names(result))
  expect_true("c d" %in% names(result))
})
