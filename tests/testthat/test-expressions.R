# --- math functions ---

test_that("abs works in mutate", {
  df <- data.frame(x = c(-1.0, 2.0, -3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(a = abs(x)) |> collect()
  expect_equal(result$a, c(1, 2, 3))
})

test_that("sqrt works in mutate", {
  df <- data.frame(x = c(4.0, 9.0, 16.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(s = sqrt(x)) |> collect()
  expect_equal(result$s, c(2, 3, 4))
})

test_that("log and exp are inverses", {
  df <- data.frame(x = c(1.0, 2.0, 10.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(lx = log(x), elx = exp(log(x))) |> collect()
  expect_equal(result$elx, result$x, tolerance = 1e-10)
})

test_that("floor and ceiling work", {
  df <- data.frame(x = c(1.2, 2.8, -1.5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(fl = floor(x), ce = ceiling(x)) |> collect()
  expect_equal(result$fl, c(1, 2, -2))
  expect_equal(result$ce, c(2, 3, -1))
})

test_that("round works", {
  df <- data.frame(x = c(1.3, 2.7, 3.4))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(r = round(x)) |> collect()
  expect_equal(result$r, c(1, 3, 3))
})

test_that("math functions propagate NA", {
  df <- data.frame(x = c(4.0, NA, 9.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(s = sqrt(x)) |> collect()
  expect_equal(result$s[1], 2)
  expect_true(is.na(result$s[2]))
  expect_equal(result$s[3], 3)
})

# --- if_else ---

test_that("if_else works in mutate", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(label = if_else(x > 2, 10.0, 0.0)) |> collect()
  expect_equal(result$label, c(0, 0, 10))
})

test_that("if_else with NA condition returns NA", {
  df <- data.frame(x = c(1.0, NA, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(label = if_else(x > 2, 1.0, 0.0)) |> collect()
  expect_equal(result$label[1], 0)
  expect_true(is.na(result$label[2]))
  expect_equal(result$label[3], 1)
})

# --- %in% ---

test_that("%in% works in filter", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x %in% c(2, 4)) |> collect()
  expect_equal(result$x, c(2, 4))
})

test_that("%in% works with strings", {
  df <- data.frame(s = c("a", "b", "c", "d"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(s %in% c("a", "c")) |> collect()
  expect_equal(result$s, c("a", "c"))
})

# --- between ---

test_that("between works in filter", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(between(x, 2, 4)) |> collect()
  expect_equal(result$x, c(2, 3, 4))
})

# --- type casting ---

test_that("as.numeric works in mutate", {
  df <- data.frame(x = c(1L, 2L, 3L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = as.numeric(x)) |> collect()
  expect_true(is.double(result$y))
})

# --- string functions ---

test_that("tolower and toupper work", {
  df <- data.frame(s = c("Hello", "WORLD"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(lo = tolower(s), up = toupper(s)) |> collect()
  expect_equal(result$lo, c("hello", "world"))
  expect_equal(result$up, c("HELLO", "WORLD"))
})

test_that("trimws removes whitespace", {
  df <- data.frame(s = c("  hello  ", "\tfoo\n"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(t = trimws(s)) |> collect()
  expect_equal(result$t, c("hello", "foo"))
})
