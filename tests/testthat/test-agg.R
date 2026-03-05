# --- NA poisoning: without na.rm, any NA in group poisons the result ---

test_that("sum without na.rm returns NA when group has NAs", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, NA, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sum(x)) |> collect()
  expect_true(is.na(result$s[result$g == "a"]))
  expect_equal(result$s[result$g == "b"], 3)
})

test_that("mean without na.rm returns NA when group has NAs", {
  df <- data.frame(g = c("a", "a", "b", "b"),
                   x = c(10.0, NA, 3.0, 7.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(m = mean(x)) |> collect()
  expect_true(is.na(result$m[result$g == "a"]))
  expect_equal(result$m[result$g == "b"], 5)
})

test_that("min/max without na.rm returns NA when group has NAs", {
  df <- data.frame(g = c("a", "a"), x = c(5.0, NA), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  r <- tbl(f) |> group_by(g) |> summarise(lo = min(x), hi = max(x)) |> collect()
  expect_true(is.na(r$lo))
  expect_true(is.na(r$hi))
})

# --- na.rm = TRUE on all-NA groups (R-matching semantics) ---

test_that("sum(na.rm=TRUE) on all-NA group returns 0", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(s = sum(x, na.rm = TRUE)) |> collect()
  expect_equal(result$s, 0)
})

test_that("mean(na.rm=TRUE) on all-NA group returns NaN", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(m = mean(x, na.rm = TRUE)) |> collect()
  expect_true(is.nan(result$m))
})

test_that("min(na.rm=TRUE) on all-NA group returns Inf", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  expect_warning(
    result <- tbl(f) |>
      group_by(g) |>
      summarise(lo = min(x, na.rm = TRUE)) |> collect(),
    "no non-missing"
  )
  expect_equal(result$lo, Inf)
})

test_that("max(na.rm=TRUE) on all-NA group returns -Inf", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  expect_warning(
    result <- tbl(f) |>
      group_by(g) |>
      summarise(hi = max(x, na.rm = TRUE)) |> collect(),
    "no non-missing"
  )
  expect_equal(result$hi, -Inf)
})

# --- na.rm = TRUE with mixed valid + NA values ---

test_that("sum(na.rm=TRUE) skips NAs and sums valid values", {
  df <- data.frame(g = c("a", "a", "a"),
                   x = c(1.0, NA, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(s = sum(x, na.rm = TRUE)) |> collect()
  expect_equal(result$s, 4)
})

test_that("mean(na.rm=TRUE) computes mean of non-NA values only", {
  df <- data.frame(g = c("a", "a", "a"),
                   x = c(10.0, NA, 20.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(m = mean(x, na.rm = TRUE)) |> collect()
  expect_equal(result$m, 15)
})

# --- n() counts all rows including NAs ---

test_that("n() counts all rows including NA values", {
  df <- data.frame(g = c("a", "a", "a"),
                   x = c(1.0, NA, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(cnt = n()) |> collect()
  expect_equal(result$cnt, 3)
})

# --- Integer aggregations ---

test_that("sum of integer column returns correct double", {
  df <- data.frame(g = c("a", "a", "b"), x = c(100L, 200L, 50L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sum(x)) |> collect()
  expect_equal(result$s[result$g == "a"], 300)
  expect_equal(result$s[result$g == "b"], 50)
})
