# ifelse() across branch types: the result column type must match the value the
# evaluator produces (common type of the two branches), or int64/double bit
# patterns get reinterpreted. Regression for ifelse(int64_col, <double/NA>).

make_f <- function(df) { f <- tempfile(fileext = ".vtr"); write_vtr(df, f); f }

test_that("ifelse(int64, NA) keeps the int64 values and yields real NAs", {
  f <- make_f(data.frame(x = c(2000L, 0L, NA, 1985L))); on.exit(unlink(f))
  # NA-condition rows are NA (R semantics); the kept values are intact, not the
  # ~4.6e18 garbage from an int64/double type mismatch.
  r <- tbl(f) |> mutate(y = ifelse(x > 0, x, NA_integer_)) |> collect()
  expect_equal(r$y, c(2000, NA, NA, 1985))
  expect_warning(
    tbl(f) |> mutate(y = ifelse(x > 0, x, NA_integer_)) |> collect(),
    regexp = NA)   # no "int64 exceeds 2^53" warning
})

test_that("ifelse(int64, double) promotes to double without corruption", {
  f <- make_f(data.frame(x = c(2000L, 0L, NA, 1985L))); on.exit(unlink(f))
  r <- tbl(f) |> mutate(y = ifelse(x > 0, x, 1.5)) |> collect()
  expect_equal(r$y, c(2000, 1.5, NA, 1985))
})

test_that("ifelse(int64, int) stays int64-valued and exact", {
  f <- make_f(data.frame(x = c(2000L, 0L, NA, 1985L))); on.exit(unlink(f))
  r <- tbl(f) |> mutate(y = ifelse(x > 0, x, 99999L)) |> collect()
  expect_equal(r$y, c(2000, 99999, NA, 1985))
})

test_that("ifelse with string branches is unaffected", {
  f <- make_f(data.frame(x = c(2000L, 0L, NA, 1985L))); on.exit(unlink(f))
  r <- tbl(f) |> mutate(y = ifelse(x > 0, "yes", "no")) |> collect()
  expect_equal(r$y, c("yes", "no", NA, "yes"))
})

test_that("earliest-known-year pattern: unknown maps to NA and sorts last", {
  # the demo idiom now that ifelse(int64, NA) is correct
  df <- data.frame(piece_id = c(1L, 1L, 2L, 2L, 3L, 3L),
                   STATUS_YR = c(2000L, 1990L, 0L, 0L, NA, 1985L))
  f <- make_f(df); on.exit(unlink(f))
  res <- tbl(f) |>
    mutate(year = ifelse(STATUS_YR > 0, STATUS_YR, NA_integer_)) |>
    group_by(piece_id) |>
    slice_min(year, n = 1, with_ties = FALSE) |>
    collect()
  res <- res[order(res$piece_id), ]
  expect_equal(res$STATUS_YR, c(1990, 0, 1985))   # piece 2 stays unknown (0)
})
