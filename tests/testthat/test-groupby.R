test_that("group_by + summarise with count", {
  df <- data.frame(g = c("a", "b", "a", "b", "a"), x = 1:5,
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(cnt = n()) |> collect()
  # First-seen order: "a" first, "b" second
  expect_equal(result$g, c("a", "b"))
  expect_equal(result$cnt, c(3, 2))
})

test_that("group_by + summarise with sum", {
  df <- data.frame(g = c("a", "a", "b", "b"), x = c(1.0, 2.0, 3.0, 4.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(total = sum(x)) |> collect()
  expect_equal(result$g, c("a", "b"))
  expect_equal(result$total, c(3, 7))
})

test_that("group_by + summarise with mean", {
  df <- data.frame(g = c("a", "a", "b", "b"), x = c(10.0, 20.0, 30.0, 40.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(avg = mean(x)) |> collect()
  expect_equal(result$avg, c(15, 35))
})

test_that("group_by + summarise with min and max", {
  df <- data.frame(g = c("a", "a", "b", "b"), x = c(5.0, 1.0, 8.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(lo = min(x), hi = max(x)) |>
    collect()
  expect_equal(result$lo, c(1, 3))
  expect_equal(result$hi, c(5, 8))
})

test_that("group_by with NA key values", {
  df <- data.frame(g = c("a", NA, "a", NA), x = c(1.0, 2.0, 3.0, 4.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(total = sum(x)) |> collect()
  expect_equal(nrow(result), 2)
  # "a" group and NA group
  expect_equal(result$total[result$g == "a" & !is.na(result$g)], 4)
  expect_equal(result$total[is.na(result$g)], 6)
})

test_that("multiple grouping columns", {
  df <- data.frame(
    a = c("x", "x", "y", "y"),
    b = c(1L, 2L, 1L, 2L),
    v = c(10.0, 20.0, 30.0, 40.0)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(a, b) |> summarise(s = sum(v)) |> collect()
  expect_equal(nrow(result), 4)
})

test_that("summarise with na.rm", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, NA, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(total = sum(x, na.rm = TRUE)) |>
    collect()
  expect_equal(result$total[result$g == "a"], 1)
  expect_equal(result$total[result$g == "b"], 3)
})

test_that("summarise without na.rm gives NA for all-NA group", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(total = sum(x)) |>
    collect()
  expect_true(is.na(result$total))
})

test_that("string key arena survives multiple resizes", {
  # Initial arena capacity is 64. 200 unique groups forces resizes at 65 and 129.
  # Regression test for UAF in arena_ensure when string data was aliased.
  set.seed(1)
  n <- 2000
  n_groups <- 200
  df <- data.frame(
    g = sample(paste0("grp_", seq_len(n_groups)), n, replace = TRUE),
    x = rnorm(n),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(sx = sum(x), n = n()) |> collect()
  expect_equal(nrow(result), n_groups)
  expect_equal(sum(result$n), n)
  # Verify against R base
  ref <- aggregate(x ~ g, data = df, FUN = sum)
  ref <- ref[match(result$g, ref$g), ]
  expect_equal(result$sx, ref$x, tolerance = 1e-10)

  # Chain a downstream operation on the grouped result to exercise
  # post-resize hash probing with the result keys still intact.
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(f2), add = TRUE)
  write_vtr(result, f2)
  result2 <- tbl(f2) |> filter(n > 5) |> collect()
  expect_true(all(result2$n > 5))
  expect_true(nrow(result2) > 0)
})

test_that("filter then group_by then summarise", {
  df <- data.frame(
    g = c("a", "b", "a", "b", "a"),
    x = c(1.0, 2.0, 3.0, 4.0, 5.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    filter(x > 2) |>
    group_by(g) |>
    summarise(s = sum(x)) |>
    collect()
  expect_equal(result$s[result$g == "a"], 8)
  expect_equal(result$s[result$g == "b"], 4)
})
