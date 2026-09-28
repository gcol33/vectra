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

test_that("summarise accepts namespace-qualified aggregation calls", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  # vectra::n() and vectra::sum() should work the same as bare n() / sum()
  r1 <- tbl(f) |> vectra::summarize(n = vectra::n()) |> collect()
  expect_equal(r1$n, 3)

  r2 <- tbl(f) |> group_by(g) |>
    summarise(cnt = vectra::n(), total = vectra::sum(x)) |> collect()
  expect_equal(r2$cnt, c(2, 1))
  expect_equal(r2$total, c(3, 3))

  # Unknown namespace-qualified function still errors with a clean message
  expect_error(
    tbl(f) |> summarise(z = vectra::nope(x)) |> collect(),
    "unknown aggregation function: nope"
  )
})

# --- programmatic grouping (#24) ---

.gb_store <- function() {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(g = c("a", "b", "a", "b", "a"), h = c(1, 1, 2, 2, 1),
                       x = 1:5, stringsAsFactors = FALSE), f)
  f
}

.gb_sorted <- function(d) d[do.call(order, unname(as.list(d))), , drop = FALSE]

test_that("group_by accepts injected symbols and the .data pronoun", {
  f <- .gb_store()
  on.exit(unlink(f))
  ref <- tbl(f) |> group_by(g, h) |> summarise(n = n()) |> collect()
  k <- "g"
  ks <- c("g", "h")
  one <- tbl(f) |> group_by(!!rlang::sym(k)) |> summarise(n = n()) |> collect()
  expect_equal(one$g, c("a", "b"))
  expect_equal(one$n, c(3, 2))
  expect_equal(tbl(f) |> group_by(!!!rlang::syms(ks)) |> summarise(n = n()) |>
                 collect(), ref)
  expect_equal(tbl(f) |> group_by(.data[[k]], .data$h) |> summarise(n = n()) |>
                 collect(), ref)
  expect_equal(tbl(f) |> group_by(across(tidyselect::all_of(ks))) |>
                 summarise(n = n()) |> collect(), ref)
  expect_equal(tbl(f) |> group_by(pick(g, h)) |> summarise(n = n()) |>
                 collect(), ref)
})

test_that("group_by with an expression adds a computed grouping column", {
  f <- .gb_store()
  on.exit(unlink(f))
  named <- tbl(f) |> group_by(odd = x %% 2) |> summarise(s = sum(x)) |> collect()
  expect_equal(.gb_sorted(as.data.frame(named)),
               data.frame(odd = c(0, 1), s = c(6, 9)), ignore_attr = TRUE)
  unnamed <- tbl(f) |> group_by(x %% 2) |> summarise(n = n()) |> collect()
  expect_equal(names(unnamed), c("x%%2", "n"))
  renamed <- tbl(f) |> group_by(grp = g) |> summarise(n = n()) |> collect()
  expect_equal(renamed$grp, c("a", "b"))
})

test_that("group_by .add keeps the existing grouping", {
  f <- .gb_store()
  on.exit(unlink(f))
  q <- tbl(f) |> group_by(g) |> group_by(h, .add = TRUE)
  expect_equal(q$.groups, c("g", "h"))
  expect_equal((tbl(f) |> group_by(g) |> group_by(h))$.groups, "h")
  expect_null((tbl(f) |> group_by())$.groups)
})

test_that("group_by names an unknown column in its error", {
  f <- .gb_store()
  on.exit(unlink(f))
  expect_error(tbl(f) |> group_by(!!rlang::sym("nope")), "column `nope` not found")
  expect_error(tbl(f) |> group_by(.data[["nope"]]), "column `nope` not found")
  expect_error(tbl(f) |> group_by(across(g, toupper)), "not supported for grouping")
})

test_that("count shares group_by's argument resolution", {
  f <- .gb_store()
  on.exit(unlink(f))
  k <- "g"
  r <- tbl(f) |> count(!!rlang::sym(k)) |> collect()
  expect_equal(r$g, c("a", "b"))
  expect_equal(r$n, c(3, 2))
  r2 <- tbl(f) |> count(odd = x %% 2) |> collect()
  expect_equal(sort(r2$n), c(2, 3))
})
