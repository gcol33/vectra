# Regression tests for the 2026-07-18 "fix all gaps" audit pass.

vtr_tmp <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

# ---- coercion / string ------------------------------------------------------

test_that("as.character of a double keeps full precision (15 sig digits)", {
  f <- vtr_tmp(data.frame(x = c(pi, 123456789, 0.1)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(s = as.character(x)) |> collect()
  expect_equal(r$s, as.character(c(pi, 123456789, 0.1)))
})

test_that("as.integer/as.numeric/as.logical coercions match base R", {
  f <- vtr_tmp(data.frame(d = c(2.7, -3.2, 0), s = c("10", "x", "3.9"),
                          stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(i = as.integer(d), b = as.logical(d),
                        sn = as.numeric(s)) |> collect()
  expect_equal(r$i, c(2, -3, 0))
  expect_equal(r$b, c(TRUE, TRUE, FALSE))
  expect_equal(r$sn, c(10, NA, 3.9))
})

test_that("paste with NA stringifies to 'NA'", {
  f <- vtr_tmp(data.frame(a = c("x", NA), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(p = paste0(a, "!")) |> collect()
  expect_equal(r$p, c("x!", "NA!"))
})

test_that("pmin/pmax on a logical column does not corrupt memory", {
  f <- vtr_tmp(data.frame(b = c(TRUE, FALSE, TRUE), y = c(0.5, 0.5, 0.5)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(m = pmin(b, y)) |> collect()
  expect_equal(r$m, pmin(c(1, 0, 1), 0.5))
})

# ---- aggregation ------------------------------------------------------------

test_that("first/last return the literal first/last element, NA-tolerant", {
  f <- vtr_tmp(data.frame(g = c(1, 1, 1, 2, 2), x = c(1, NA, 3, NA, 5)))
  on.exit(unlink(f))
  r <- tbl(f) |> group_by(g) |> summarise(f = first(x), l = last(x)) |>
    collect()
  r <- r[order(r$g), ]
  expect_equal(r$f, c(1, NA))   # group 2's first element is NA
  expect_equal(r$l, c(3, 5))
})

# ---- arrange ----------------------------------------------------------------

test_that("arrange(-x) sorts descending and NA sorts last", {
  f <- vtr_tmp(data.frame(x = c(3, 1, NA, 2)))
  on.exit(unlink(f))
  r <- tbl(f) |> arrange(-x) |> collect()
  expect_equal(r$x, c(3, 2, 1, NA))
  r2 <- tbl(f) |> arrange(desc(x)) |> collect()
  expect_equal(r2$x, c(3, 2, 1, NA))
})

# ---- .by --------------------------------------------------------------------

test_that(".by groups a summarise and returns ungrouped", {
  f <- vtr_tmp(data.frame(g = c(1, 1, 2, 2), x = c(1, 2, 3, 4)))
  on.exit(unlink(f))
  r <- tbl(f) |> summarise(m = mean(x), .by = g) |> collect()
  r <- r[order(r$g), ]
  expect_equal(r$m, c(1.5, 3.5))
})

test_that(".by partitions a windowed mutate", {
  f <- vtr_tmp(data.frame(g = c(1, 1, 1, 2, 2), x = c(5, 6, 7, 8, 9)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(rn = row_number(), .by = g) |> collect()
  expect_equal(r$rn, c(1, 2, 3, 1, 2))
  expect_false(".by" %in% names(r))
})

# ---- if_any / if_all --------------------------------------------------------

test_that("if_all / if_any expand in filter", {
  # row1 both >0, row2 both <0, row3 mixed.
  f <- vtr_tmp(data.frame(a = c(1, -1, 2), b = c(1, -1, -2)))
  on.exit(unlink(f))
  ra <- tbl(f) |> filter(if_all(c(a, b), ~ .x > 0)) |> collect()
  expect_equal(nrow(ra), 1L)                     # only row1
  ro <- tbl(f) |> filter(if_any(c(a, b), ~ .x > 0)) |> collect()
  expect_equal(nrow(ro), 2L)                     # row1 and row3
})

# ---- across -----------------------------------------------------------------

test_that("across supports anonymous lambdas and {.fn} in .names", {
  f <- vtr_tmp(data.frame(a = c(1, 2), b = c(3, 4)))
  on.exit(unlink(f))
  r <- tbl(f) |> summarise(across(c(a, b), \(x) mean(x), .names = "{.col}_{.fn}")) |>
    collect()
  expect_true(all(c("a_1", "b_1") %in% names(r)))
  expect_equal(r$a_1, 1.5)
})

# ---- .data pronoun ----------------------------------------------------------

test_that(".data[[var]] resolves to a column", {
  f <- vtr_tmp(data.frame(x = c(1, 2, 3)))
  on.exit(unlink(f))
  var <- "x"
  r <- tbl(f) |> filter(.data[[var]] > 1) |> collect()
  expect_equal(r$x, c(2, 3))
})

# ---- grouped summarise streams its output -----------------------------------

test_that("high-cardinality summarise emits bounded output batches", {
  n <- 200000L                                   # > GROUP_AGG_EMIT (131072)
  f <- vtr_tmp(data.frame(g = seq_len(n), x = as.numeric(seq_len(n))))
  on.exit(unlink(f))
  # correctness: one group per key, sum == the value itself
  r <- tbl(f) |> group_by(g) |> summarise(s = sum(x)) |> collect()
  expect_equal(nrow(r), n)
  r <- r[order(r$g), ]
  expect_equal(r$s, as.numeric(seq_len(n)))

  # bounded: output arrives in multiple chunks, none larger than the emit cap
  n_chunks <- 0L; max_rows <- 0L
  total <- collect_chunked(tbl(f) |> group_by(g) |> summarise(s = sum(x)),
                           function(acc, chunk) {
                             n_chunks <<- n_chunks + 1L
                             max_rows <<- max(max_rows, nrow(chunk))
                             acc + nrow(chunk)
                           }, 0)
  expect_equal(total, n)
  expect_gt(n_chunks, 1L)
  expect_lte(max_rows, 131072L)
})

# ---- bind_rows --------------------------------------------------------------

test_that("bind_rows splices a list and .id is character", {
  f1 <- vtr_tmp(data.frame(x = c(1, 2)))
  f2 <- vtr_tmp(data.frame(x = c(3, 4)))
  on.exit(unlink(c(f1, f2)))
  r <- bind_rows(list(tbl(f1), tbl(f2)), .id = "src")
  expect_equal(nrow(r), 4L)
  expect_type(r$src, "character")
})

# ---- 2026-07-18 follow-up: gaps found reviewing the 0.11.6 pass -------------

test_that("unary math on a logical column coerces (no over-read)", {
  f <- vtr_tmp(data.frame(a = c(3, 1, 2), b = c(1, 2, 2)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(s = sqrt(a > b), r = round(a > b)) |> collect()
  expect_equal(r$s, sqrt(c(1, 0, 0)))
  expect_equal(r$r, c(1, 0, 0))
})

test_that("as.character of Inf/-Inf matches base R (not lowercase)", {
  f <- vtr_tmp(data.frame(x = c(Inf, -Inf, 1.5)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(s = as.character(x)) |> collect()
  expect_equal(r$s, c("Inf", "-Inf", "1.5"))
})

test_that("substr with a huge negative start does not overflow/crash", {
  f <- vtr_tmp(data.frame(x = c("hello", "world"), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(s = substr(x, -1e20, 3)) |> collect()
  expect_equal(r$s, c("hel", "wor"))
})

test_that("select()/across() resolve all_of() from an external variable", {
  f <- vtr_tmp(data.frame(a = c(1, 2), b = c(3, 4), c = c(5, 6)))
  on.exit(unlink(f))
  v <- c("a", "b")
  r <- tbl(f) |> select(all_of(v)) |> collect()
  expect_equal(names(r), c("a", "b"))
  cols <- c("a", "b")
  s <- tbl(f) |> summarise(across(all_of(cols), \(x) sum(x))) |> collect()
  expect_equal(s$a, 3); expect_equal(s$b, 7)
})

test_that("if_any/if_all accept an anonymous \\(x) lambda", {
  f <- vtr_tmp(data.frame(a = c(1, -1, 2), b = c(1, -1, -2)))
  on.exit(unlink(f))
  ra <- tbl(f) |> filter(if_all(c(a, b), \(x) x > 0)) |> collect()
  expect_equal(nrow(ra), 1L)
  ro <- tbl(f) |> filter(if_any(c(a, b), \(x) x > 0)) |> collect()
  expect_equal(nrow(ro), 2L)
})

test_that("a window can reference a column created earlier in the same mutate", {
  f <- vtr_tmp(data.frame(a = c(1, 2, 3), b = c(0, 0, 0)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(z = a + b, rn = row_number(desc(z))) |> collect()
  expect_equal(r$z, c(1, 2, 3))
  expect_equal(r$rn, c(3, 2, 1))
})

test_that("across() errors on a duplicate output name instead of dropping", {
  f <- vtr_tmp(data.frame(a = c(1, 2)))
  on.exit(unlink(f))
  expect_error(
    tbl(f) |> summarise(across(a, list(sum, mean), .names = "{.col}")) |> collect(),
    "unique|duplicate"
  )
})

test_that("arrange() sorts by an expression", {
  f <- vtr_tmp(data.frame(x = c(1, 2, 3), y = c(3, 1, 0)))
  on.exit(unlink(f))
  r <- tbl(f) |> arrange(x + y) |> collect()          # sums 4,3,3 -> asc
  expect_equal(r$x, c(2, 3, 1))
  r2 <- tbl(f) |> arrange(desc(x * 2)) |> collect()
  expect_equal(r2$x, c(3, 2, 1))
})

test_that("median()/n_distinct() accept the .data pronoun and expressions", {
  f <- vtr_tmp(data.frame(g = c(1, 1, 2, 2), x = c(1, 3, 5, 9), y = c(1, 1, 1, 1)))
  on.exit(unlink(f))
  var <- "x"
  r <- tbl(f) |> group_by(g) |>
    summarise(m = median(.data[[var]]), s = median(x + y)) |> collect()
  r <- r[order(r$g), ]
  expect_equal(r$m, c(2, 7))
  expect_equal(r$s, c(3, 8))
})

test_that("summarise() rejects .keep", {
  f <- vtr_tmp(data.frame(g = c(1, 1), s = c("a", "b"), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  expect_error(tbl(f) |> summarise(n = sum(g), .keep = "all"), "\\.keep")
})
