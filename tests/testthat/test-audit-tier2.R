# Regression tests for the Tier-2 audit fixes (2026-07-18): dplyr-compat gaps.

vtr <- function(df) { f <- tempfile(fileext = ".vtr"); write_vtr(df, f); f }

# ---- across() lambdas / formulas ---------------------------------------------

test_that("across() accepts a purrr-style formula lambda", {
  f <- vtr(data.frame(x = c(1, 2, 3), y = c(10, 20, 30))); on.exit(unlink(f))
  r <- collect(mutate(tbl(f), across(c(x, y), ~ .x + 1)))
  expect_equal(r$x, c(2, 3, 4))
  expect_equal(r$y, c(11, 21, 31))
})

test_that("across() lambda with a call and named arg works", {
  f <- vtr(data.frame(g = c("a", "a", "b"), x = c(1, 2, 3))); on.exit(unlink(f))
  r <- collect(summarise(group_by(tbl(f), g), across(x, ~ mean(.x, na.rm = TRUE))))
  expect_setequal(r$x, c(1.5, 3))
})

# ---- window functions --------------------------------------------------------

test_that("min_rank() works as a rank alias", {
  f <- vtr(data.frame(x = c(3, 1, 2))); on.exit(unlink(f))
  r <- collect(mutate(tbl(f), rk = min_rank(x)))
  expect_equal(r$rk, c(3, 1, 2))
})

test_that("window functions accept a compound (nested) argument", {
  f <- vtr(data.frame(x = c(1, 2, 3), y = c(10, 20, 30))); on.exit(unlink(f))
  r <- collect(mutate(tbl(f), cs = cumsum(x + y)))
  expect_equal(r$cs, cumsum(c(11, 22, 33)))
  expect_false(any(grepl("__win_arg", names(r))))          # temp col dropped
  r2 <- collect(mutate(tbl(f), rk = min_rank(desc(x * y))))
  expect_equal(r2$rk, c(3, 2, 1))
})

# ---- count() / tally() -------------------------------------------------------

test_that("count() keeps the existing group_by() keys", {
  f <- vtr(data.frame(g = c(1, 1, 2, 2), b = c("x", "y", "x", "x"))); on.exit(unlink(f))
  r <- collect(count(group_by(tbl(f), g), b))
  expect_true(all(c("g", "b", "n") %in% names(r)))
  expect_equal(nrow(r), 3)                                  # (1,x) (1,y) (2,x)
})

test_that("count(wt=) and tally(wt=) sum weights with na.rm = TRUE", {
  f <- vtr(data.frame(g = c("a", "a", "b"), w = c(1, NA, 3))); on.exit(unlink(f))
  r <- collect(count(tbl(f), g, wt = w))
  expect_setequal(r$n, c(1, 3))                             # NA weight dropped
  r2 <- collect(tally(group_by(tbl(f), g), wt = w))
  expect_setequal(r2$n, c(1, 3))
})

# ---- grepl / gsub / sub ignore.case, perl ------------------------------------

test_that("grepl honours ignore.case in regex and fixed modes", {
  f <- vtr(data.frame(s = c("Foo", "BAR", "baz"))); on.exit(unlink(f))
  expect_equal(nrow(collect(filter(tbl(f), grepl("foo", s, ignore.case = TRUE)))), 1)
  expect_equal(nrow(collect(filter(tbl(f), grepl("BA", s, ignore.case = TRUE, fixed = TRUE)))), 2)
})

test_that("gsub/sub honour ignore.case", {
  f <- vtr(data.frame(s = c("aAbB"))); on.exit(unlink(f))
  expect_equal(collect(mutate(tbl(f), z = gsub("a", "-", s, ignore.case = TRUE)))$z, "--bB")
  expect_equal(collect(mutate(tbl(f), z = sub("a", "-", s, ignore.case = TRUE)))$z, "-AbB")
})

test_that("perl = TRUE is rejected clearly, not silently mis-evaluated", {
  f <- vtr(data.frame(s = c("abc"))); on.exit(unlink(f))
  expect_error(collect(filter(tbl(f), grepl("a", s, perl = TRUE))), "perl")
  expect_error(collect(mutate(tbl(f), z = gsub("a", "b", s, perl = TRUE))), "perl")
})

# ---- grouped slice_head / slice_tail / slice ---------------------------------

test_that("slice_head / slice_tail / slice are group-aware", {
  d <- data.frame(g = c("a", "a", "a", "b", "b"), x = c(1, 2, 3, 4, 5))
  f <- vtr(d); on.exit(unlink(f))
  h <- collect(slice_head(group_by(tbl(f), g), n = 2))
  expect_equal(nrow(h), 4)                                  # 2 per group
  expect_setequal(paste(h$g, h$x), c("a 1", "a 2", "b 4", "b 5"))
  t <- slice_tail(group_by(tbl(f), g), n = 1)
  expect_setequal(paste(t$g, t$x), c("a 3", "b 5"))
  s <- slice(group_by(tbl(f), g), 1:2)
  expect_setequal(paste(s$g, s$x), c("a 1", "a 2", "b 4", "b 5"))
})

test_that("ungrouped slice_head / slice_tail / slice unchanged", {
  f <- vtr(data.frame(x = 1:5)); on.exit(unlink(f))
  expect_equal(collect(slice_head(tbl(f), n = 2))$x, c(1, 2))
  expect_equal(slice_tail(tbl(f), n = 2)$x, c(4, 5))
  expect_equal(slice(tbl(f), -1)$x, c(2, 3, 4, 5))
})
