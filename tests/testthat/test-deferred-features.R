# Regression tests for the previously-deferred features finished in this pass:
#   - bounded (external sort-merge) diff_vtr
#   - string first()/last() aggregates
#   - string lag()/lead()
#   - average rank (base::rank ties.method = "average")
#   - n() inside mutate() (partition/group size)
#   - post-aggregation summarise() expressions (incl. sequential references)

vtr_tmp <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

# ── string first()/last() ──────────────────────────────────────────────────

test_that("first()/last() work on a string column", {
  f <- vtr_tmp(data.frame(g = c(1L, 1L, 1L, 2L, 2L),
                          s = c("a", "b", "c", "x", "y"),
                          v = c(3, 1, 2, 2, 1), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> group_by(g) |> summarise(f1 = first(s), l1 = last(s)) |> collect()
  r <- r[order(r$g), ]
  expect_identical(r$f1, c("a", "x"))
  expect_identical(r$l1, c("c", "y"))

  r2 <- tbl(f) |> summarise(f1 = first(s), l1 = last(s)) |> collect()
  expect_identical(r2$f1, "a")
  expect_identical(r2$l1, "y")
})

test_that("first()/last() on a string column respect NA (default keeps position)", {
  f <- vtr_tmp(data.frame(g = c(1L, 1L, 2L, 2L),
                          s = c("p", NA, NA, "r"), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> group_by(g) |> summarise(fd = first(s), ld = last(s)) |> collect()
  r <- r[order(r$g), ]
  expect_identical(r$fd, c("p", NA))   # g1 first = p ; g2 first = NA
  expect_identical(r$ld, c(NA, "r"))   # g1 last  = NA; g2 last  = r
})

# ── string lag()/lead() ────────────────────────────────────────────────────

test_that("lag()/lead() preserve a string column (ungrouped, streaming)", {
  f <- vtr_tmp(data.frame(s = c("a", "b", "c", "d"), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(p = lag(s), n2 = lead(s), p2 = lag(s, 2)) |> collect()
  expect_identical(r$p,  c(NA, "a", "b", "c"))
  expect_identical(r$n2, c("b", "c", "d", NA))
  expect_identical(r$p2, c(NA, NA, "a", "b"))
})

test_that("grouped string lag()/lead() reset per group", {
  f <- vtr_tmp(data.frame(g = c(1L, 1L, 1L, 2L, 2L),
                          s = c("a", "b", "c", "x", "y"),
                          v = c(1, 2, 3, 1, 2), stringsAsFactors = FALSE))
  on.exit(unlink(f))
  r <- tbl(f) |> group_by(g) |> mutate(p = lag(s), n2 = lead(s)) |> collect()
  r <- r[order(r$g, r$v), ]
  expect_identical(r$p,  c(NA, "a", "b", NA, "x"))
  expect_identical(r$n2, c("b", "c", NA, "y", NA))
})

test_that("numeric lag() is unchanged by the typed path", {
  f <- vtr_tmp(data.frame(v = c(10, 20, 30)))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(p = lag(v)) |> collect()
  expect_identical(r$p, c(NA, 10, 20))
})

# ── average rank ───────────────────────────────────────────────────────────

test_that("rank(ties.method = 'average') matches base::rank", {
  set.seed(7)
  x <- c(2, 2, 1, 3, 3, 3, 5)
  f <- vtr_tmp(data.frame(x = x))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(a = rank(x, ties.method = "average")) |> collect()
  expect_equal(r$a, as.numeric(rank(x, ties.method = "average")))
})

test_that("grouped average rank matches base::rank per group", {
  g <- rep(1:3, each = 4)
  x <- c(1, 1, 2, 3,  5, 5, 5, 4,  9, 8, 8, 7)
  f <- vtr_tmp(data.frame(g = g, x = x))
  on.exit(unlink(f))
  r <- tbl(f) |> group_by(g) |> mutate(a = rank(x, ties.method = "average")) |> collect()
  r <- r[order(r$g, r$x), ]
  exp <- ave(x, g, FUN = function(z) rank(z, ties.method = "average"))[order(g, x)]
  expect_equal(as.numeric(r$a), as.numeric(exp))
})

test_that("bare rank() stays min-rank; unknown ties.method errors", {
  x <- c(2, 2, 1, 3)
  f <- vtr_tmp(data.frame(x = x))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(a = rank(x), b = rank(x, ties.method = "min")) |> collect()
  expect_equal(r$a, as.numeric(rank(x, ties.method = "min")))
  expect_equal(r$b, as.numeric(rank(x, ties.method = "min")))
  expect_error(tbl(f) |> mutate(a = rank(x, ties.method = "max")) |> collect(),
               "not supported")
})

# ── n() in mutate() ────────────────────────────────────────────────────────

test_that("n() in mutate() gives the partition/group size", {
  g <- rep(1:3, c(4, 3, 5))
  f <- vtr_tmp(data.frame(g = g, x = seq_along(g) * 1.0))
  on.exit(unlink(f))
  r <- tbl(f) |> mutate(nn = n()) |> collect()
  expect_true(all(r$nn == length(g)))

  r <- tbl(f) |> group_by(g) |> mutate(nn = n()) |> collect()
  r <- r[order(r$g, r$x), ]
  expect_equal(as.numeric(r$nn), as.numeric(ave(g, g, FUN = length)[order(g, seq_along(g))]))

  # still works as an aggregate in summarise()
  r <- tbl(f) |> group_by(g) |> summarise(cnt = n()) |> collect()
  expect_equal(as.numeric(r$cnt[order(r$g)]), c(4, 3, 5))
})

# ── post-aggregation summarise() ───────────────────────────────────────────

test_that("summarise() supports expressions over aggregates", {
  g <- c(1L, 1L, 1L, 2L, 2L); x <- c(1, 2, 3, 4, 6)
  f <- vtr_tmp(data.frame(g = g, x = x))
  on.exit(unlink(f))
  em <- as.numeric(tapply(x, g, mean)); es <- as.numeric(tapply(x, g, sum))

  r <- tbl(f) |> group_by(g) |> summarise(m2 = mean(x) + 1, rr = sum(x) / mean(x)) |>
    collect()
  r <- r[order(r$g), ]
  expect_equal(r$m2, em + 1)
  expect_equal(r$rr, es / em)
})

test_that("summarise() evaluates sequentially (later output refs earlier)", {
  g <- c(1L, 1L, 1L, 2L, 2L); x <- c(1, 2, 3, 4, 6)
  f <- vtr_tmp(data.frame(g = g, x = x))
  on.exit(unlink(f))
  em <- as.numeric(tapply(x, g, mean)); es <- as.numeric(tapply(x, g, sum))

  r <- tbl(f) |> group_by(g) |>
    summarise(m = mean(x), m2 = m * 2, s = sum(x), r = m2 / s) |> collect()
  r <- r[order(r$g), ]
  expect_identical(names(r), c("g", "m", "m2", "s", "r"))
  expect_equal(r$m,  em)
  expect_equal(r$m2, em * 2)
  expect_equal(r$s,  es)
  expect_equal(r$r,  (em * 2) / es)
})

test_that("post-aggregation expressions can reference a group key", {
  g <- c(1L, 1L, 2L, 2L); x <- c(2, 4, 6, 8)
  f <- vtr_tmp(data.frame(g = g, x = x))
  on.exit(unlink(f))
  r <- tbl(f) |> group_by(g) |> summarise(ratio = mean(x) / g) |> collect()
  r <- r[order(r$g), ]
  expect_equal(r$ratio, as.numeric(tapply(x, g, mean)) / c(1, 2))
})
