# rlang injection (!!, !!!, :=), the .data and .env pronouns, in every NSE verb.

.inj_store <- function() {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(g = c("a", "b", "a", "b", "a"), x = c(5, 1, 4, 2, 3),
                       y = c(10, 20, 30, 40, 50), stringsAsFactors = FALSE), f)
  f
}

test_that("mutate() resolves !!sym, !!value, := names and both pronouns", {
  f <- .inj_store()
  on.exit(unlink(f))
  k <- "x"
  v <- 100
  nm <- "z"
  ref <- c(5, 1, 4, 2, 3) + 100
  expect_equal(collect(mutate(tbl(f), z = !!rlang::sym(k) + !!v))$z, ref)
  expect_equal(collect(mutate(tbl(f), z = .data[[k]] + .env$v))$z, ref)
  expect_equal(collect(mutate(tbl(f), !!nm := x + v))$z, ref)
  exprs <- list(a = rlang::expr(x * 2), b = rlang::expr(y - 1))
  got <- collect(mutate(tbl(f), !!!exprs))
  expect_equal(got$a, c(10, 2, 8, 4, 6))
  expect_equal(got$b, c(9, 19, 29, 39, 49))
})

test_that("transmute() resolves injection and pronouns", {
  f <- .inj_store()
  on.exit(unlink(f))
  k <- "y"
  v <- 2
  got <- collect(transmute(tbl(f), a = !!rlang::sym(k) / !!v, b = .data[[k]] * .env$v))
  expect_equal(names(got), c("a", "b"))
  expect_equal(got$a, c(5, 10, 15, 20, 25))
  expect_equal(got$b, c(20, 40, 60, 80, 100))
})

test_that("filter() resolves injection and pronouns", {
  f <- .inj_store()
  on.exit(unlink(f))
  k <- "x"
  v <- 3
  expect_equal(collect(filter(tbl(f), !!rlang::sym(k) > !!v))$x, c(5, 4))
  expect_equal(collect(filter(tbl(f), .data[[k]] > .env$v))$x, c(5, 4))
  preds <- list(rlang::expr(x > 1), rlang::expr(y < 50))
  expect_equal(collect(filter(tbl(f), !!!preds))$x, c(5, 4, 2))
  # A local variable named like a column still reaches the value via .env.
  x <- 4
  expect_equal(collect(filter(tbl(f), x >= .env$x))$x, c(5, 4))
})

test_that("summarise() resolves injection and pronouns", {
  f <- .inj_store()
  on.exit(unlink(f))
  k <- "y"
  nm <- "total"
  s1 <- collect(summarise(group_by(tbl(f), g), s = sum(!!rlang::sym(k))))
  s2 <- collect(summarise(group_by(tbl(f), g), s = sum(.data[[k]])))
  s3 <- collect(summarise(group_by(tbl(f), g), !!nm := sum(y)))
  expect_equal(s1$s, c(90, 60))
  expect_equal(s2$s, c(90, 60))
  expect_equal(s3$total, c(90, 60))
  v <- 10
  s4 <- collect(summarise(tbl(f), m = max(x) * !!v, e = max(x) * .env$v))
  expect_equal(s4$m, 50)
  expect_equal(s4$e, 50)
})

test_that("arrange() resolves injection and pronouns", {
  f <- .inj_store()
  on.exit(unlink(f))
  k <- "x"
  expect_equal(collect(arrange(tbl(f), !!rlang::sym(k)))$x, 1:5)
  expect_equal(collect(arrange(tbl(f), desc(.data[[k]])))$x, 5:1)
  keys <- rlang::syms(c("g", "x"))
  expect_equal(collect(arrange(tbl(f), !!!keys))$x, c(3, 4, 5, 1, 2))
  v <- -1
  expect_equal(collect(arrange(tbl(f), x * .env$v))$x, 5:1)
  expect_equal(collect(arrange(tbl(f), x * !!v))$x, 5:1)
})

test_that("pull(), slice_min()/slice_max() and tally(wt) take injected columns", {
  f <- .inj_store()
  on.exit(unlink(f))
  k <- "x"
  expect_equal(pull(tbl(f), !!rlang::sym(k)), c(5, 1, 4, 2, 3))
  expect_equal(pull(tbl(f), .data[[k]]), c(5, 1, 4, 2, 3))
  expect_equal(collect(slice_min(tbl(f), !!rlang::sym(k), n = 2))$x, c(1, 2))
  expect_equal(collect(slice_max(tbl(f), .data[[k]], n = 1))$x, 5)
  expect_equal(collect(tally(tbl(f), wt = !!rlang::sym("y")))$n, 150)
  expect_equal(collect(count(tbl(f), g, wt = .data[["y"]]))$n, c(90, 60))
})
