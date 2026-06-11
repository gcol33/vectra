# The offload functor and its laws. offload() must be the identity on values
# (only the memory/cost profile changes), replay must be repeatable, and a
# partitioned monoidal reduce must equal the single-pass fold. One verb: no `by`
# gives a replay-cache node, `by` gives a partition (a list of shard nodes).

test_that("offload is the identity on values", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  full <- tbl(f) |> filter(cyl > 4) |> select(mpg, wt, hp) |> collect()
  spilled <- offload(tbl(f) |> filter(cyl > 4) |> select(mpg, wt, hp))
  expect_equal(collect(spilled), full)
  expect_s3_class(spilled, "vectra_offload")
  expect_s3_class(spilled, "vectra_node")
})

test_that("offload preserves string, factor, and Date columns", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(
    s = rep(c("alpha", "beta", "gamma"), length.out = 120),
    g = factor(rep(c("lo", "hi"), length.out = 120), levels = c("lo", "hi")),
    d = as.Date("2020-01-01") + 0:119,
    v = as.double(1:120),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 25)

  full <- tbl(f) |> collect()
  got  <- collect(offload(tbl(f)))
  expect_equal(got$s, full$s)
  expect_identical(levels(got$g), levels(full$g))
  expect_equal(as.character(got$g), as.character(full$g))
  expect_s3_class(got$d, "Date")
  expect_equal(got$d, full$d)
  expect_equal(got$v, full$v)
})

test_that("an offloaded node reports its replay-cache grade", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)
  expect_output(print(offload(tbl(f))), "replay cache")
  expect_output(print(tbl(f)), "vectra query node")   # plain node, no grade tier
})

test_that("chunk_feeder replays an offloaded node on every pass", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  feed <- chunk_feeder(offload(tbl(f) |> select(mpg, wt, hp)))
  drain <- function() {
    feed(reset = TRUE)
    rows <- 0L
    while (!is.null(ch <- feed(reset = FALSE))) rows <- rows + nrow(ch)
    rows
  }
  expect_equal(drain(), nrow(mtcars))
  expect_equal(drain(), nrow(mtcars))   # repeatable from disk
})

test_that("offload feeds bigglm identically to the raw query", {
  skip_if_not_installed("biglm")
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  set.seed(7)
  n <- 1500
  bio1 <- rnorm(n); bio12 <- rnorm(n)
  presence <- rbinom(n, 1, plogis(-0.3 + 1.2 * bio1 - 0.8 * bio12))
  write.csv(data.frame(presence = as.double(presence), bio1 = bio1,
                       bio12 = bio12), f, row.names = FALSE)

  src_raw <- function() tbl_csv(f, batch_size = 256) |>
    select(presence, bio1, bio12)
  s <- offload(tbl_csv(f, batch_size = 256) |> select(presence, bio1, bio12))

  fit_raw <- biglm::bigglm(presence ~ bio1 + bio12, data = chunk_feeder(src_raw),
                           family = binomial(), maxit = 25)
  fit_off <- biglm::bigglm(presence ~ bio1 + bio12, data = chunk_feeder(s),
                           family = binomial(), maxit = 25)
  expect_equal(unname(coef(fit_off)), unname(coef(fit_raw)), tolerance = 1e-8)
})

test_that("offload(by=) on a discrete key is a true partition (list-like)", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(g = rep(c("a", "b", "c"), length.out = 150),
                   x = as.double(1:150), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 20)

  p <- offload(tbl(f), by = "g")
  expect_s3_class(p, "vectra_partition")
  expect_setequal(names(p), c("a", "b", "c"))     # names() are the keys
  expect_equal(length(p), 3L)

  rebuilt <- do.call(rbind, lapply(p, collect))   # list idioms work
  expect_equal(sort(rebuilt$x), sort(df$x))        # union reproduces input
  for (i in seq_along(p))
    expect_equal(length(unique(collect(p[[i]])$g)), 1L)
})

test_that("offload(by=) auto-range-partitions a continuous key", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  set.seed(5)
  df <- data.frame(x = rnorm(400), y = as.double(1:400))
  write_vtr(df, f, batch_size = 32)

  p <- offload(tbl(f), by = "x")                  # double -> range (auto)
  expect_gt(length(p), 1L)
  rebuilt <- do.call(rbind, lapply(p, collect))
  expect_equal(sort(rebuilt$y), sort(df$y))
})

test_that("offload(by=, method='hash') co-locates keys and loses no rows", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(g = rep(letters[1:7], length.out = 350),
                   x = as.double(1:350), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 16)

  p <- offload(tbl(f), by = "g", method = "hash", n = 3)
  expect_lte(length(p), 3L)
  rebuilt <- do.call(rbind, lapply(p, collect))
  expect_equal(sort(rebuilt$x), sort(df$x))
  # each distinct key lands in exactly one shard (co-location)
  shard_of <- unlist(lapply(seq_along(p), function(i)
    setNames(rep(i, length(unique(collect(p[[i]])$g))),
             unique(collect(p[[i]])$g))))
  expect_equal(length(shard_of), length(unique(df$g)))
})

test_that("multi-flush routing (tiny budget) still reproduces the input", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(g = rep(c("a", "b"), length.out = 500),
                   x = as.double(1:500), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 25)

  # Force many flushes so shards become multi-run concats.
  old <- options(vectra.partition_budget = 50)
  on.exit(options(old), add = TRUE)
  p <- offload(tbl(f), by = "g")
  rebuilt <- do.call(rbind, lapply(p, collect))
  expect_equal(sort(rebuilt$x), sort(df$x))
  expect_equal(sum(attr(p, ".counts")), nrow(df))
})

test_that("partitioned monoidal reduce equals the single-pass fold", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(g = rep(c("a", "b", "c", "d"), length.out = 200),
                   x = as.double(1:200), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 16)

  p <- offload(tbl(f), by = "g")
  streamed <- collect_chunked(p, function(acc, chunk) acc + sum(chunk$x),
                              .init = 0, combine = `+`)
  single <- collect_chunked(tbl(f), function(acc, chunk) acc + sum(chunk$x),
                            .init = 0)
  expect_equal(streamed, sum(df$x))
  expect_equal(streamed, single)
})

test_that("partitioned X'X reduce reproduces lm() (monoid law, order-free)", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  set.seed(42)
  df <- data.frame(g = rep(c("a", "b", "c"), length.out = 600),
                   wt = rnorm(600, 3, 1), hp = rnorm(600, 150, 40),
                   stringsAsFactors = FALSE)
  df$mpg <- 30 - 5 * df$wt - 0.05 * df$hp + rnorm(600, 0, 1)
  write_vtr(df, f, batch_size = 50)

  accumulate <- function(acc, chunk) {
    X <- cbind(1, chunk$wt, chunk$hp); y <- chunk$mpg
    list(XtX = acc$XtX + crossprod(X), Xty = acc$Xty + crossprod(X, y))
  }
  combine <- function(a, b) list(XtX = a$XtX + b$XtX, Xty = a$Xty + b$Xty)

  p <- offload(tbl(f) |> select(g, mpg, wt, hp), by = "g")
  acc <- collect_chunked(p, accumulate,
                         .init = list(XtX = matrix(0, 3, 3), Xty = matrix(0, 3, 1)),
                         combine = combine, commutative = TRUE)
  beta_part <- drop(solve(acc$XtX, acc$Xty))
  beta_lm <- unname(coef(lm(mpg ~ wt + hp, data = df)))
  expect_equal(beta_part, beta_lm, tolerance = 1e-8)
})

test_that("offload validates its arguments", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  expect_error(offload(mtcars), "vectra_node")
  expect_error(offload(tbl(f), by = c("a", "b")), "single column")
  expect_error(offload(tbl(f), by = "nope"), "not found")

  expect_error(chunk_feeder(42), "function")
  expect_error(chunk_feeder(tbl(f)), "offload")          # plain node rejected
  expect_error(chunk_feeder(offload(tbl(f))), NA)        # offloaded accepted

  p <- offload(tbl(f), by = "cyl")
  expect_error(collect_chunked(p, function(a, c) a + nrow(c), .init = 0L),
               "combine")
})

# group_map / group_modify: per-shard application over a partition. group_map
# must fit each shard exactly as lm() on that subset (recovery), key the result
# list by shard, and forward extra args; group_modify must rebind per-shard
# frames and restore the key column.

test_that("group_map fits each shard and matches lm() on that subset", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  set.seed(11)
  df <- data.frame(g = rep(c("a", "b", "c"), length.out = 900),
                   x = rnorm(900), stringsAsFactors = FALSE)
  df$y <- 2 + 3 * df$x + match(df$g, c("a", "b", "c")) + rnorm(900, 0, 0.1)
  write_vtr(df, f, batch_size = 64)

  p <- offload(tbl(f), by = "g")
  fits <- group_map(p, function(d, key) coef(lm(y ~ x, data = d)))

  expect_type(fits, "list")
  expect_setequal(names(fits), c("a", "b", "c"))   # list keyed by shard
  for (g in c("a", "b", "c")) {
    ref <- coef(lm(y ~ x, data = df[df$g == g, , drop = FALSE]))
    expect_equal(unname(fits[[g]]), unname(ref), tolerance = 1e-8)
  }
})

test_that("group_map accepts a purrr-style formula and forwards extra args", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(g = rep(c("a", "b"), length.out = 200),
                   x = as.double(1:200), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 16)

  p <- offload(tbl(f), by = "g")
  counts <- group_map(p, ~ nrow(.x))               # .x = shard, .y = key
  expect_equal(sum(unlist(counts)), nrow(df))

  trimmed <- group_map(p, function(d, key, top) head(d, top), top = 2)
  expect_true(all(vapply(trimmed, nrow, integer(1)) == 2L))
})

test_that("group_modify recombines shard tables and restores the key column", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  set.seed(12)
  df <- data.frame(g = rep(c("a", "b", "c"), length.out = 450),
                   y = rnorm(450), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 32)

  p <- offload(tbl(f), by = "g")
  out <- group_modify(p, function(d, key)
    data.frame(n = nrow(d), mean_y = mean(d$y)))

  expect_s3_class(out, "data.frame")
  expect_true("g" %in% names(out))                 # by-key restored as a column
  expect_setequal(out$g, c("a", "b", "c"))
  expect_equal(sum(out$n), nrow(df))
  for (g in c("a", "b", "c"))
    expect_equal(out$mean_y[out$g == g], mean(df$y[df$g == g]),
                 tolerance = 1e-12)
})

test_that("group_modify keeps a key the result already carries (no duplicate)", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  df <- data.frame(g = rep(c("a", "b", "c"), length.out = 300),
                   x = as.double(1:300), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 24)

  p <- offload(tbl(f), by = "g")
  out <- group_modify(p, function(d, key) d[d$x %% 2 == 0, , drop = FALSE])

  expect_equal(sum(names(out) == "g"), 1L)         # not duplicated
  expect_setequal(unique(out$g), c("a", "b", "c"))
  expect_equal(sort(out$x), sort(df$x[df$x %% 2 == 0]))
})

test_that("group_modify requires a data.frame from .f", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(mtcars, f)

  p <- offload(tbl(f), by = "cyl")
  expect_error(group_modify(p, function(d, key) mean(d$mpg)), "data.frame")
})
