# Grouped summarise aggregates by hash while the table fits the budget, and
# past it hash-partitions the remaining groups to run files (recursively, then
# the sort-based path). A tiny vectra.memory drives every level; the result must
# match the in-budget path and base R, in key order with NA keys last.

make_vtr <- function(df, batch_size = NULL) {
  f <- tempfile(fileext = ".vtr")
  if (is.null(batch_size)) write_vtr(df, f) else write_vtr(df, f, batch_size = batch_size)
  f
}

summ <- function(f, mem, ...) {
  old <- options(vectra.memory = mem)
  on.exit(options(old))
  tbl(f) |> group_by(...) |>
    summarise(n = n(), s = sum(x), m = mean(x), lo = min(x), hi = max(x),
              v = var(x), fx = first(x), lx = last(x),
              fs = first(s_col), ls = last(s_col)) |>
    collect()
}

reference <- function(df, keys) {
  id <- do.call(paste, c(lapply(df[keys], function(v)
    ifelse(is.na(v), "<NA>", paste0("=", v))), sep = "
"))
  sp <- split(seq_len(nrow(df)), id)
  rows <- lapply(sp, function(i) {
    x <- df$x[i]
    out <- df[i[1], keys, drop = FALSE]
    out$n <- length(i); out$s <- sum(x); out$m <- mean(x)
    out$lo <- min(x); out$hi <- max(x)
    out$v <- if (length(x) > 1) var(x) else NA_real_
    out$fx <- x[1]; out$lx <- x[length(x)]
    out$fs <- df$s_col[i[1]]; out$ls <- df$s_col[i[length(i)]]
    out
  })
  ref <- do.call(rbind, rows)
  ref <- ref[do.call(order, c(unname(as.list(ref[keys])), na.last = TRUE)), ]
  rownames(ref) <- NULL
  ref
}

test_that("hash aggregation matches base R in and past the budget", {
  set.seed(7)
  n <- 30000
  df <- data.frame(
    g = sample(c(1:3000, NA), n, replace = TRUE),
    x = round(rnorm(n), 3),
    s_col = sample(c(letters, "a-longer-string"), n, replace = TRUE)
  )
  f <- make_vtr(df, batch_size = 4096); on.exit(unlink(f))
  ref <- reference(df[!is.na(df$g), ], "g")
  na_rows <- df[is.na(df$g), ]

  for (mem in c("1GB", "64KB", "2KB")) {
    got <- as.data.frame(summ(f, mem, g))
    expect_equal(nrow(got), 3001L, info = mem)
    expect_true(is.na(got$g[nrow(got)]), info = mem)
    expect_equal(got$g[-nrow(got)], ref$g, info = mem)
    for (col in c("n", "s", "m", "lo", "hi", "v", "fx", "lx", "fs", "ls"))
      expect_equal(got[[col]][-nrow(got)], ref[[col]], info = paste(mem, col))
    last <- got[nrow(got), ]
    expect_equal(last$n, nrow(na_rows))
    expect_equal(last$fx, na_rows$x[1])
    expect_equal(last$lx, na_rows$x[nrow(na_rows)])
  }
})

test_that("multi-key and string-key grouping survives partitioning", {
  set.seed(11)
  n <- 20000
  df <- data.frame(
    a = sample(c("alpha", "beta", "gamma", NA, ""), n, replace = TRUE),
    b = sample(1:400, n, replace = TRUE),
    x = runif(n),
    s_col = sample(letters, n, replace = TRUE)
  )
  f <- make_vtr(df, batch_size = 3000); on.exit(unlink(f))
  big <- as.data.frame(summ(f, "1GB", a, b))
  for (mem in c("32KB", "2KB")) {
    small <- as.data.frame(summ(f, mem, a, b))
    expect_identical(small, big, info = mem)
  }
  ref <- reference(df, c("a", "b"))
  expect_equal(big$n, ref$n)
  expect_equal(big$s, ref$s)
  expect_equal(big$a, ref$a)
  expect_equal(big$b, ref$b)
})

test_that("double keys: -0 joins 0, NaN is stored as NA and sorts last", {
  df <- data.frame(g = c(2.5, NaN, -1, NA, 2.5, NaN, 0, -0),
                   x = 1:8, s_col = letters[1:8])
  f <- make_vtr(df); on.exit(unlink(f))
  for (mem in c("1GB", "2KB")) {
    got <- summ(f, mem, g)
    expect_equal(got$g[1:3], c(-1, 0, 2.5), info = mem)
    expect_equal(got$n, c(1, 2, 2, 3), info = mem)
    expect_equal(got$s, c(3, 15, 6, 12), info = mem)
  }
})

test_that("empty input and ungrouped summarise are unchanged", {
  df <- data.frame(g = integer(0), x = numeric(0), s_col = character(0))
  f <- make_vtr(data.frame(g = 1L, x = 1, s_col = "a"))
  on.exit(unlink(f))
  empty <- tbl(f) |> filter(g > 5) |> group_by(g) |>
    summarise(n = n(), s = sum(x)) |> collect()
  expect_equal(nrow(empty), 0L)
  old <- options(vectra.memory = "2KB"); on.exit(options(old), add = TRUE)
  tot <- tbl(f) |> summarise(n = n(), s = sum(x)) |> collect()
  expect_equal(tot$n, 1)
})

test_that("holistic aggregates keep their sort-based path", {
  set.seed(3)
  df <- data.frame(g = sample(1:50, 5000, TRUE), x = sample(1:30, 5000, TRUE))
  f <- make_vtr(df); on.exit(unlink(f))
  got <- tbl(f) |> group_by(g) |>
    summarise(md = median(x), nd = n_distinct(x), s = sum(x)) |> collect()
  ref_md <- tapply(df$x, df$g, median)
  ref_nd <- tapply(df$x, df$g, function(v) length(unique(v)))
  expect_equal(got$g, as.numeric(names(ref_md)))
  expect_equal(got$md, unname(as.numeric(ref_md)))
  expect_equal(got$nd, unname(as.numeric(ref_nd)))
})

test_that("partition run files are removed after collect", {
  set.seed(5)
  df <- data.frame(g = sample(1:5000, 20000, TRUE), x = runif(20000),
                   s_col = "a")
  f <- make_vtr(df); on.exit(unlink(f))
  before <- list.files(tempdir(), pattern = "^vectra_hagg_")
  invisible(summ(f, "2KB", g))
  after <- list.files(tempdir(), pattern = "^vectra_hagg_")
  expect_equal(setdiff(after, before), character(0))
})

test_that("parallel shards agree with the reference on full-size batches", {
  skip_on_cran()
  set.seed(9)
  n <- 3e5
  df <- data.frame(
    sp = sprintf("species_%05d", sample.int(40000, n, TRUE)),
    yr = sample(1990:2020, n, TRUE),
    x = round(runif(n), 4)
  )
  df$sp[sample.int(n, 50)] <- NA
  f <- make_vtr(df); on.exit(unlink(f))
  run <- function(mem) {
    old <- options(vectra.memory = mem); on.exit(options(old))
    tbl(f) |> group_by(sp) |>
      summarise(n = n(), s = sum(x), hi = max(yr)) |> collect() |>
      as.data.frame()
  }
  ref <- run("8GB")
  key <- ifelse(is.na(df$sp), "\r", df$sp)
  expect_equal(nrow(ref), length(unique(key)))
  expect_false(is.unsorted(ref$sp[!is.na(ref$sp)]))
  expect_true(is.na(ref$sp[nrow(ref)]))
  expect_equal(ref$n, as.numeric(table(key)[ifelse(is.na(ref$sp), "\r", ref$sp)]),
               ignore_attr = TRUE)
  expect_equal(ref$s, as.vector(tapply(df$x, key, sum)[ifelse(is.na(ref$sp), "\r", ref$sp)]),
               tolerance = 1e-9)
  for (mem in c("4MB", "256KB")) expect_equal(run(mem), ref, info = mem)

  old <- options(vectra.memory = "4MB")
  last_sp <- tbl(f) |> group_by(yr) |>
    summarise(fs = first(sp), ls = last(sp), n = n()) |> collect()
  options(old)
  o <- split(df$sp, df$yr)
  expect_equal(last_sp$fs, unname(vapply(o, `[`, "", 1)))
  expect_equal(last_sp$ls, unname(vapply(o, function(v) v[length(v)], "")))
})
