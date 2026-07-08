# External sort: bounded fan-in multi-pass merge. A tiny vectra.memory plus a
# small row-group size forces many spilled runs, so the run count exceeds the
# merge fan-in and the multi-pass reduction runs. Peak merge memory is then
# O(fan-in) rather than O(n_runs); results must match the in-RAM sort and base R
# exactly, including wide (small-fan-in) rows and NA keys.

make_vtr <- function(df, batch_size = NULL) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f, batch_size = batch_size)
  f
}

test_that("arrange over many spilled runs is exact (multi-pass merge)", {
  set.seed(11)
  n <- 100000
  df <- data.frame(id = seq_len(n), x = runif(n))
  f <- make_vtr(df, batch_size = 1000L); on.exit(unlink(f))    # 100 row groups

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> arrange(x) |> collect()

  expect_equal(nrow(got), n)
  expect_false(is.unsorted(got$x))                 # fully sorted across runs
  expect_equal(length(unique(got$id)), n)          # no rows lost / duplicated
  expect_true(all(sort(got$id) == seq_len(n)))
  expect_true(all(got$x == df$x[got$id]))          # payload aligned to key
})

test_that("spilled arrange matches the in-RAM arrange byte-for-byte", {
  set.seed(12)
  n <- 40000
  df <- data.frame(id = seq_len(n), x = sample(rnorm(2000), n, replace = TRUE))
  f <- make_vtr(df, batch_size = 800L); on.exit(unlink(f))

  old <- options(vectra.memory = "4GB")
  ref <- tbl(f) |> arrange(x, id) |> collect()      # in-RAM single-pass
  options(old)

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> arrange(x, id) |> collect()      # forced many-run merge
  expect_equal(got, ref)                            # identical total order
})

test_that("wide-string arrange spills with a small fan-in and stays exact", {
  set.seed(13)
  m <- 20000
  df <- data.frame(
    id = seq_len(m), x = runif(m),
    s  = vapply(seq_len(m),
                function(i) paste(rep(sprintf("val-%06d", i), 20), collapse = "-"),
                character(1)),
    stringsAsFactors = FALSE
  )
  f <- make_vtr(df, batch_size = 500L); on.exit(unlink(f))      # wide rows

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> arrange(x) |> collect()
  expect_equal(nrow(got), m)
  expect_false(is.unsorted(got$x))
  expect_true(all(got$s == df$s[got$id]))           # string payload aligned
})

test_that("NA keys sort last under spill", {
  set.seed(14)
  n <- 50000
  x <- runif(n); x[sample(n, 300)] <- NA
  df <- data.frame(id = seq_len(n), x = x)
  f <- make_vtr(df, batch_size = 1000L); on.exit(unlink(f))

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> arrange(x) |> collect()
  nna <- sum(is.na(x))
  expect_equal(nrow(got), n)
  expect_true(all(is.na(tail(got$x, nna))))         # NA last
  expect_false(is.unsorted(head(got$x, n - nna)))   # non-NA prefix sorted
})
