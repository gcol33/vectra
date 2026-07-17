# The holistic aggregates (median, n_distinct) push scalars into RecSpill, the
# generic fixed-width external sort-merge. A genuinely larger-than-RAM spill
# produces far more runs than can be opened at once; the merge must reduce the
# run count to a bounded fan-in over multiple passes rather than opening every
# run (handle exhaustion) or holding O(n_runs) read blocks. A tiny vectra.memory
# on a large ungrouped aggregate forces hundreds of runs through that path; the
# answer must still match base R exactly.

test_that("ungrouped median/n_distinct stay exact when the record sort spills to many runs", {
  set.seed(202)
  n <- 80000L
  x <- sample(1:6000, n, replace = TRUE)          # ties + many distinct values
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = as.double(x)), f)

  old <- options(vectra.memory = "4GB")
  ref <- tbl(f) |> summarise(m = median(x), nd = n_distinct(x)) |> collect()
  options(old)

  # 8-byte records, ~1 KB per run after the two holistic budgets split a 2 KB
  # budget -> hundreds of runs, well past the merge fan-in cap.
  old <- options(vectra.memory = 2048); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> summarise(m = median(x), nd = n_distinct(x)) |> collect()

  expect_equal(got, ref)                                  # spill == resident
  expect_equal(got$m, stats::median(as.double(x)))        # == base R
  expect_equal(got$nd, length(unique(x)))
})

test_that("grouped median/n_distinct match base R under a spilling record sort", {
  set.seed(203)
  n <- 60000L
  g <- sample(1:8, n, replace = TRUE)              # few large groups
  x <- sample(1:4000, n, replace = TRUE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(g = g, x = as.double(x)), f)

  old <- options(vectra.memory = 4096); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> group_by(g) |>
    summarise(m = median(x), nd = n_distinct(x)) |> arrange(g) |> collect()

  bm  <- tapply(x, g, stats::median)
  bnd <- tapply(x, g, function(v) length(unique(v)))
  expect_equal(got$m,  as.numeric(bm[as.character(got$g)]))
  expect_equal(got$nd, as.numeric(bnd[as.character(got$g)]))
})
