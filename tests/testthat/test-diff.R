# Brute-force recovery tests for diff_vtr, the primary-key diff:
#   deleted = keys in A (old) that are absent from B (new)
#   added   = rows of B whose key is absent from A
# The key is treated as a primary key, so keys are unique within each file (the
# old/new snapshot contract). These pin the semantics and serve as the
# correctness oracle for the planned bounded (external sort-merge) rewrite of
# C_diff_vtr, which currently holds all distinct A keys resident (see ?diff_vtr
# and the tracked issue). Order is not part of the contract, so comparisons sort.

test_that("diff_vtr matches a brute-force key-set diff (integer keys, random)", {
  for (seed in 1:20) {
    set.seed(seed)
    na <- sample(1:100, 1); nb <- sample(1:100, 1)
    a_keys <- sample(1:100, na)          # unique keys per file (primary key)
    b_keys <- sample(1:100, nb)
    f1 <- tempfile(fileext = ".vtr"); f2 <- tempfile(fileext = ".vtr")
    write_vtr(data.frame(id = a_keys, val = seq_len(na) * 1.0), f1)
    write_vtr(data.frame(id = b_keys, val = seq_len(nb) * 1.0), f2)

    d <- diff_vtr(f1, f2, "id")
    exp_deleted <- sort(setdiff(a_keys, b_keys))
    exp_added   <- sort(setdiff(b_keys, a_keys))

    expect_equal(sort(unique(d$deleted)), exp_deleted, info = paste("seed", seed))
    expect_equal(sort(collect(d$added)$id), exp_added, info = paste("seed", seed))
    unlink(c(f1, f2))
  }
})

test_that("diff_vtr matches a brute-force key-set diff (string keys, random)", {
  pool <- paste0("k", 1:60)
  for (seed in 21:35) {
    set.seed(seed)
    na <- sample(1:60, 1); nb <- sample(1:60, 1)
    a_keys <- sample(pool, na)
    b_keys <- sample(pool, nb)
    f1 <- tempfile(fileext = ".vtr"); f2 <- tempfile(fileext = ".vtr")
    write_vtr(data.frame(id = a_keys, val = seq_len(na) * 1.0,
                         stringsAsFactors = FALSE), f1)
    write_vtr(data.frame(id = b_keys, val = seq_len(nb) * 1.0,
                         stringsAsFactors = FALSE), f2)

    d <- diff_vtr(f1, f2, "id")
    exp_deleted <- sort(setdiff(a_keys, b_keys))
    exp_added   <- sort(setdiff(b_keys, a_keys))

    expect_equal(sort(unique(as.character(d$deleted))), exp_deleted,
                 info = paste("seed", seed))
    expect_equal(sort(collect(d$added)$id), exp_added, info = paste("seed", seed))
    unlink(c(f1, f2))
  }
})

test_that("diff_vtr edge cases: no overlap, full overlap, single-side", {
  mk <- function(keys) {
    f <- tempfile(fileext = ".vtr")
    write_vtr(data.frame(id = keys, val = seq_along(keys) * 1.0), f)
    f
  }
  # disjoint: everything deleted, everything added
  d <- diff_vtr(mk(1:3), mk(4:6), "id")
  expect_equal(sort(d$deleted), 1:3)
  expect_equal(sort(collect(d$added)$id), 4:6)
  # identical keys: nothing added or deleted
  d <- diff_vtr(mk(1:4), mk(1:4), "id")
  expect_length(d$deleted, 0)
  expect_equal(nrow(collect(d$added)), 0)
})
