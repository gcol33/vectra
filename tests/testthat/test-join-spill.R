# Grace-hash spill: when the build side exceeds the memory budget, the join
# hash-partitions both sides to run-files and joins one partition at a time.
# A tiny options(vectra.memory) forces the spill path on small data; the result
# must be byte-for-byte identical to the in-memory join for every join kind.

norm <- function(df) {
  df <- df[do.call(order, lapply(df, as.character)), , drop = FALSE]
  rownames(df) <- NULL
  df
}

test_that("spilling grace-hash join equals the in-memory join (all kinds)", {
  set.seed(42)
  n_l <- 1500L; n_r <- 800L
  L <- data.frame(k = sample(1:200, n_l, TRUE),
                  s = sample(c("aa", "bb", "cc", "dd"), n_l, TRUE),
                  lv = rnorm(n_l))
  R <- data.frame(k = sample(1:200, n_r, TRUE),
                  rv = rnorm(n_r), tag = sample(letters, n_r, TRUE))
  # NA keys on both sides (must never match, and must survive the partitioner)
  L$k[c(3, 50, 900)] <- NA
  R$k[c(7, 400)] <- NA
  fl <- tempfile(fileext = ".vtr"); fr <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fl, fr)))
  write_vtr(L, fl, batch_size = 128L)
  write_vtr(R, fr, batch_size = 100L)

  kinds <- list(inner = inner_join, left = left_join, right = right_join,
                full = full_join, semi = semi_join, anti = anti_join)
  for (nm in names(kinds)) {
    jf <- kinds[[nm]]
    ref <- collect(jf(tbl(fl), tbl(fr), by = "k"))          # in-memory
    old <- options(vectra.memory = 3000)                    # force spill
    spl <- collect(jf(tbl(fl), tbl(fr), by = "k"))
    options(old)
    expect_equal(norm(spl), norm(ref), info = nm,
                 ignore_attr = TRUE)
  }
})

test_that("spilling join handles a composite key and duplicate matches", {
  set.seed(7)
  L <- data.frame(a = sample(1:40, 1200, TRUE), b = sample(1:5, 1200, TRUE),
                  lv = 1:1200)
  R <- data.frame(a = sample(1:40, 600, TRUE), b = sample(1:5, 600, TRUE),
                  rv = -(1:600))
  fl <- tempfile(fileext = ".vtr"); fr <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fl, fr)))
  write_vtr(L, fl, batch_size = 90L); write_vtr(R, fr, batch_size = 70L)

  ref <- collect(inner_join(tbl(fl), tbl(fr), by = c("a", "b")))
  old <- options(vectra.memory = 2500); on.exit(options(old), add = TRUE)
  spl <- collect(inner_join(tbl(fl), tbl(fr), by = c("a", "b")))
  expect_equal(norm(spl), norm(ref), ignore_attr = TRUE)
})
