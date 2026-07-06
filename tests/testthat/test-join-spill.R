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

# Sorted inputs take the merge-join path; shuffled inputs take the hash path.
# A join is order-independent, so the two paths must agree for every kind --
# including NA keys (which never match, as in the hash path) and unmatched build
# rows (emitted exactly once by full, not doubled by the finalize pass).
test_that("merge-join path agrees with the hash-join path (NA, unmatched build)", {
  set.seed(99)
  # 0 is a large repeated group; 41:50 are build-only (unmatched build for full);
  # 900:909 are probe-only (unmatched left); NA keys never match.
  Lk <- c(rep(0L, 40), 1:40, 900:909, NA, NA)
  Rk <- c(rep(0L, 25), 1:40, 41:50, NA)
  L <- data.frame(k = Lk, lv = seq_along(Lk))
  R <- data.frame(k = Rk, rv = seq_along(Rk))
  Ls <- L[order(L$k, na.last = TRUE), ]; Rs <- R[order(R$k, na.last = TRUE), ]
  Lp <- L[sample(nrow(L)), ];            Rp <- R[sample(nrow(R)), ]
  f_sl <- tempfile(fileext = ".vtr"); f_sr <- tempfile(fileext = ".vtr")
  f_pl <- tempfile(fileext = ".vtr"); f_pr <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f_sl, f_sr, f_pl, f_pr)))
  write_vtr(Ls, f_sl, batch_size = 16L); write_vtr(Rs, f_sr, batch_size = 16L)
  write_vtr(Lp, f_pl, batch_size = 16L); write_vtr(Rp, f_pr, batch_size = 16L)

  for (nm in c("inner", "left", "full", "semi", "anti")) {
    jf <- get(paste0(nm, "_join"))
    merged <- collect(jf(tbl(f_sl), tbl(f_sr), by = "k"))   # sorted -> merge
    hashed <- collect(jf(tbl(f_pl), tbl(f_pr), by = "k"))   # shuffled -> hash
    expect_equal(norm(merged), norm(hashed), info = nm, ignore_attr = TRUE)
  }
})

# A single dominant key value cannot be split by hashing at any recursion
# depth. Past JOIN_MAX_SPILL_DEPTH the partition drops to the block-nested-loop
# fallback (build blocked under the budget, probe re-scanned per block). With a
# tiny budget the hot key's build side spans several blocks, and the non-hot
# keys exercise the recursive salted re-partitioning in the same query. Every
# kind must still equal the in-memory join.
test_that("hot-key join falls back to bounded block-nested-loop (all kinds)", {
  hot <- 0L
  # Build (right): hot key 200x + 60 cold singletons + NA.
  R <- data.frame(
    k  = c(rep(hot, 200L), 1:60, NA),
    rv = c(1:200, 101:160, -1),
    rt = c(sprintf("r%03d", 1:200), sprintf("c%02d", 1:60), "rna"),
    stringsAsFactors = FALSE)
  # Probe (left): hot key 120x + 80 cold (40 matching, 40 not) + NA.
  L <- data.frame(
    k  = c(rep(hot, 120L), 1:40, 900:939, NA),
    lv = c(1:120, 201:240, 301:340, -2),
    stringsAsFactors = FALSE)
  fl <- tempfile(fileext = ".vtr"); fr <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fl, fr)))
  write_vtr(L, fl, batch_size = 32L)
  write_vtr(R, fr, batch_size = 24L)

  kinds <- list(inner = inner_join, left = left_join, right = right_join,
                full = full_join, semi = semi_join, anti = anti_join)
  for (nm in names(kinds)) {
    jf <- kinds[[nm]]
    ref <- collect(jf(tbl(fl), tbl(fr), by = "k"))
    old <- options(vectra.memory = 1500)   # << hot-key build, forces BNL
    spl <- collect(jf(tbl(fl), tbl(fr), by = "k"))
    options(old)
    expect_equal(norm(spl), norm(ref), info = nm, ignore_attr = TRUE)
  }
})

# Same, but the hot key is a string (exercises string coercion, hashing, and
# emission on the block-nested-loop path).
test_that("hot-key join with a string key is bounded and correct", {
  R <- data.frame(
    k  = c(rep("HOT", 180L), sprintf("k%02d", 1:50)),
    rv = c(1:180, 1:50),
    stringsAsFactors = FALSE)
  L <- data.frame(
    k  = c(rep("HOT", 90L), sprintf("k%02d", 1:30), sprintf("z%02d", 1:30)),
    lv = c(1:90, 1:30, 1:30),
    stringsAsFactors = FALSE)
  fl <- tempfile(fileext = ".vtr"); fr <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fl, fr)))
  write_vtr(L, fl, batch_size = 40L); write_vtr(R, fr, batch_size = 30L)

  for (nm in c("inner", "left", "full", "semi", "anti")) {
    jf <- get(paste0(nm, "_join"))
    ref <- collect(jf(tbl(fl), tbl(fr), by = "k"))
    old <- options(vectra.memory = 1200); spl <- collect(jf(tbl(fl), tbl(fr), by = "k"))
    options(old)
    expect_equal(norm(spl), norm(ref), info = nm, ignore_attr = TRUE)
  }
})

# Many distinct keys with a budget far below the data drive several levels of
# recursive re-partitioning (salted per level so partitions actually split).
# The result must match the in-memory join for a composite key with duplicates.
test_that("deep recursive spill re-partitions correctly (composite key)", {
  set.seed(11)
  L <- data.frame(a = sample(1:120, 4000, TRUE), b = sample(1:8, 4000, TRUE),
                  lv = 1:4000)
  R <- data.frame(a = sample(1:120, 2000, TRUE), b = sample(1:8, 2000, TRUE),
                  rv = -(1:2000))
  fl <- tempfile(fileext = ".vtr"); fr <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fl, fr)))
  write_vtr(L, fl, batch_size = 64L); write_vtr(R, fr, batch_size = 48L)

  for (nm in c("inner", "left", "full", "anti")) {
    jf <- get(paste0(nm, "_join"))
    ref <- collect(jf(tbl(fl), tbl(fr), by = c("a", "b")))
    old <- options(vectra.memory = 1024); spl <- collect(jf(tbl(fl), tbl(fr), by = c("a", "b")))
    options(old)
    expect_equal(norm(spl), norm(ref), info = nm, ignore_attr = TRUE)
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
