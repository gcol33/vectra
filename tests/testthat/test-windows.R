# --- lag ---

test_that("lag shifts values down by 1", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(prev = lag(x)) |> collect()
  expect_true(is.na(result$prev[1]))
  expect_equal(result$prev[2:4], c(10, 20, 30))
})

test_that("lag with n = 2", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(prev2 = lag(x, 2)) |> collect()
  expect_true(is.na(result$prev2[1]))
  expect_true(is.na(result$prev2[2]))
  expect_equal(result$prev2[3:5], c(1, 2, 3))
})

test_that("lag with default value", {
  df <- data.frame(x = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(prev = lag(x, 1, default = 0)) |> collect()
  expect_equal(result$prev, c(0, 10, 20))
})

# --- lead ---

test_that("lead shifts values up by 1", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(nxt = lead(x)) |> collect()
  expect_equal(result$nxt[1:3], c(20, 30, 40))
  expect_true(is.na(result$nxt[4]))
})

test_that("lead with default value", {
  df <- data.frame(x = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(nxt = lead(x, 1, default = 99)) |> collect()
  expect_equal(result$nxt, c(20, 30, 99))
})

# --- row_number ---

test_that("row_number assigns sequential integers", {
  df <- data.frame(x = c(5.0, 3.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(rn = row_number()) |> collect()
  expect_equal(result$rn, c(1, 2, 3))
})

# --- cumsum ---

test_that("cumsum computes cumulative sum", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs, c(1, 3, 6, 10))
})

test_that("cumsum propagates NA", {
  df <- data.frame(x = c(1.0, NA, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs[1], 1)
  expect_true(is.na(result$cs[2]))
  expect_true(is.na(result$cs[3]))
})

# --- cummean ---

test_that("cummean computes cumulative mean", {
  df <- data.frame(x = c(2.0, 4.0, 6.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cm = cummean(x)) |> collect()
  expect_equal(result$cm, c(2, 3, 4))
})

# --- cummin / cummax ---

test_that("cummin computes running minimum", {
  df <- data.frame(x = c(5.0, 3.0, 4.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cmi = cummin(x)) |> collect()
  expect_equal(result$cmi, c(5, 3, 3, 1))
})

test_that("cummax computes running maximum", {
  df <- data.frame(x = c(1.0, 4.0, 2.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cmx = cummax(x)) |> collect()
  expect_equal(result$cmx, c(1, 4, 4, 5))
})

# --- grouped windows ---

test_that("lag works within groups", {
  df <- data.frame(
    g = c("a", "a", "a", "b", "b"),
    x = c(1.0, 2.0, 3.0, 10.0, 20.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(prev = lag(x)) |> collect()
  # Group a: NA, 1, 2; Group b: NA, 10
  expect_true(is.na(result$prev[1]))
  expect_equal(result$prev[2], 1)
  expect_equal(result$prev[3], 2)
  expect_true(is.na(result$prev[4]))
  expect_equal(result$prev[5], 10)
})

test_that("row_number resets within groups", {
  df <- data.frame(
    g = c("a", "a", "b", "b", "b"),
    x = c(1.0, 2.0, 3.0, 4.0, 5.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(rn = row_number()) |> collect()
  expect_equal(result$rn, c(1, 2, 1, 2, 3))
})

test_that("cumsum resets within groups", {
  df <- data.frame(
    g = c("a", "a", "b", "b"),
    x = c(1.0, 2.0, 10.0, 20.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs, c(1, 3, 10, 30))
})

# --- mixed window + regular mutate ---

test_that("window and regular mutate in same call", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cs = cumsum(x), doubled = x * 2) |> collect()
  expect_equal(result$cs, c(1, 3, 6))
  expect_equal(result$doubled, c(2, 4, 6))
})

# --- rank ---

test_that("rank assigns min rank with gaps for ties", {
  df <- data.frame(x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(r = rank(x)) |> collect()
  expect_equal(result$r, c(4, 1, 4, 3, 1))
})

test_that("rank works with groups", {
  df <- data.frame(g = c("a", "a", "a", "b", "b"),
                   x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(r = rank(x)) |> collect()
  expect_equal(result$r[result$g == "a"], c(2, 1, 2))
  expect_equal(result$r[result$g == "b"], c(2, 1))
})

# --- dense_rank ---

test_that("dense_rank assigns consecutive ranks without gaps", {
  df <- data.frame(x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(dr = dense_rank(x)) |> collect()
  expect_equal(result$dr, c(3, 1, 3, 2, 1))
})

test_that("dense_rank works with groups", {
  df <- data.frame(g = c("a", "a", "a", "b", "b"),
                   x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(dr = dense_rank(x)) |> collect()
  expect_equal(result$dr[result$g == "a"], c(2, 1, 2))
  expect_equal(result$dr[result$g == "b"], c(2, 1))
})

# --- spill-safe grouped path: order preservation and cross-batch groups ---

test_that("grouped window preserves original row order (interleaved groups)", {
  # Groups are interleaved, not contiguous: the spill-safe path sorts by key to
  # process one group at a time, then restores arrival order via the row-id.
  df <- data.frame(
    g = c("b", "a", "b", "a", "b", "a"),
    x = c(1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |>
    mutate(cs = cumsum(x), rn = row_number()) |> collect()

  # rows come back in original order, untouched
  expect_equal(result$g, df$g)
  expect_equal(result$x, df$x)
  # cumulative sum runs in arrival order within each group
  # b: x=1,3,5 -> 1,4,9 ; a: x=2,4,6 -> 2,6,12
  expect_equal(result$cs, c(1, 2, 4, 6, 9, 12))
  expect_equal(result$rn, c(1, 1, 2, 2, 3, 3))
})

test_that("grouped window is correct when a group spans many row groups", {
  # batch_size below the group span forces groups to straddle batch boundaries,
  # exercising cross-batch accumulation in the streaming pull.
  set.seed(1)
  df <- data.frame(
    g = rep(c("a", "b", "c"), length.out = 300),
    x = as.numeric(1:300),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  result <- tbl(f) |> group_by(g) |> mutate(cs = cumsum(x)) |> collect()

  ref_cs <- ave(df$x, df$g, FUN = cumsum)   # per-group cumsum, order preserved
  expect_equal(result$g, df$g)
  expect_equal(result$x, df$x)
  expect_equal(result$cs, ref_cs)
})

test_that("ungrouped cumulative window streams across many row groups", {
  # batch_size well below the row count forces the running state to carry across
  # batch boundaries in the cumulative streaming path.
  df <- data.frame(x = as.numeric(1:250))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  result <- tbl(f) |>
    mutate(cs = cumsum(x), cm = cummean(x), rn = row_number(),
           cmi = cummin(x), cmx = cummax(x)) |> collect()

  expect_equal(result$cs, cumsum(df$x))
  expect_equal(result$cm, cumsum(df$x) / seq_along(df$x))
  expect_equal(result$rn, as.numeric(seq_along(df$x)))
  expect_equal(result$cmi, cummin(df$x))
  expect_equal(result$cmx, cummax(df$x))
})

test_that("ungrouped cumsum propagates NA across row groups", {
  x <- as.numeric(1:100)
  x[40] <- NA
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  result <- tbl(f) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs[1:39], cumsum(x[1:39]))
  expect_true(all(is.na(result$cs[40:100])))
})

test_that("grouped window with NA keys groups the NAs together", {
  df <- data.frame(
    g = c("a", NA, "a", NA, "b"),
    x = c(1.0, 2.0, 3.0, 4.0, 5.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(cs = cumsum(x)) |> collect()

  ref_cs <- ave(df$x, addNA(factor(df$g)), FUN = cumsum)
  expect_equal(result$g, df$g)
  expect_equal(result$cs, ref_cs)
})

# --- spill-safe ungrouped ordered streaming ---
#
# Every ungrouped ordered window (rank family, rolling, lag/lead, ntile) streams
# in a single forward pass over a globally sorted input rather than
# materializing the whole table. A batch_size well below the row count forces
# the running state to carry across batch boundaries, and the sort/restore
# round-trip must return rows to arrival order. References are base R.

test_that("ungrouped rank family streams across row groups", {
  set.seed(11)
  n <- 253
  x <- as.numeric(sample(1:20, n, replace = TRUE))   # heavy ties
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  r <- tbl(f) |> mutate(
    rk  = rank(x),
    drk = dense_rank(x),
    rkd = rank(desc(x)),
    rn  = row_number(x),
    pr  = percent_rank(x)
  ) |> collect()

  expect_equal(r$x, x)                                       # arrival order kept
  expect_equal(r$rk,  as.numeric(rank(x, ties.method = "min")))
  expect_equal(r$drk, as.numeric(match(x, sort(unique(x)))))
  expect_equal(r$rkd, as.numeric(rank(-x, ties.method = "min")))
  expect_equal(r$rn,  as.numeric(rank(x, ties.method = "first")))
  expect_equal(r$pr,  (rank(x, ties.method = "min") - 1) / (n - 1))
})

test_that("ungrouped cume_dist and ntile stream with correct partition size", {
  set.seed(12)
  n <- 240
  x <- as.numeric(sample(1:12, n, replace = TRUE))
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  r <- tbl(f) |> mutate(cd = cume_dist(x), nt = ntile(4)) |> collect()

  expect_equal(r$x, x)
  expect_equal(r$cd, vapply(x, function(v) mean(x <= v), numeric(1)))
  # positional ntile: bucket by arrival position, not value
  pos <- 0:(n - 1)
  expect_equal(r$nt, as.numeric((pos * 4L) %/% n + 1L))
})

test_that("ungrouped cume_dist treats NA as the largest value", {
  set.seed(13)
  n <- 120
  x <- as.numeric(sample(1:8, n, replace = TRUE))
  x[c(4, 40, 90)] <- NA
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  r <- tbl(f) |> mutate(cd = cume_dist(x)) |> collect()
  ref <- vapply(seq_along(x), function(i) {
    if (is.na(x[i])) return(1.0)          # NAs sort last -> cume_dist 1
    sum(!is.na(x) & x <= x[i]) / n
  }, numeric(1))
  expect_equal(r$cd, ref)
})

test_that("ungrouped lag and lead stream across row groups", {
  set.seed(14)
  n <- 205
  x <- as.numeric(sample(1:30, n, replace = TRUE))
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  shift <- function(v, k) {              # positive k = lead, negative = lag
    m <- length(v); out <- rep(NA_real_, m)
    if (k >= 0 && k < m) out[1:(m - k)] <- v[(k + 1):m]
    if (k < 0 && -k < m) out[(1 - k):m] <- v[1:(m + k)]
    out
  }
  r <- tbl(f) |> mutate(
    lg  = lag(x, 5),
    ld  = lead(x, 4),
    ldd = lead(x, 2, default = -1)
  ) |> collect()

  expect_equal(r$x, x)
  expect_equal(r$lg, shift(x, -5))
  expect_equal(r$ld, shift(x, 4))
  ref_ldd <- shift(x, 2); ref_ldd[is.na(ref_ldd)] <- -1
  expect_equal(r$ldd, ref_ldd)
})

test_that("ungrouped rolling aggregates stream across row groups", {
  set.seed(15)
  n <- 231
  v  <- as.numeric(sample(1:10, n, replace = TRUE))
  ts <- sort(runif(n, 0, 50)) * 86400 + 1e7    # seconds, above the day heuristic
  df <- data.frame(v = v, ts = ts)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)

  w <- 5 * 86400
  roll_ref <- function(fn) vapply(seq_along(ts), function(i) {
    idx <- which(ts > ts[i] - w & ts <= ts[i]); fn(v[idx])
  }, numeric(1))

  r <- tbl(f) |> mutate(
    rs  = roll_sum(v, ts, "5 days"),
    rm  = roll_mean(v, ts, "5 days"),
    rmi = roll_min(v, ts, "5 days"),
    rmx = roll_max(v, ts, "5 days"),
    rnn = roll_n(ts, "5 days")
  ) |> collect()

  expect_equal(r$v, v)
  expect_equal(r$rs,  roll_ref(sum))
  expect_equal(r$rm,  roll_ref(mean))
  expect_equal(r$rmi, roll_ref(min))
  expect_equal(r$rmx, roll_ref(max))
  expect_equal(r$rnn, roll_ref(length))
})

test_that("ungrouped cumsum NA poison still holds in the streaming path", {
  x <- as.numeric(1:150)
  x[70] <- NA
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)
  r <- tbl(f) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(r$cs[1:69], cumsum(x[1:69]))
  expect_true(all(is.na(r$cs[70:150])))
})

test_that("mixed-ordering ungrouped windows fall back but stay correct", {
  # rank(x) needs x-ascending order, rank(desc(x)) needs x-descending: the two
  # orderings cannot share one stream, so the node uses the in-memory path.
  set.seed(16)
  n <- 130
  x <- as.numeric(sample(1:10, n, replace = TRUE))
  df <- data.frame(x = x)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 16)
  r <- tbl(f) |> mutate(a = rank(x), b = rank(desc(x))) |> collect()
  expect_equal(r$a, as.numeric(rank(x, ties.method = "min")))
  expect_equal(r$b, as.numeric(rank(-x, ties.method = "min")))
})
