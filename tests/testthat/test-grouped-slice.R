# Group-aware slice_min / slice_max and ordered row_number().
# Known-truth recovery: earliest/latest row per group, whole winning row kept.

make_tbl <- function() {
  df <- data.frame(
    piece_id  = c(1L,1L,1L, 2L,2L, 3L,3L,3L),
    STATUS_YR = c(2010L,1995L,2020L, 0L,2001L, 1980L,1980L,1999L),
    src       = c(11L,12L,13L, 21L,22L, 31L,32L,33L),
    geom_wkb  = c("A1","A2","A3","B1","B2","C1","C2","C3"),
    stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

test_that("grouped slice_min keeps the earliest row per group (with_ties = FALSE)", {
  f <- make_tbl(); on.exit(unlink(f))
  g <- tbl(f) |> group_by(piece_id) |>
    slice_min(STATUS_YR, n = 1, with_ties = FALSE) |> collect()
  g <- g[order(g$piece_id), ]

  expect_equal(nrow(g), 3L)
  expect_equal(as.numeric(g$STATUS_YR), c(1995, 0, 1980))
  # The whole winning row is preserved, including the string geometry column.
  expect_equal(g$geom_wkb, c("A2", "B1", "C1"))
  expect_equal(g$src, c(12L, 21L, 31L))
})

test_that("grouped slice_max keeps the latest row per group (with_ties = FALSE)", {
  f <- make_tbl(); on.exit(unlink(f))
  g <- tbl(f) |> group_by(piece_id) |>
    slice_max(STATUS_YR, n = 1, with_ties = FALSE) |> collect()
  g <- g[order(g$piece_id), ]

  expect_equal(nrow(g), 3L)
  expect_equal(as.numeric(g$STATUS_YR), c(2020, 2001, 1999))
  expect_equal(g$geom_wkb, c("A3", "B2", "C3"))
})

test_that("grouped slice_min with_ties = TRUE keeps boundary ties", {
  f <- make_tbl(); on.exit(unlink(f))
  g <- tbl(f) |> group_by(piece_id) |>
    slice_min(STATUS_YR, n = 1, with_ties = TRUE) |> collect()
  g <- g[order(g$piece_id, g$src), ]

  # piece 3 has two sources at 1980 -> both survive.
  expect_equal(nrow(g), 4L)
  expect_equal(g$src[g$piece_id == 3], c(31L, 32L))
})

test_that("grouped slice keeps exactly n per group when n > 1", {
  f <- make_tbl(); on.exit(unlink(f))
  g <- tbl(f) |> group_by(piece_id) |>
    slice_min(STATUS_YR, n = 2, with_ties = FALSE) |> collect()

  expect_equal(sum(g$piece_id == 1), 2L)   # 1995, 2010
  expect_equal(sum(g$piece_id == 2), 2L)   # 0, 2001
  expect_equal(sort(g$STATUS_YR[g$piece_id == 1]), c(1995, 2010))
})

test_that("NA in the order column sorts last so a known value wins", {
  df <- data.frame(
    g    = c(1L, 1L, 1L),
    yr   = c(NA_integer_, 2000L, 2010L),
    tag  = c("na", "win", "late"),
    stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  g <- tbl(f) |> group_by(g) |>
    slice_min(yr, n = 1, with_ties = FALSE) |> collect()
  expect_equal(nrow(g), 1L)
  expect_equal(g$tag, "win")
})

test_that("ordered row_number() ranks by the column within each group", {
  f <- make_tbl(); on.exit(unlink(f))
  rn <- tbl(f) |> group_by(piece_id) |>
    mutate(rk = row_number(STATUS_YR)) |> collect()

  p1 <- rn[rn$piece_id == 1, ]
  p1 <- p1[order(p1$rk), ]
  expect_equal(p1$rk, c(1, 2, 3))
  expect_equal(as.numeric(p1$STATUS_YR), c(1995, 2010, 2020))
})

test_that("row_number(desc()) ranks largest first within each group", {
  f <- make_tbl(); on.exit(unlink(f))
  rn <- tbl(f) |> group_by(piece_id) |>
    mutate(rk = row_number(desc(STATUS_YR))) |> collect()

  p1 <- rn[rn$piece_id == 1, ]
  p1 <- p1[order(p1$rk), ]
  expect_equal(as.numeric(p1$STATUS_YR), c(2020, 2010, 1995))
})

test_that("bare row_number() is unchanged (input order within group)", {
  f <- make_tbl(); on.exit(unlink(f))
  rn <- tbl(f) |> group_by(piece_id) |>
    mutate(rk = row_number()) |> collect()
  p1 <- rn[rn$piece_id == 1, ]
  expect_equal(p1$rk[match(c("A1","A2","A3"), p1$geom_wkb)], c(1, 2, 3))
})

test_that("ungrouped slice_min/slice_max remain global", {
  f <- make_tbl(); on.exit(unlink(f))
  u <- tbl(f) |> slice_min(STATUS_YR, n = 2, with_ties = FALSE) |> collect()
  expect_equal(nrow(u), 2L)
  expect_true(all(u$STATUS_YR <= 1980))   # two globally smallest (0, 1980)
})

# Base-R reference: the winning row per group is the earliest known order value
# (NA sorts last), ties broken by first appearance -- matching the streaming
# grouped-top-n operator that n == 1, with_ties = FALSE routes through.
ref_winner <- function(df, group_cols, ord_col, desc = FALSE) {
  key <- do.call(paste, c(df[group_cols], sep = "\r"))
  ord <- df[[ord_col]]
  rn  <- seq_len(nrow(df))
  win <- vapply(split(rn, factor(key, levels = unique(key))), function(idx) {
    v <- ord[idx]; valid <- !is.na(v)
    if (!any(valid)) return(idx[1L])
    iv <- idx[valid]; vv <- v[valid]
    iv[if (desc) which.max(vv) else which.min(vv)]
  }, integer(1))
  df[sort(win), , drop = FALSE]
}

test_that("grouped slice recovers the per-group winner across types", {
  set.seed(7)
  n <- 4000L
  d <- data.frame(
    k1   = sample(1:200, n, replace = TRUE),
    k2   = sample(letters[1:6], n, replace = TRUE),
    ord  = sample.int(500L, n, replace = TRUE),
    dbl  = rnorm(n),
    flag = sample(c(TRUE, FALSE), n, replace = TRUE),
    blob = paste0("WKB", sprintf("%07d", seq_len(n)), strrep("z", 30)),
    stringsAsFactors = FALSE)
  d$ord[sample(n, n %/% 8)] <- NA_integer_   # NA order values sort last
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f)); write_vtr(d, f)

  sort_rows <- function(x) x[order(x$k1, x$k2), , drop = FALSE]

  for (desc in c(FALSE, TRUE)) {
    fn  <- if (desc) slice_max else slice_min
    got <- tbl(f) |> group_by(k1, k2) |>
      fn(ord, n = 1, with_ties = FALSE) |> collect()
    ref <- ref_winner(d, c("k1", "k2"), "ord", desc = desc)
    g <- sort_rows(got); r <- sort_rows(ref)
    expect_equal(nrow(g), nrow(r))
    expect_equal(g$ord, r$ord)
    expect_equal(g$dbl, r$dbl)             # double passthrough
    expect_equal(g$flag, r$flag)           # bool passthrough
    expect_equal(g$blob, r$blob)           # whole geometry-sized string kept
  }
})

test_that("grouped slice emits its winners in batches (crosses the emit boundary)", {
  # More than one output batch (the operator emits 131072 winners per batch), so
  # this exercises the row-range boundary in the chunked emit path.
  N <- 140000L
  d <- data.frame(g = 1:N, ord = sample.int(9999L, N, replace = TRUE),
                  tag = sprintf("t%06d", 1:N), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f)); write_vtr(d, f)

  out <- tbl(f) |> group_by(g) |> slice_min(ord, n = 1, with_ties = FALSE) |> collect()
  out <- out[order(out$g), ]
  expect_equal(nrow(out), N)                       # one winner per group
  expect_identical(as.integer(out$g), 1:N)         # contiguous across the boundary
  expect_identical(as.integer(out$ord), as.integer(d$ord))
  expect_identical(out$tag, d$tag)                 # string column intact across batches
})
