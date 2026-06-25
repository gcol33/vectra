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
