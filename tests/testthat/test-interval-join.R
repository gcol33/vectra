# Recovery tests for interval_join: every result is checked against an
# independent brute-force O(n*m) overlap computation in plain R.

# Brute-force reference: all (i, j) pairs whose [start, end] ranges overlap,
# optionally restricted to rows sharing a blocking key. Returns a data frame
# of probe/build row indices sorted for stable comparison.
brute_overlap <- function(xdf, ydf, closed = TRUE, by = NULL) {
  pairs <- list()
  for (i in seq_len(nrow(xdf))) {
    ps <- xdf$start[i]; pe <- xdf$end[i]
    if (is.na(ps) || is.na(pe) || ps > pe) next
    for (j in seq_len(nrow(ydf))) {
      bs <- ydf$start[j]; be <- ydf$end[j]
      if (is.na(bs) || is.na(be) || bs > be) next
      if (!is.null(by)) {
        kx <- xdf[[by[1]]][i]; ky <- ydf[[by[2]]][j]
        if (is.na(kx) || is.na(ky) || kx != ky) next
      }
      ov <- if (closed) (ps <= be && bs <= pe) else (ps < be && bs < pe)
      if (ov) pairs[[length(pairs) + 1]] <- c(i, j)
    }
  }
  if (length(pairs) == 0)
    return(data.frame(i = integer(0), j = integer(0)))
  m <- do.call(rbind, pairs)
  df <- data.frame(i = m[, 1], j = m[, 2])
  df[order(df$i, df$j), , drop = FALSE]
}

# Run interval_join over two data frames and return probe/build index pairs,
# recovered from a unique probe id column (px) and build id column (by).
join_pairs <- function(xdf, ydf, closed = TRUE, by = NULL, type = "inner") {
  f1 <- tempfile(fileext = ".vtr"); f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  xdf$.pi <- seq_len(nrow(xdf))
  ydf$.bi <- seq_len(nrow(ydf))
  write_vtr(xdf, f1); write_vtr(ydf, f2)
  res <- interval_join(tbl(f1), tbl(f2),
                       start = "start", end = "end", by = by,
                       type = type, closed = closed) |> collect()
  res
}

set.seed(1)
rand_intervals <- function(n, lo = 0, hi = 100) {
  a <- sample(lo:hi, n, replace = TRUE)
  w <- sample(0:15, n, replace = TRUE)
  data.frame(start = a, end = a + w)
}

test_that("inner overlap matches brute force (no blocking)", {
  x <- rand_intervals(40); y <- rand_intervals(40)
  res <- join_pairs(x, y)
  got <- res[order(res$.pi, res$.bi), c(".pi", ".bi")]
  ref <- brute_overlap(x, y, closed = TRUE)
  expect_equal(nrow(got), nrow(ref))
  expect_equal(unname(as.matrix(got)), unname(as.matrix(ref)))
})

test_that("strict overlap excludes touching endpoints", {
  # probe ends exactly where build starts: overlap only when closed = TRUE.
  x <- data.frame(start = c(1, 10), end = c(5, 20))
  y <- data.frame(start = c(5, 21), end = c(8, 30))

  closed <- join_pairs(x, y, closed = TRUE)
  strict <- join_pairs(x, y, closed = FALSE)

  ref_closed <- brute_overlap(x, y, closed = TRUE)
  ref_strict <- brute_overlap(x, y, closed = FALSE)

  expect_equal(nrow(closed), nrow(ref_closed))
  expect_equal(nrow(strict), nrow(ref_strict))
  # The touching pair (probe 1 end=5, build 1 start=5) survives only closed.
  expect_true(nrow(closed) > nrow(strict))
})

test_that("blocking by a string key matches brute force", {
  set.seed(2)
  n <- 60
  chrom <- c("chr1", "chr2", "chr3")
  x <- rand_intervals(n); x$chr <- sample(chrom, n, replace = TRUE)
  y <- rand_intervals(n); y$chr <- sample(chrom, n, replace = TRUE)

  res <- join_pairs(x, y, by = c("chr" = "chr"))
  got <- res[order(res$.pi, res$.bi), c(".pi", ".bi")]
  ref <- brute_overlap(x, y, closed = TRUE, by = c("chr", "chr"))
  expect_equal(nrow(got), nrow(ref))
  expect_equal(unname(as.matrix(got)), unname(as.matrix(ref)))
  # carried key columns agree on every row
  expect_true(all(res$chr == res$chr.y))
})

test_that("left join keeps every probe row, NA build on no overlap", {
  x <- data.frame(start = c(1, 100, 50), end = c(5, 110, 55))
  y <- data.frame(start = c(2, 51), end = c(9, 60))  # nothing near probe 2

  res <- join_pairs(x, y, type = "left")
  # every probe id present
  expect_setequal(unique(res$.pi), c(1, 2, 3))
  # probe 2 has no overlap -> one row with NA build columns
  p2 <- res[res$.pi == 2, ]
  expect_equal(nrow(p2), 1)
  expect_true(is.na(p2$.bi))
  # inner-join rows of left == inner join
  inner <- join_pairs(x, y, type = "inner")
  matched <- res[!is.na(res$.bi), c(".pi", ".bi")]
  matched <- matched[order(matched$.pi, matched$.bi), ]
  inner <- inner[order(inner$.pi, inner$.bi), c(".pi", ".bi")]
  expect_equal(unname(as.matrix(matched)), unname(as.matrix(inner)))
})

test_that("NA endpoints and inverted intervals are dropped", {
  x <- data.frame(start = c(1, NA, 10), end = c(5, 8, 2))  # row2 NA, row3 inverted
  y <- data.frame(start = c(0), end = c(100))
  res <- join_pairs(x, y, type = "inner")
  expect_equal(sort(unique(res$.pi)), 1)  # only the valid probe overlaps
})

test_that("multiple overlaps per probe are all returned", {
  x <- data.frame(start = 0, end = 100)
  y <- data.frame(start = c(10, 20, 30, 200), end = c(15, 25, 35, 300))
  res <- join_pairs(x, y, type = "inner")
  expect_equal(nrow(res), 3)  # first three overlap, the 200-300 one does not
})
