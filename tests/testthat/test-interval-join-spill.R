# interval_join is bounded: both sides route through the external sort (keyed by
# block, start) and a serial sweep keeps only the active (open) intervals. A tiny
# vectra.memory forces those sorts to spill; results must still match a
# brute-force O(n*m) overlap, including deep overlap stacks (large active sets),
# blocking, and the left join.

brute_iv <- function(xdf, ydf, closed = TRUE, by = NULL) {
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
  if (length(pairs) == 0) return(data.frame(i = integer(0), j = integer(0)))
  m <- do.call(rbind, pairs)
  data.frame(i = m[, 1], j = m[, 2])[order(m[, 1], m[, 2]), ]
}

iv_pairs <- function(xdf, ydf, closed = TRUE, by = NULL, type = "inner",
                     batch = 64L) {
  f1 <- tempfile(fileext = ".vtr"); f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  xdf$.pi <- seq_len(nrow(xdf)); ydf$.bi <- seq_len(nrow(ydf))
  write_vtr(xdf, f1, batch_size = batch); write_vtr(ydf, f2, batch_size = batch)
  interval_join(tbl(f1), tbl(f2), start = "start", end = "end", by = by,
                type = type, closed = closed) |> collect()
}

test_that("inner overlap matches brute force when both sorts spill", {
  set.seed(31)
  n <- 800L; m <- 700L
  x <- data.frame(start = sample.int(2000L, n, replace = TRUE))
  x$end <- x$start + sample.int(30L, n, replace = TRUE)
  y <- data.frame(start = sample.int(2000L, m, replace = TRUE))
  y$end <- y$start + sample.int(30L, m, replace = TRUE)

  old <- options(vectra.memory = 8192); on.exit(options(old), add = TRUE)
  res <- iv_pairs(x, y, closed = TRUE, batch = 40L)
  got <- res[order(res$.pi, res$.bi), c(".pi", ".bi")]
  ref <- brute_iv(x, y, closed = TRUE)
  expect_equal(nrow(got), nrow(ref))
  expect_equal(unname(as.matrix(got)), unname(as.matrix(ref)))
})

test_that("deep overlap stacks (large active sets) stay exact under spill", {
  # Many wide intervals all covering a shared region -> the active set is large.
  set.seed(32)
  n <- 500L; m <- 500L
  x <- data.frame(start = sample.int(50L, n, replace = TRUE))
  x$end <- x$start + sample.int(200L, n, replace = TRUE)   # wide, heavy overlap
  y <- data.frame(start = sample.int(50L, m, replace = TRUE))
  y$end <- y$start + sample.int(200L, m, replace = TRUE)

  old <- options(vectra.memory = 8192); on.exit(options(old), add = TRUE)
  res <- iv_pairs(x, y, closed = TRUE, batch = 32L)
  got <- res[order(res$.pi, res$.bi), c(".pi", ".bi")]
  ref <- brute_iv(x, y, closed = TRUE)
  expect_equal(nrow(got), nrow(ref))
  expect_equal(unname(as.matrix(got)), unname(as.matrix(ref)))
})

test_that("blocked interval join matches brute force across spilled blocks", {
  set.seed(33)
  n <- 600L; m <- 600L
  chrom <- paste0("chr", 1:5)
  x <- data.frame(start = sample.int(1500L, n, replace = TRUE))
  x$end <- x$start + sample.int(25L, n, replace = TRUE)
  x$chr <- sample(chrom, n, replace = TRUE)
  y <- data.frame(start = sample.int(1500L, m, replace = TRUE))
  y$end <- y$start + sample.int(25L, m, replace = TRUE)
  y$chr <- sample(chrom, m, replace = TRUE)

  old <- options(vectra.memory = 8192); on.exit(options(old), add = TRUE)
  res <- iv_pairs(x, y, closed = TRUE, by = c("chr" = "chr"), batch = 48L)
  got <- res[order(res$.pi, res$.bi), c(".pi", ".bi")]
  ref <- brute_iv(x, y, closed = TRUE, by = c("chr", "chr"))
  expect_equal(nrow(got), nrow(ref))
  expect_equal(unname(as.matrix(got)), unname(as.matrix(ref)))
  expect_true(all(res$chr == res$chr.y))
})

test_that("left join keeps every probe under spill, NA build on no overlap", {
  set.seed(34)
  n <- 400L; m <- 300L
  x <- data.frame(start = sample.int(4000L, n, replace = TRUE))   # sparse
  x$end <- x$start + sample.int(5L, n, replace = TRUE)
  y <- data.frame(start = sample.int(4000L, m, replace = TRUE))
  y$end <- y$start + sample.int(5L, m, replace = TRUE)

  old <- options(vectra.memory = 8192); on.exit(options(old), add = TRUE)
  res <- iv_pairs(x, y, closed = TRUE, type = "left", batch = 32L)

  expect_setequal(unique(res$.pi), seq_len(n))            # every probe present
  matched <- res[!is.na(res$.bi), c(".pi", ".bi")]
  matched <- matched[order(matched$.pi, matched$.bi), ]
  ref <- brute_iv(x, y, closed = TRUE)
  expect_equal(unname(as.matrix(matched)), unname(as.matrix(ref)))
  # probes with no overlap appear exactly once with NA build
  unmatched <- setdiff(seq_len(n), unique(ref$i))
  na_probes <- res$.pi[is.na(res$.bi)]
  expect_setequal(na_probes, unmatched)
})

test_that("strict overlap under spill excludes touching endpoints", {
  set.seed(35)
  n <- 500L; m <- 500L
  x <- data.frame(start = sample.int(300L, n, replace = TRUE))
  x$end <- x$start + sample.int(10L, n, replace = TRUE)
  y <- data.frame(start = sample.int(300L, m, replace = TRUE))
  y$end <- y$start + sample.int(10L, m, replace = TRUE)

  old <- options(vectra.memory = 8192); on.exit(options(old), add = TRUE)
  res <- iv_pairs(x, y, closed = FALSE, batch = 40L)
  got <- res[order(res$.pi, res$.bi), c(".pi", ".bi")]
  ref <- brute_iv(x, y, closed = FALSE)
  expect_equal(nrow(got), nrow(ref))
  expect_equal(unname(as.matrix(got)), unname(as.matrix(ref)))
})
