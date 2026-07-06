# --- Fuzzy join (streaming-probe, resident-build) ---
#
# Correctness is checked against a base-R adist() brute force for the
# normalized-Levenshtein method; the streaming path, blocking, and NA handling
# are all exercised through that reference.

# Brute-force normalized-Levenshtein reference.
ref_fuzzy <- function(pdf, bdf, max_dist, pblock = NULL, bblock = NULL) {
  out <- list()
  for (i in seq_len(nrow(pdf))) {
    pn <- pdf$pname[i]; if (is.na(pn)) next
    for (j in seq_len(nrow(bdf))) {
      bn <- bdf$bname[j]; if (is.na(bn)) next
      if (!is.null(pblock)) {
        pb <- pdf[[pblock]][i]; bb <- bdf[[bblock]][j]
        if (is.na(pb) || is.na(bb) || pb != bb) next
      }
      ml <- max(nchar(pn), nchar(bn))
      d  <- if (ml == 0) 0 else adist(pn, bn)[1, 1] / ml
      if (d <= max_dist + 1e-12)
        out[[length(out) + 1]] <-
          data.frame(pid = pdf$pid[i], bid = bdf$bid[j], dist = d)
    }
  }
  if (length(out) == 0)
    return(data.frame(pid = integer(), bid = integer(), dist = numeric()))
  do.call(rbind, out)
}

norm_set <- function(df) {
  df <- df[order(df$pid, df$bid, df$dist), , drop = FALSE]
  rownames(df) <- NULL
  df
}

expect_fuzzy_equal <- function(got, ref) {
  g <- norm_set(data.frame(pid = as.numeric(got$pid), bid = as.numeric(got$bid),
                           dist = as.numeric(got$fuzzy_dist)))
  r <- norm_set(data.frame(pid = as.numeric(ref$pid), bid = as.numeric(ref$bid),
                           dist = as.numeric(ref$dist)))
  expect_equal(nrow(g), nrow(r))
  expect_equal(g$pid, r$pid)
  expect_equal(g$bid, r$bid)
  expect_equal(g$dist, r$dist, tolerance = 1e-9)
}

rword <- function(n, len) vapply(seq_len(n), function(i)
  paste0(sample(letters, sample(3:len, 1), replace = TRUE), collapse = ""), "")

make_frames <- function() {
  set.seed(42)
  NP <- 400; NB <- 300
  pdf <- data.frame(pid = seq_len(NP), pname = rword(NP, 7), stringsAsFactors = FALSE)
  bdf <- data.frame(bid = seq_len(NB), bname = rword(NB, 7), stringsAsFactors = FALSE)
  pdf$pname[1:50]  <- bdf$bname[1:50]                 # exact dups
  pdf$pname[51:80] <- sub("^.", "z", bdf$bname[51:80]) # ~1 edit
  list(pdf = pdf, bdf = bdf)
}

test_that("fuzzy join streams the probe side across many row groups", {
  fr <- make_frames()
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(fr$pdf, fp, batch_size = 16)   # ~25 probe batches
  write_vtr(fr$bdf, fb, batch_size = 16)

  got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    method = "levenshtein", max_dist = 0.3, n_threads = 4) |> collect()
  expect_fuzzy_equal(got, ref_fuzzy(fr$pdf, fr$bdf, 0.3))
  expect_identical(names(got), c("pid", "pname", "bid", "bname", "fuzzy_dist"))
})

test_that("fuzzy join is thread-count invariant", {
  fr <- make_frames()
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(fr$pdf, fp, batch_size = 64)
  write_vtr(fr$bdf, fb, batch_size = 64)
  ref <- ref_fuzzy(fr$pdf, fr$bdf, 0.3)
  for (nt in c(1, 4)) {
    got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                      method = "levenshtein", max_dist = 0.3, n_threads = nt) |> collect()
    expect_fuzzy_equal(got, ref)
  }
})

test_that("fuzzy join with blocking restricts to same-block comparisons", {
  fr <- make_frames()
  pdf <- fr$pdf; bdf <- fr$bdf
  pdf$pgen <- sample(c("A", "B", "C"), nrow(pdf), replace = TRUE)
  bdf$bgen <- sample(c("A", "B", "C"), nrow(bdf), replace = TRUE)
  pdf$pgen[1:80] <- "A"; bdf$bgen[1:80] <- "A"
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(pdf, fp, batch_size = 32)
  write_vtr(bdf, fb, batch_size = 32)

  got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    block_by = c("pgen" = "bgen"),
                    method = "levenshtein", max_dist = 0.3, n_threads = 4) |> collect()
  expect_fuzzy_equal(got, ref_fuzzy(pdf, bdf, 0.3, pblock = "pgen", bblock = "bgen"))
})

test_that("fuzzy join drops NA keys and NA block values", {
  pdf <- data.frame(pid = 1:6,
                    pname = c("apple", NA, "grape", "mango", "peach", "apple"),
                    pgen  = c("f", "f", NA, "f", "f", "f"), stringsAsFactors = FALSE)
  bdf <- data.frame(bid = 1:4,
                    bname = c("apple", "grapes", "mango", NA),
                    bgen  = c("f", "f", "f", "f"), stringsAsFactors = FALSE)
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(pdf, fp, batch_size = 2)
  write_vtr(bdf, fb, batch_size = 2)

  got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    method = "levenshtein", max_dist = 0.4, n_threads = 2) |> collect()
  expect_fuzzy_equal(got, ref_fuzzy(pdf, bdf, 0.4))

  gotb <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                     block_by = c("pgen" = "bgen"),
                     method = "levenshtein", max_dist = 0.4, n_threads = 2) |> collect()
  expect_fuzzy_equal(gotb, ref_fuzzy(pdf, bdf, 0.4, pblock = "pgen", bblock = "bgen"))
})

test_that("fuzzy join with no matches returns an empty, well-formed result", {
  fr <- make_frames()
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(fr$pdf, fp, batch_size = 16)
  write_vtr(fr$bdf, fb, batch_size = 16)
  got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    method = "levenshtein", max_dist = 0.0, n_threads = 2) |> collect()
  # max_dist = 0 keeps only exact matches
  expect_fuzzy_equal(got, ref_fuzzy(fr$pdf, fr$bdf, 0.0))
  expect_identical(names(got), c("pid", "pname", "bid", "bname", "fuzzy_dist"))
})

test_that("dl and jw methods stay within the distance threshold", {
  pdf <- data.frame(pid = 1:6,
                    pname = c("apple", "aple", "grape", "mango", "peach", "aplle"),
                    stringsAsFactors = FALSE)
  bdf <- data.frame(bid = 1:3, bname = c("apple", "grapes", "mango"),
                    stringsAsFactors = FALSE)
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(pdf, fp, batch_size = 2)
  write_vtr(bdf, fb, batch_size = 2)
  for (m in c("dl", "jw")) {
    got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                      method = m, max_dist = 0.5) |> collect()
    expect_true(all(got$fuzzy_dist <= 0.5 + 1e-12))
  }
})
