# fuzzy_join() bounds the build side: when it exceeds vectra.memory it is spilled
# to a .vtr run file and each probe batch is matched one rowgroup chunk at a
# time. A tiny budget forces that spill (many chunks); the match set + distances
# must equal the resident-build result and a brute-force reference.

ref_fuzzy_s <- function(pdf, bdf, max_dist, pblock = NULL, bblock = NULL) {
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
        out[[length(out) + 1]] <- data.frame(pid = pdf$pid[i], bid = bdf$bid[j], dist = d)
    }
  }
  if (length(out) == 0)
    return(data.frame(pid = integer(), bid = integer(), dist = numeric()))
  do.call(rbind, out)
}

norm_s <- function(df) {
  df <- df[order(df$pid, df$bid, df$dist), , drop = FALSE]
  rownames(df) <- NULL; df
}
expect_fuzzy_s <- function(got, ref) {
  g <- norm_s(data.frame(pid = as.numeric(got$pid), bid = as.numeric(got$bid),
                         dist = as.numeric(got$fuzzy_dist)))
  r <- norm_s(data.frame(pid = as.numeric(ref$pid), bid = as.numeric(ref$bid),
                         dist = as.numeric(ref$dist)))
  expect_equal(nrow(g), nrow(r))
  expect_equal(g$pid, r$pid); expect_equal(g$bid, r$bid)
  expect_equal(g$dist, r$dist, tolerance = 1e-9)
}

rword_s <- function(n, len) vapply(seq_len(n), function(i)
  paste0(sample(letters, sample(3:len, 1), replace = TRUE), collapse = ""), "")

big_frames <- function(seed = 7, NP = 250, NB = 300) {
  set.seed(seed)
  pdf <- data.frame(pid = seq_len(NP), pname = rword_s(NP, 7), stringsAsFactors = FALSE)
  bdf <- data.frame(bid = seq_len(NB), bname = rword_s(NB, 7), stringsAsFactors = FALSE)
  pdf$pname[1:40]  <- bdf$bname[1:40]                    # exact dups
  pdf$pname[41:70] <- sub("^.", "z", bdf$bname[41:70])   # ~1 edit
  list(pdf = pdf, bdf = bdf)
}

test_that("unblocked fuzzy join matches brute force when the build spills", {
  fr <- big_frames()
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(fr$pdf, fp, batch_size = 40)
  write_vtr(fr$bdf, fb, batch_size = 40)

  old <- options(vectra.memory = 512); on.exit(options(old), add = TRUE)  # tiny -> spill
  got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    method = "levenshtein", max_dist = 0.3, n_threads = 4) |> collect()
  expect_fuzzy_s(got, ref_fuzzy_s(fr$pdf, fr$bdf, 0.3))
})

test_that("spilled build equals resident build exactly", {
  fr <- big_frames(seed = 9)
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(fr$pdf, fp, batch_size = 32)
  write_vtr(fr$bdf, fb, batch_size = 32)

  old <- options(vectra.memory = "4GB")
  res <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    method = "levenshtein", max_dist = 0.4) |> collect()
  options(old)

  old <- options(vectra.memory = 256); on.exit(options(old), add = TRUE)
  spl <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    method = "levenshtein", max_dist = 0.4) |> collect()
  expect_fuzzy_s(spl, data.frame(pid = res$pid, bid = res$bid, dist = res$fuzzy_dist))
})

test_that("blocked fuzzy join stays correct across spilled chunks", {
  fr <- big_frames(seed = 11); pdf <- fr$pdf; bdf <- fr$bdf
  pdf$pgen <- sample(c("A", "B", "C"), nrow(pdf), replace = TRUE)
  bdf$bgen <- sample(c("A", "B", "C"), nrow(bdf), replace = TRUE)
  pdf$pgen[1:70] <- "A"; bdf$bgen[1:70] <- "A"
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(pdf, fp, batch_size = 32); write_vtr(bdf, fb, batch_size = 32)

  old <- options(vectra.memory = 512); on.exit(options(old), add = TRUE)
  got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                    block_by = c("pgen" = "bgen"),
                    method = "levenshtein", max_dist = 0.3, n_threads = 3) |> collect()
  expect_fuzzy_s(got, ref_fuzzy_s(pdf, bdf, 0.3, pblock = "pgen", bblock = "bgen"))
})

test_that("spilled build is thread-count invariant", {
  fr <- big_frames(seed = 13)
  fp <- tempfile(fileext = ".vtr"); on.exit(unlink(fp), add = TRUE)
  fb <- tempfile(fileext = ".vtr"); on.exit(unlink(fb), add = TRUE)
  write_vtr(fr$pdf, fp, batch_size = 50); write_vtr(fr$bdf, fb, batch_size = 50)
  ref <- ref_fuzzy_s(fr$pdf, fr$bdf, 0.3)
  old <- options(vectra.memory = 400); on.exit(options(old), add = TRUE)
  for (nt in c(1, 4)) {
    got <- fuzzy_join(tbl(fp), tbl(fb), by = c("pname" = "bname"),
                      method = "levenshtein", max_dist = 0.3, n_threads = nt) |> collect()
    expect_fuzzy_s(got, ref)
  }
})
