# Native seq_* biological-sequence expressions in mutate()/filter().
# A sequence rides through the engine as an ASCII string column; each op is
# recovery-tested cell-for-cell against an established ground truth
# (Biostrings for the biology, stringdist for edit distance, base R for the
# rest), over random sequences across several seeds plus explicit
# ambiguity-code cases.

# Materialize a character vector of sequences to a .vtr and run one seq_* op.
# The parameter is dotted (`.seqs`) so a short `...` output name such as `s =`
# cannot partial-match it (R matches named args to formals before `...`).
seq_col <- function(.seqs, ...) {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(seq = .seqs, stringsAsFactors = FALSE), f)
  tbl(f) |> mutate(...) |> collect()
}

# Random DNA sequences of varied length (some length %% 3 == 0 for translate).
rand_dna <- function(n, minlen = 3, maxlen = 60, mult3 = FALSE) {
  vapply(seq_len(n), function(i) {
    L <- sample(minlen:maxlen, 1)
    if (mult3) L <- L - (L %% 3) + 3
    paste(sample(c("A", "C", "G", "T"), L, replace = TRUE), collapse = "")
  }, character(1))
}

# ---- measures -------------------------------------------------------------

test_that("seq_length recovers nchar", {
  x <- rand_dna(50)
  d <- seq_col(x, len = seq_length(seq))
  expect_equal(d$len, nchar(x))
})

test_that("seq_gc recovers the GC fraction", {
  for (s in 1:5) {
    set.seed(s)
    x <- rand_dna(40)
    d <- seq_col(x, gc = seq_gc(seq))
    manual <- vapply(x, function(v) {
      chars <- strsplit(v, "")[[1]]
      sum(chars %in% c("G", "C")) / nchar(v)
    }, numeric(1), USE.NAMES = FALSE)
    expect_equal(d$gc, manual, tolerance = 1e-12)
  }
})

test_that("seq_gc matches Biostrings letterFrequency", {
  skip_if_not_installed("Biostrings")
  set.seed(42)
  x <- rand_dna(30)
  d <- seq_col(x, gc = seq_gc(seq))
  dss <- Biostrings::DNAStringSet(x)
  gt <- as.numeric(Biostrings::letterFrequency(dss, "GC", as.prob = TRUE))
  expect_equal(d$gc, gt, tolerance = 1e-12)
})

# ---- transforms vs Biostrings ---------------------------------------------

test_that("seq_revcomp / seq_complement / seq_reverse recover Biostrings", {
  skip_if_not_installed("Biostrings")
  for (s in 1:5) {
    set.seed(s)
    x <- rand_dna(40)
    d <- seq_col(x,
                 rc = seq_revcomp(seq),
                 co = seq_complement(seq),
                 rv = seq_reverse(seq))
    dss <- Biostrings::DNAStringSet(x)
    expect_equal(d$rc, as.character(Biostrings::reverseComplement(dss)),
                 info = paste("seed", s))
    expect_equal(d$co, as.character(Biostrings::complement(dss)),
                 info = paste("seed", s))
    expect_equal(d$rv, as.character(Biostrings::reverse(dss)),
                 info = paste("seed", s))
  }
})

test_that("seq_translate recovers the standard genetic code (Biostrings)", {
  skip_if_not_installed("Biostrings")
  for (s in 1:5) {
    set.seed(s)
    x <- rand_dna(30, mult3 = TRUE)
    d <- seq_col(x, aa = seq_translate(seq))
    dss <- Biostrings::DNAStringSet(x)
    gt <- as.character(Biostrings::translate(dss, no.init.codon = TRUE))
    expect_equal(d$aa, gt, info = paste("seed", s))
  }
})

test_that("seq_translate drops a trailing partial codon", {
  # 7 bases -> 2 codons translated, last base dropped
  d <- seq_col("ATGTAAG", aa = seq_translate(seq))
  expect_equal(d$aa, "M*")
})

# ---- transforms with a base-R ground truth --------------------------------

test_that("seq_transcribe swaps T<->U in both directions", {
  dna <- rand_dna(20)
  d <- seq_col(dna, rna = seq_transcribe(seq))
  expect_equal(d$rna, chartr("T", "U", dna))
  # RNA back to DNA
  rna <- chartr("T", "U", dna)
  d2 <- seq_col(rna, back = seq_transcribe(seq))
  expect_equal(d2$back, dna)
})

test_that("seq_subseq recovers substr", {
  set.seed(7)
  x <- rand_dna(30, minlen = 10, maxlen = 40)
  d <- seq_col(x, s = seq_subseq(seq, 3, 5))
  expect_equal(d$s, substr(x, 3, 7))
  # width running past the end clamps
  d2 <- seq_col(x, s = seq_subseq(seq, 5, 1000))
  expect_equal(d2$s, substr(x, 5, nchar(x)))
})

# ---- distance vs stringdist -----------------------------------------------

test_that("seq_dist recovers stringdist (lv / osa / hamming)", {
  skip_if_not_installed("stringdist")
  for (s in 1:5) {
    set.seed(s)
    x   <- rand_dna(40, minlen = 5, maxlen = 30)
    ref <- rand_dna(40, minlen = 5, maxlen = 30)
    f <- tempfile(fileext = ".vtr"); on.exit(unlink(f), add = TRUE)
    write_vtr(data.frame(seq = x, ref = ref, stringsAsFactors = FALSE), f)
    d <- tbl(f) |> mutate(
      lv = seq_dist(seq, ref),
      dl = seq_dist(seq, ref, method = "osa")
    ) |> collect()
    expect_equal(d$lv, as.integer(stringdist::stringdist(x, ref, method = "lv")),
                 info = paste("seed", s))
    expect_equal(d$dl, as.integer(stringdist::stringdist(x, ref, method = "osa")),
                 info = paste("seed", s))
  }
})

test_that("seq_dist hamming: NA on unequal length, count otherwise", {
  skip_if_not_installed("stringdist")
  x   <- c("ACGT", "ACGT", "AAAA")
  ref <- c("ACGA", "ACG",  "AAAA")   # row 2 differs in length -> NA
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = x, ref = ref, stringsAsFactors = FALSE), f)
  d <- tbl(f) |> mutate(h = seq_dist(seq, ref, method = "hamming")) |> collect()
  expect_equal(d$h, c(1L, NA_integer_, 0L))
})

test_that("seq_dist works against a constant reference", {
  skip_if_not_installed("stringdist")
  set.seed(11)
  x <- rand_dna(20, minlen = 8, maxlen = 20)
  d <- seq_col(x, d = seq_dist(seq, "ACGTACGTAC"))
  expect_equal(d$d,
               as.integer(stringdist::stringdist(x, "ACGTACGTAC", method = "lv")))
})

# ---- IUPAC ambiguity codes ------------------------------------------------

test_that("seq_complement maps IUPAC ambiguity codes", {
  # R Y S W K M B V D H N  ->  Y R S W M K V B H D N
  d <- seq_col("RYSWKMBVDHN", co = seq_complement(seq), rc = seq_revcomp(seq))
  expect_equal(d$co, "YRSWMKVBHDN")
  expect_equal(d$rc, "NDHBVKMWSRY")   # reverse of the complement
})

test_that("seq_complement preserves lowercase", {
  d <- seq_col("acgtACGT", co = seq_complement(seq))
  expect_equal(d$co, "tgcaTGCA")
})

# ---- NA handling and filter() ---------------------------------------------

test_that("seq ops yield NA on NA input, never error", {
  x <- c("ACGT", NA, "GGCC")
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = x, stringsAsFactors = FALSE), f)
  d <- tbl(f) |> mutate(
    len = seq_length(seq),
    gc  = seq_gc(seq),
    rc  = seq_revcomp(seq),
    d   = seq_dist(seq, "ACGT")
  ) |> collect()
  expect_equal(is.na(d$len), c(FALSE, TRUE, FALSE))
  expect_equal(is.na(d$gc),  c(FALSE, TRUE, FALSE))
  expect_equal(is.na(d$rc),  c(FALSE, TRUE, FALSE))
  expect_equal(is.na(d$d),   c(FALSE, TRUE, FALSE))
})

test_that("seq_* composes inside filter()", {
  x <- c("GGGG", "ATAT", "GCGC", "AAAA")
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = x, stringsAsFactors = FALSE), f)
  d <- tbl(f) |> filter(seq_gc(seq) > 0.5) |> collect()
  expect_setequal(d$seq, c("GGGG", "GCGC"))
})

# ---- streaming invariance (multi-batch == single-batch) -------------------

test_that("seq transform is invariant to row-group batching", {
  skip_if_not_installed("Biostrings")
  set.seed(99)
  x <- rand_dna(500, minlen = 10, maxlen = 40)
  f1 <- tempfile(fileext = ".vtr"); on.exit(unlink(f1))
  f2 <- tempfile(fileext = ".vtr"); on.exit(unlink(f2), add = TRUE)
  write_vtr(data.frame(seq = x, stringsAsFactors = FALSE), f1)          # one group
  write_vtr(data.frame(seq = x, stringsAsFactors = FALSE), f2,
            batch_size = 64)                                            # many groups
  a <- tbl(f1) |> mutate(rc = seq_revcomp(seq)) |> collect()
  b <- tbl(f2) |> mutate(rc = seq_revcomp(seq)) |> collect()
  expect_equal(a$rc, b$rc)
  expect_equal(a$rc, as.character(Biostrings::reverseComplement(
    Biostrings::DNAStringSet(x))))
})
