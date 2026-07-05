# k-mer spectrum node (kmer()).
#
# Counts every k-mer of a nucleotide column, grouped by zero or more keys, in a
# native 2-bit-packed hash table. Recovery-tested against a hand-rolled R
# tabulation: ungrouped and by-group counts, the canonical (reverse-complement
# collapse) option, non-ACGT window skipping, and streaming invariance across
# input batch sizes.

# ---- R reference k-mer tabulation -----------------------------------------

.b2b <- c(A = 0, C = 1, G = 2, T = 3)
.packk <- function(s) Reduce(function(a, x) a * 4 + x,
                             .b2b[strsplit(s, "")[[1]]], 0)
.revcomp <- function(s)
  chartr("ACGT", "TGCA", paste(rev(strsplit(s, "")[[1]]), collapse = ""))
# canonical representative: the smaller of a k-mer and its reverse complement
# under the 2-bit packing (A<C<G<T), matching the C node exactly.
.canon <- function(s) { r <- .revcomp(s); if (.packk(s) <= .packk(r)) s else r }

.kmers_of <- function(s, k) {
  n <- nchar(s)
  if (n < k) return(character(0))
  st <- 1:(n - k + 1)
  substring(s, st, st + k - 1)
}

# spectrum of a set of sequences (one group); skips windows with non-ACGT bases
ref_counts <- function(seqs, k, canonical = FALSE) {
  all_k <- unlist(lapply(seqs, function(s) {
    km <- .kmers_of(s, k)
    km[!grepl("[^ACGT]", km)]
  }))
  if (canonical && length(all_k)) all_k <- vapply(all_k, .canon, character(1))
  tt <- table(all_k)
  data.frame(kmer = names(tt), count = as.numeric(tt),
             stringsAsFactors = FALSE)
}

ref_grouped <- function(df, k, canonical = FALSE) {
  parts <- lapply(split(df, df$id), function(chunk) {
    r <- ref_counts(chunk$seq, k, canonical)
    if (nrow(r) == 0) return(NULL)
    data.frame(id = chunk$id[1], kmer = r$kmer, count = r$count,
               stringsAsFactors = FALSE)
  })
  do.call(rbind, parts)
}

rand_dna <- function(n, minlen = 20, maxlen = 50) {
  vapply(seq_len(n), function(i) {
    L <- sample(minlen:maxlen, 1)
    paste(sample(c("A", "C", "G", "T"), L, replace = TRUE), collapse = "")
  }, character(1))
}

expect_spectrum_equal <- function(d, ref) {
  d <- d[order(d$kmer), , drop = FALSE]
  ref <- ref[order(ref$kmer), , drop = FALSE]
  expect_equal(d$kmer, ref$kmer)
  expect_equal(as.numeric(d$count), as.numeric(ref$count))
}

expect_grouped_equal <- function(d, ref) {
  d <- d[order(d$id, d$kmer), , drop = FALSE]
  ref <- ref[order(ref$id, ref$kmer), , drop = FALSE]
  expect_equal(as.character(d$id), as.character(ref$id))
  expect_equal(d$kmer, ref$kmer)
  expect_equal(as.numeric(d$count), as.numeric(ref$count))
}

write_seq <- function(df, path, batch_size = NULL) {
  if (is.null(batch_size)) write_vtr(df, path)
  else write_vtr(df, path, batch_size = batch_size)
}

# ---- ungrouped spectrum ---------------------------------------------------

test_that("kmer (ungrouped) matches a hand-rolled tabulation across k", {
  set.seed(1)
  seqs <- rand_dna(6, 15, 40)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = seqs, stringsAsFactors = FALSE), f)
  for (k in c(1L, 2L, 3L, 6L)) {
    d <- tbl(f) |> kmer(seq, k = k) |> collect()
    expect_named(d, c("kmer", "count"))
    expect_spectrum_equal(d, ref_counts(seqs, k))
  }
})

test_that("counts sum to the number of valid windows", {
  set.seed(11)
  seqs <- rand_dna(4, 25, 45)   # pure ACGT, so every window is valid
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = seqs, stringsAsFactors = FALSE), f)
  d <- tbl(f) |> kmer(seq, k = 5) |> collect()
  expect_equal(sum(d$count), sum(pmax(nchar(seqs) - 5 + 1, 0)))
})

# ---- grouped spectrum -----------------------------------------------------

test_that("kmer by id matches per-group tabulation", {
  set.seed(2)
  df <- data.frame(id = paste0("s", 1:8),
                   seq = rand_dna(8, 20, 50), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)
  d <- tbl(f) |> kmer(seq, k = 4, by = id) |> collect()
  expect_named(d, c("id", "kmer", "count"))
  expect_grouped_equal(d, ref_grouped(df, 4))
})

test_that("multiple rows sharing an id count into one spectrum", {
  df <- data.frame(id = c("a", "a", "b"),
                   seq = c("ACGT", "ACGT", "TTTT"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)
  d <- tbl(f) |> kmer(seq, k = 2, by = id) |> collect()
  expect_grouped_equal(d, ref_grouped(df, 2))
  # group a: AC, CG, GT each appear twice (two identical rows)
  a <- d[d$id == "a", ]
  expect_true(all(a$count == 2))
})

# ---- canonical ------------------------------------------------------------

test_that("canonical kmer collapses reverse complements", {
  set.seed(3)
  seqs <- rand_dna(5, 20, 40)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = seqs, stringsAsFactors = FALSE), f)
  d <- tbl(f) |> kmer(seq, k = 4, canonical = TRUE) |> collect()
  expect_spectrum_equal(d, ref_counts(seqs, 4, canonical = TRUE))
  # every emitted k-mer is its own canonical representative
  expect_true(all(d$kmer == vapply(d$kmer, .canon, character(1))))
})

test_that("canonical merges a palindrome with itself, not double-counts", {
  # ACGT is its own reverse complement; canonical count == plain count.
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = "ACGTACGT", stringsAsFactors = FALSE), f)
  plain <- tbl(f) |> kmer(seq, k = 4) |> collect()
  canon <- tbl(f) |> kmer(seq, k = 4, canonical = TRUE) |> collect()
  expect_equal(plain$count[plain$kmer == "ACGT"],
               canon$count[canon$kmer == "ACGT"])
})

# ---- non-ACGT handling ----------------------------------------------------

test_that("windows spanning a non-ACGT base are skipped", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = "ACGTNACGT", stringsAsFactors = FALSE), f)
  d <- tbl(f) |> kmer(seq, k = 3) |> collect()
  expect_spectrum_equal(d, data.frame(kmer = c("ACG", "CGT"),
                                       count = c(2, 2)))
})

# ---- streaming invariance -------------------------------------------------

test_that("kmer is invariant to input batching", {
  set.seed(4)
  df <- data.frame(id = paste0("s", 1:50),
                   seq = rand_dna(50, 20, 60), stringsAsFactors = FALSE)
  f1 <- tempfile(fileext = ".vtr"); on.exit(unlink(f1))
  f2 <- tempfile(fileext = ".vtr"); on.exit(unlink(f2), add = TRUE)
  write_seq(df, f1)
  write_seq(df, f2, batch_size = 7)
  a <- tbl(f1) |> kmer(seq, k = 5, by = id) |> arrange(id, kmer) |> collect()
  b <- tbl(f2) |> kmer(seq, k = 5, by = id) |> arrange(id, kmer) |> collect()
  expect_equal(a, b)
  expect_grouped_equal(a, ref_grouped(df, 5))
})

# ---- integration with the FASTA scan --------------------------------------

test_that("kmer runs directly over a FASTA scan", {
  seqs <- c("ACGTACGTAA", "TTTTGGGGCC")
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  writeLines(c(">a x", seqs[1], ">b y", seqs[2]), f)
  d <- tbl_fasta(f, quiet = TRUE) |> kmer(seq, k = 3, by = id) |> collect()
  df <- data.frame(id = c("a", "b"), seq = seqs, stringsAsFactors = FALSE)
  expect_grouped_equal(d, ref_grouped(df, 3))
})

# ---- defaults and edges ---------------------------------------------------

test_that("kmer defaults to the 'seq' column", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = "ACGTACGT", stringsAsFactors = FALSE), f)
  d1 <- tbl(f) |> kmer(k = 3) |> arrange(kmer) |> collect()
  d2 <- tbl(f) |> kmer(seq, k = 3) |> arrange(kmer) |> collect()
  expect_equal(d1, d2)
})

test_that("k longer than every sequence yields zero rows with the schema", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(id = "a", seq = "ACGT", stringsAsFactors = FALSE), f)
  d <- tbl(f) |> kmer(seq, k = 10, by = id) |> collect()
  expect_equal(nrow(d), 0L)
  expect_named(d, c("id", "kmer", "count"))
})

test_that("k out of range is rejected", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = "ACGT", stringsAsFactors = FALSE), f)
  expect_error(tbl(f) |> kmer(seq, k = 0), "between 1 and 32")
  expect_error(tbl(f) |> kmer(seq, k = 33), "between 1 and 32")
})

test_that("k = 32 packs without overflow", {
  s <- paste(rep("ACGT", 9), collapse = "")   # length 36
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = s, stringsAsFactors = FALSE), f)
  d <- tbl(f) |> kmer(seq, k = 32) |> collect()
  expect_spectrum_equal(d, ref_counts(s, 32))
  expect_equal(nchar(d$kmer), rep(32L, nrow(d)))
})
