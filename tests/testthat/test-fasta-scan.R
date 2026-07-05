# Streaming FASTA / FASTQ scan backends (tbl_fasta / tbl_fastq).
#
# Records become rows (id, desc, seq [, qual]); the file streams one batch at
# a time. Recovery-tested against an independent parser (Biostrings for the
# biology, ShortRead for FASTQ quality when installed) plus a hand-built
# fixture whose ids / descriptions / sequences / qualities are known exactly.
# Gzip and plain input, streaming invariance across batch sizes, and loud
# failure on a truncated / malformed record are all covered.

# ---- fixture writers ------------------------------------------------------

open_con <- function(path, mode = "w") {
  if (grepl("\\.gz$", path)) gzfile(path, mode) else file(path, mode)
}

write_fasta <- function(ids, descs, seqs, path, wrap = 0L) {
  con <- open_con(path); on.exit(close(con))
  for (i in seq_along(ids)) {
    hdr <- if (nzchar(descs[i])) paste0(">", ids[i], " ", descs[i])
           else paste0(">", ids[i])
    writeLines(hdr, con)
    s <- seqs[i]
    if (wrap > 0L && nchar(s) > 0L) {
      starts <- seq(1L, nchar(s), by = wrap)
      writeLines(substring(s, starts, starts + wrap - 1L), con)
    } else {
      writeLines(s, con)
    }
  }
}

write_fastq <- function(ids, descs, seqs, quals, path) {
  con <- open_con(path); on.exit(close(con))
  for (i in seq_along(ids)) {
    hdr <- if (nzchar(descs[i])) paste0("@", ids[i], " ", descs[i])
           else paste0("@", ids[i])
    writeLines(c(hdr, seqs[i], "+", quals[i]), con)
  }
}

rand_dna <- function(n, minlen = 8, maxlen = 60) {
  vapply(seq_len(n), function(i) {
    L <- sample(minlen:maxlen, 1)
    paste(sample(c("A", "C", "G", "T"), L, replace = TRUE), collapse = "")
  }, character(1))
}

# random printable Phred33 quality string of a given length
rand_qual <- function(len) {
  vapply(len, function(L)
    paste(sample(intToUtf8(33:73, multiple = TRUE), L, replace = TRUE),
          collapse = ""), character(1))
}

# ---- FASTA: fixture + Biostrings recovery ---------------------------------

test_that("tbl_fasta recovers id, desc, seq (fixture + Biostrings)", {
  ids   <- c("seq1", "seq2", "seq3")
  descs <- c("first description", "", "third one here")
  seqs  <- c("ACGTACGTAA", "GGGGCCCCTTTT", "ATATATATGCGC")
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  write_fasta(ids, descs, seqs, f)

  d <- tbl_fasta(f, quiet = TRUE) |> collect()
  expect_equal(d$id, ids)
  expect_equal(d$desc, descs)
  expect_equal(d$seq, seqs)

  skip_if_not_installed("Biostrings")
  dss <- Biostrings::readDNAStringSet(f)
  expect_equal(d$seq, unname(as.character(dss)))
  expect_equal(d$id, unname(sub("\\s.*$", "", names(dss))))
})

test_that("tbl_fasta concatenates a line-wrapped sequence", {
  seqs <- paste(rep("ACGT", 50), collapse = "")   # exactly 200 bases
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  write_fasta("wrapped", "", seqs, f, wrap = 60L)  # 4 sequence lines (60x3 + 20)
  d <- tbl_fasta(f, quiet = TRUE) |> collect()
  expect_equal(d$seq, seqs)
  expect_equal(nchar(d$seq), 200L)
})

test_that("tbl_fasta desc is empty string (not NA) when header has no description", {
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  write_fasta(c("a", "b"), c("", "has desc"), c("ACGT", "TTTT"), f)
  d <- tbl_fasta(f, quiet = TRUE) |> collect()
  expect_identical(d$desc, c("", "has desc"))
  expect_false(anyNA(d$desc))
})

test_that("tbl_fasta gzip input matches plain", {
  set.seed(1)
  ids  <- paste0("r", 1:20)
  seqs <- rand_dna(20)
  f  <- tempfile(fileext = ".fasta");    on.exit(unlink(f))
  fg <- tempfile(fileext = ".fasta.gz"); on.exit(unlink(fg), add = TRUE)
  write_fasta(ids, "", seqs, f)
  write_fasta(ids, "", seqs, fg)
  a <- tbl_fasta(f,  quiet = TRUE) |> collect()
  b <- tbl_fasta(fg, quiet = TRUE) |> collect()
  expect_equal(a, b)
  expect_equal(a$seq, seqs)
})

test_that("tbl_fasta is invariant to record batching", {
  set.seed(2)
  ids  <- paste0("s", 1:300)
  seqs <- rand_dna(300)
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  write_fasta(ids, "", seqs, f)
  one   <- tbl_fasta(f, quiet = TRUE) |> collect()
  many  <- tbl_fasta(f, batch_size = 7, quiet = TRUE) |> collect()
  expect_equal(one, many)
  expect_equal(nrow(one), 300L)
})

test_that("seq_* composes over the seq column of a FASTA scan", {
  skip_if_not_installed("Biostrings")
  set.seed(3)
  seqs <- rand_dna(30)
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  write_fasta(paste0("s", seq_along(seqs)), "", seqs, f)
  d <- tbl_fasta(f, quiet = TRUE) |>
    mutate(rc = seq_revcomp(seq), gc = seq_gc(seq)) |> collect()
  expect_equal(d$rc,
               unname(as.character(Biostrings::reverseComplement(
                 Biostrings::DNAStringSet(seqs)))))
})

# ---- FASTQ: fixture + Biostrings / ShortRead recovery ---------------------

test_that("tbl_fastq recovers id, desc, seq, qual (fixture + Biostrings)", {
  set.seed(4)
  ids   <- paste0("read", 1:15)
  descs <- ifelse(seq_len(15) %% 2 == 0, "", "some meta info")
  seqs  <- rand_dna(15, minlen = 10, maxlen = 40)
  quals <- rand_qual(nchar(seqs))
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  write_fastq(ids, descs, seqs, quals, f)

  d <- tbl_fastq(f, quiet = TRUE) |> collect()
  expect_equal(d$id, ids)
  expect_equal(d$desc, descs)
  expect_equal(d$seq, seqs)
  expect_equal(d$qual, quals)
  expect_equal(nchar(d$qual), nchar(d$seq))

  skip_if_not_installed("Biostrings")
  dss <- Biostrings::readDNAStringSet(f, format = "fastq")
  expect_equal(d$seq, unname(as.character(dss)))
})

test_that("tbl_fastq recovers sequences via ShortRead", {
  skip_if_not_installed("ShortRead")
  set.seed(5)
  ids   <- paste0("r", 1:12)
  seqs  <- rand_dna(12, minlen = 12, maxlen = 30)
  quals <- rand_qual(nchar(seqs))
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  write_fastq(ids, "", seqs, quals, f)
  d <- tbl_fastq(f, quiet = TRUE) |> collect()
  fq <- ShortRead::readFastq(f)
  expect_equal(d$seq, as.character(ShortRead::sread(fq)))
})

test_that("tbl_fastq tolerates '@' and '+' inside the quality string", {
  # A naive '@'/'+' boundary scan would mis-split; strict 4-line parsing must
  # treat these as ordinary quality bytes.
  ids   <- c("q1", "q2")
  seqs  <- c("ACGT", "TTGGCC")
  quals <- c("I@!I", "+@!!+I")   # same length as seqs, contain @ and +
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  write_fastq(ids, "", seqs, quals, f)
  d <- tbl_fastq(f, quiet = TRUE) |> collect()
  expect_equal(d$seq, seqs)
  expect_equal(d$qual, quals)
})

test_that("tbl_fastq gzip input matches plain", {
  set.seed(6)
  ids   <- paste0("r", 1:20)
  seqs  <- rand_dna(20)
  quals <- rand_qual(nchar(seqs))
  f  <- tempfile(fileext = ".fastq");    on.exit(unlink(f))
  fg <- tempfile(fileext = ".fastq.gz"); on.exit(unlink(fg), add = TRUE)
  write_fastq(ids, "", seqs, quals, f)
  write_fastq(ids, "", seqs, quals, fg)
  expect_equal(tbl_fastq(f,  quiet = TRUE) |> collect(),
               tbl_fastq(fg, quiet = TRUE) |> collect())
})

test_that("tbl_fastq is invariant to record batching", {
  set.seed(7)
  ids   <- paste0("s", 1:250)
  seqs  <- rand_dna(250)
  quals <- rand_qual(nchar(seqs))
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  write_fastq(ids, "", seqs, quals, f)
  expect_equal(tbl_fastq(f, quiet = TRUE) |> collect(),
               tbl_fastq(f, batch_size = 11, quiet = TRUE) |> collect())
})

# ---- loud failure on truncated / malformed input --------------------------

test_that("a FASTQ record cut short is a loud error, not a silent drop", {
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  # header + seq + '+' but no quality line
  writeLines(c("@r1", "ACGT", "+"), f)
  expect_error(tbl_fastq(f, quiet = TRUE) |> collect(), "truncated")
})

test_that("a FASTQ seq/qual length mismatch errors", {
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  writeLines(c("@r1", "ACGT", "+", "III"), f)   # qual len 3 != seq len 4
  expect_error(tbl_fastq(f, quiet = TRUE) |> collect(), "length mismatch")
})

test_that("a FASTQ line where '+' is expected errors", {
  f <- tempfile(fileext = ".fastq"); on.exit(unlink(f))
  writeLines(c("@r1", "ACGT", "x", "IIII"), f)  # 3rd line not '+'
  expect_error(tbl_fastq(f, quiet = TRUE) |> collect(), "expected '\\+'")
})

test_that("a FASTA that does not start with '>' errors", {
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  writeLines(c("ACGTACGT", "no header here"), f)
  expect_error(tbl_fasta(f, quiet = TRUE) |> collect(), "malformed FASTA")
})

test_that("an empty FASTA yields zero rows without error", {
  f <- tempfile(fileext = ".fasta"); on.exit(unlink(f))
  file.create(f)
  d <- tbl_fasta(f, quiet = TRUE) |> collect()
  expect_equal(nrow(d), 0L)
  expect_named(d, c("id", "desc", "seq"))
})
