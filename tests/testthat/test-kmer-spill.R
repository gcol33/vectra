# kmer(): bounded, spill-safe counting. A tiny vectra.memory forces the record
# sort (RecSpill) to spill many runs, and a distinct-k-mer count above the emit
# batch size forces the run-length counter to carry an open (group, k-mer) run
# across output batches. Results must match a hand-rolled tabulation and the
# non-spilled run exactly.

rand_dna_str <- function(L) paste(sample(c("A", "C", "G", "T"), L, replace = TRUE),
                                  collapse = "")

ref_spectrum <- function(s, k) {          # every ACGT window of one sequence
  st <- seq_len(nchar(s) - k + 1)
  km <- substring(s, st, st + k - 1)
  km <- km[!grepl("[^ACGT]", km)]
  tt <- table(km)
  data.frame(kmer = names(tt), count = as.numeric(tt), stringsAsFactors = FALSE)
}

test_that("ungrouped kmer counts are exact when the record sort spills", {
  set.seed(101)
  s <- rand_dna_str(40000)                 # ~40k windows at k=8
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = s, stringsAsFactors = FALSE), f)

  old <- options(vectra.memory = 4096); on.exit(options(old), add = TRUE)  # 256 recs/run
  got <- tbl(f) |> kmer(seq, k = 8) |> arrange(kmer) |> collect()
  ref <- ref_spectrum(s, 8)
  ref <- ref[order(ref$kmer), , drop = FALSE]

  expect_equal(got$kmer, ref$kmer)
  expect_equal(as.numeric(got$count), ref$count)
  expect_equal(sum(got$count), nchar(s) - 8 + 1)   # nothing lost or doubled
})

test_that("spilled kmer matches the in-RAM kmer exactly (grouped)", {
  set.seed(102)
  df <- data.frame(id = rep(paste0("g", 1:6), each = 4),
                   seq = vapply(1:24, function(i) rand_dna_str(sample(200:400, 1)),
                                character(1)),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f, batch_size = 3L)         # many small input batches too

  old <- options(vectra.memory = "4GB")
  ref <- tbl(f) |> kmer(seq, k = 7, by = id) |> arrange(id, kmer) |> collect()
  options(old)

  old <- options(vectra.memory = 2048); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> kmer(seq, k = 7, by = id) |> arrange(id, kmer) |> collect()
  expect_equal(got, ref)                     # spill path == resident path
})

test_that("distinct k-mers past the emit batch size carry runs across batches", {
  set.seed(103)
  s <- rand_dna_str(300000)                  # ~289k distinct 11-mers -> >1 emit batch
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = s, stringsAsFactors = FALSE), f)

  old <- options(vectra.memory = 16384); on.exit(options(old), add = TRUE)  # ~1k recs/run
  got <- tbl(f) |> kmer(seq, k = 11) |> collect()

  st <- seq_len(nchar(s) - 11 + 1)
  km <- substring(s, st, st + 10)
  expect_equal(nrow(got), length(unique(km)))     # each distinct k-mer once
  expect_equal(length(unique(got$kmer)), nrow(got))
  expect_equal(sum(got$count), length(km))        # total windows preserved
  expect_false(is.unsorted(got$kmer))             # emitted in ascending order
})

test_that("canonical counting survives a spill", {
  set.seed(104)
  s <- rand_dna_str(30000)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(seq = s, stringsAsFactors = FALSE), f)

  old <- options(vectra.memory = "4GB")
  ref <- tbl(f) |> kmer(seq, k = 6, canonical = TRUE) |> arrange(kmer) |> collect()
  options(old)

  old <- options(vectra.memory = 4096); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> kmer(seq, k = 6, canonical = TRUE) |> arrange(kmer) |> collect()
  expect_equal(got, ref)
})
