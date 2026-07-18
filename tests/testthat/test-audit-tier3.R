# Regression tests for the Tier-3 audit fixes (2026-07-18): crafted / corrupt
# .vtri sidecar files must be rejected cleanly, never crash. A segfault here
# would take down the whole test process, so "the query returns or errors" IS
# the assertion. (The tdc LZ / DICT_1D decoder hardening for crafted .vtr blocks
# is covered by tdc's own test_adversarial_inputs.c.)

make_indexed <- function() {
  f <- tempfile(fileext = ".vtr")
  df <- data.frame(genus = rep(c("Quercus", "Pinus", "Fagus"), each = 50),
                   val = seq_len(150), stringsAsFactors = FALSE)
  write_vtr(df, f, batch_size = 50L)
  create_index(f, "genus")
  list(f = f, vtri = paste0(f, ".genus.vtri"))
}

probe <- function(ix) {
  tryCatch(collect(filter(tbl(ix$f), genus == "Pinus")),
           error = function(e) "errored")
}

test_that("a truncated .vtri never crashes the query", {
  for (keep in c(6L, 9L, 17L, 25L, 40L)) {
    ix <- make_indexed()
    raw <- readBin(ix$vtri, "raw", file.info(ix$vtri)$size)
    writeBin(raw[seq_len(min(keep, length(raw)))], ix$vtri)
    res <- probe(ix)
    expect_true(is.data.frame(res) || identical(res, "errored"))
    unlink(c(ix$f, ix$vtri))
  }
})

test_that("a .vtri header claiming an enormous entry count errors, not overflows", {
  ix <- make_indexed()
  raw <- readBin(ix$vtri, "raw", file.info(ix$vtri)$size)
  ver <- as.integer(raw[5]) + 256L * as.integer(raw[6])   # version u16, LE
  skip_if_not(ver == 1L, "test targets the v1 index layout")
  # v1: magic(4) version(2) col_idx(2) ci(1) => n_entries u64 at byte offset 9
  # (R index 10..17). Set it to 2^61 (little-endian) to force the overflow path.
  raw[10:17] <- as.raw(c(0, 0, 0, 0, 0, 0, 0, 0x20))
  writeBin(raw, ix$vtri)
  res <- probe(ix)
  expect_true(is.data.frame(res) || identical(res, "errored"))  # no crash
  unlink(c(ix$f, ix$vtri))
})

test_that("a valid index still works after the corruption tests", {
  ix <- make_indexed(); on.exit(unlink(c(ix$f, ix$vtri)))
  r <- collect(filter(tbl(ix$f), genus == "Pinus"))
  expect_equal(nrow(r), 50L)
  expect_true(all(r$genus == "Pinus"))
})
