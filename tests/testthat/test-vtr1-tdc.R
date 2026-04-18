# P3: tdc-backed row-group container — round-trip via vtr1_tdc.
#
# Exercises src/vtr1_tdc.c. Builds a multi-column data.frame, splits
# it into row groups via the writer, opens the resulting tdc container
# and reads it back, then verifies bit-identical equality column by
# column.
#
# String columns and per-column statistics are intentionally absent in
# P3 (gap tracked in VECTRA_REWIRE.md). VEC_STRING decode requires a
# tdc API extension; stats are a read-side filter-pushdown optimization
# that P4 may add.

VTR_COMPRESS_NONE  <- 0L
VTR_COMPRESS_FAST  <- 1L
VTR_COMPRESS_SMALL <- 2L

vtr_tdc_roundtrip <- function(df, rowgroup_size, comp_level) {
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df,
        as.integer(rowgroup_size), as.integer(comp_level),
        PACKAGE = "vectra")
  out <- .Call("C_read_vtr_tdc", path, PACKAGE = "vectra")
  attr(out, "row.names") <- .set_row_names(length(out[[1]]))
  class(out) <- "data.frame"
  out
}

test_that("multi-column data.frame round-trips byte-exactly across comp levels", {
  set.seed(1)
  n <- 5000L
  df <- data.frame(
    x_dbl = rnorm(n, 100, 25),
    x_int = sample.int(.Machine$integer.max, n) - 1L,
    x_lgl = sample(c(TRUE, FALSE), n, replace = TRUE),
    x_seq = as.double(seq_len(n)),
    stringsAsFactors = FALSE
  )

  for (level in c(VTR_COMPRESS_NONE, VTR_COMPRESS_FAST, VTR_COMPRESS_SMALL)) {
    for (rg in c(64L, 1024L, n)) {
      rt <- vtr_tdc_roundtrip(df, rg, level)
      expect_identical(names(rt), names(df),
                       info = sprintf("level=%d rg=%d", level, rg))
      for (col in names(df)) {
        expect_identical(rt[[col]], df[[col]],
                         info = sprintf("col=%s level=%d rg=%d",
                                        col, level, rg))
      }
    }
  }
})

test_that("single-rowgroup write matches the input exactly", {
  df <- data.frame(
    a = as.double(1:1024),
    b = 1024:1L,
    c = rep(c(TRUE, FALSE), 512),
    stringsAsFactors = FALSE
  )
  rt <- vtr_tdc_roundtrip(df, 4096L, VTR_COMPRESS_FAST)
  expect_identical(rt$a, df$a)
  expect_identical(rt$b, df$b)
  expect_identical(rt$c, df$c)
})

test_that("rowgroup size that does not divide n_rows works", {
  df <- data.frame(
    v = as.double(seq_len(2050)),
    stringsAsFactors = FALSE
  )
  rt <- vtr_tdc_roundtrip(df, 256L, VTR_COMPRESS_FAST)
  expect_identical(rt$v, df$v)
})

test_that("constant column compresses below raw and round-trips", {
  df <- data.frame(
    k = rep(3.14, 8192),
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 8192L, VTR_COMPRESS_FAST,
        PACKAGE = "vectra")
  raw_size <- length(df$k) * 8
  expect_lt(file.info(path)$size, raw_size)
  rt <- .Call("C_read_vtr_tdc", path, PACKAGE = "vectra")
  expect_identical(rt$k, df$k)
})
