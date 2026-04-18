# P2a + P2b: tdc-backed encode/decode bridges — round-trip via tdc.
#
# Exercises src/vtr_codec_tdc.c. Encodes an R vector through
# vtr_encode_column_tdc, then decodes the resulting tdc block record via
# vtr_decode_column_tdc_into. Verifies bit-identical round-trip across
# REALSXP / INTSXP / LGLSXP.
#
# String round-trip is intentionally absent: tdc v0 has no public size
# query for variable-width payloads, so the bridge returns
# TDC_E_UNSUPPORTED for VEC_STRING. Strings land alongside the tdc API
# extension (tracked in VECTRA_REWIRE.md follow-ups).

VTR_COMPRESS_NONE  <- 0L
VTR_COMPRESS_FAST  <- 1L
VTR_COMPRESS_SMALL <- 2L

# Match the SEXPTYPE codes hard-coded in vtr_codec_tdc.c::r_sxp_to_vectype.
# These are stable R constants (Rinternals.h).
SXP_LGL  <- 10L
SXP_INT  <- 13L
SXP_REAL <- 14L

roundtrip <- function(x, comp_level) {
  rt <- switch(typeof(x),
    "double"  = SXP_REAL,
    "integer" = SXP_INT,
    "logical" = SXP_LGL,
    stop("unsupported R type: ", typeof(x))
  )
  raw_bytes <- .Call("C_tdc_encode_column", x, comp_level, PACKAGE = "vectra")
  decoded   <- .Call("C_tdc_decode_column", raw_bytes, length(x), rt,
                     PACKAGE = "vectra")
  list(raw = raw_bytes, decoded = decoded)
}

test_that("REALSXP round-trips at every comp_level", {
  set.seed(42)
  cases <- list(
    monotone     = as.double(seq_len(1024)),
    random       = runif(1024, -100, 100),
    constant     = rep(3.14, 512),
    mixed_signs  = rnorm(2000),
    small        = c(1.0, 2.0, 3.0)
  )
  for (level in c(VTR_COMPRESS_NONE, VTR_COMPRESS_FAST, VTR_COMPRESS_SMALL)) {
    for (nm in names(cases)) {
      x <- cases[[nm]]
      rt <- roundtrip(x, level)
      expect_identical(rt$decoded, x,
                       info = sprintf("REALSXP case=%s level=%d", nm, level))
    }
  }
})

test_that("INTSXP round-trips at every comp_level", {
  set.seed(7)
  cases <- list(
    monotone = seq_len(2048),
    random   = sample.int(.Machine$integer.max, 1024) - 1L,
    constant = rep(42L, 256),
    small    = c(-1L, 0L, 1L, 2L, 3L)
  )
  for (level in c(VTR_COMPRESS_NONE, VTR_COMPRESS_FAST, VTR_COMPRESS_SMALL)) {
    for (nm in names(cases)) {
      x <- cases[[nm]]
      rt <- roundtrip(x, level)
      expect_identical(rt$decoded, x,
                       info = sprintf("INTSXP case=%s level=%d", nm, level))
    }
  }
})

test_that("LGLSXP round-trips at every comp_level", {
  set.seed(13)
  cases <- list(
    all_true  = rep(TRUE, 1024),
    all_false = rep(FALSE, 1024),
    mixed     = sample(c(TRUE, FALSE), 4096, replace = TRUE),
    small     = c(TRUE, FALSE, TRUE)
  )
  for (level in c(VTR_COMPRESS_NONE, VTR_COMPRESS_FAST, VTR_COMPRESS_SMALL)) {
    for (nm in names(cases)) {
      x <- cases[[nm]]
      rt <- roundtrip(x, level)
      expect_identical(rt$decoded, x,
                       info = sprintf("LGLSXP case=%s level=%d", nm, level))
    }
  }
})

test_that("FAST compresses a low-entropy double vector below raw size", {
  x <- as.double(rep(seq_len(64), 256))     # 16384 doubles, very repetitive
  raw_size <- length(x) * 8
  rt <- roundtrip(x, VTR_COMPRESS_FAST)
  expect_lt(length(rt$raw), raw_size)
  expect_identical(rt$decoded, x)
})

test_that("NONE produces a passthrough block (no entropy stage)", {
  x <- runif(256)
  rt_none <- roundtrip(x, VTR_COMPRESS_NONE)
  rt_fast <- roundtrip(x, VTR_COMPRESS_FAST)
  expect_gte(length(rt_none$raw), length(x) * 8)
  expect_identical(rt_none$decoded, x)
  expect_identical(rt_fast$decoded, x)
})

test_that("empty vectors round-trip cleanly", {
  for (x in list(double(0), integer(0), logical(0))) {
    rt <- roundtrip(x, VTR_COMPRESS_FAST)
    expect_identical(rt$decoded, x)
  }
})
