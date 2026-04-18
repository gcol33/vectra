# P2a: tdc-backed encode bridge — round-trip via tdc decoder.
#
# Exercises src/vtr_codec_tdc.c. Encodes a double vector through
# vtr_encode_column_tdc, then decodes the resulting tdc block record via
# tdc_decode_block_into. Verifies bit-identical round-trip.
#
# This is intentionally narrow (REALSXP only). The full multi-type decode
# bridge is P2b's deliverable.

VTR_COMPRESS_NONE  <- 0L
VTR_COMPRESS_FAST  <- 1L
VTR_COMPRESS_SMALL <- 2L

roundtrip_double <- function(x, comp_level) {
  raw_bytes <- .Call("C_tdc_encode_double", x, comp_level, PACKAGE = "vectra")
  decoded   <- .Call("C_tdc_decode_double", raw_bytes, length(x), PACKAGE = "vectra")
  list(raw = raw_bytes, decoded = decoded)
}

test_that("round-trip preserves double vector at every comp_level", {
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
      rt <- roundtrip_double(x, level)
      expect_equal(rt$decoded, x, tolerance = 0,
                   info = sprintf("case=%s level=%d", nm, level))
    }
  }
})

test_that("FAST compresses a low-entropy vector below raw size", {
  x <- as.double(rep(seq_len(64), 256))     # 16384 doubles, very repetitive
  raw_size <- length(x) * 8
  rt <- roundtrip_double(x, VTR_COMPRESS_FAST)
  expect_lt(length(rt$raw), raw_size)
  expect_equal(rt$decoded, x, tolerance = 0)
})

test_that("NONE produces a passthrough block (no entropy stage)", {
  x <- runif(256)
  rt_none <- roundtrip_double(x, VTR_COMPRESS_NONE)
  rt_fast <- roundtrip_double(x, VTR_COMPRESS_FAST)
  # NONE should not be smaller than FAST on random data — it's pure overhead
  # over the raw payload (block header + zero compression).
  expect_gte(length(rt_none$raw), length(x) * 8)
  expect_equal(rt_none$decoded, x, tolerance = 0)
  expect_equal(rt_fast$decoded, x, tolerance = 0)
})

test_that("empty vector round-trips cleanly", {
  rt <- roundtrip_double(double(0), VTR_COMPRESS_FAST)
  expect_equal(length(rt$decoded), 0L)
})
