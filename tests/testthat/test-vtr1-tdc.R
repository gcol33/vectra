# P3 + P4: tdc-backed row-group container — round-trip via vtr1_tdc.
#
# Exercises src/vtr1_tdc.c. Builds a multi-column data.frame, splits
# it into row groups via the writer, opens the resulting tdc container
# and reads it back, then verifies bit-identical equality column by
# column.
#
# String columns land in P4d via tdc_decode_block_varlen; the dedicated
# round-trip cases live at the bottom of this file (basic, NA, multi-
# rowgroup, unicode, varied length).

VTR_COMPRESS_NONE  <- 0L
VTR_COMPRESS_FAST  <- 1L
VTR_COMPRESS_SMALL <- 2L

vtr_tdc_roundtrip <- function(df, rowgroup_size, comp_level,
                              annotations = NULL) {
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df,
        as.integer(rowgroup_size), as.integer(comp_level),
        annotations,
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
  .Call("C_write_vtr_tdc", path, df, 8192L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")
  raw_size <- length(df$k) * 8
  expect_lt(file.info(path)$size, raw_size)
  rt <- .Call("C_read_vtr_tdc", path, PACKAGE = "vectra")
  expect_identical(rt$k, df$k)
})

# ----- P4a: per-column user annotations propagate end-to-end -----------------

test_that("user annotations round-trip through the schema slot", {
  df <- data.frame(
    a = as.double(1:32),
    b = 1:32L,
    c = rep(c(TRUE, FALSE), 16),
    stringsAsFactors = FALSE
  )
  ann <- c("factor|low|mid|high", NA, "units=kg")

  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 32L, VTR_COMPRESS_FAST, ann,
        PACKAGE = "vectra")

  back <- .Call("C_read_vtr_tdc_annotations", path, PACKAGE = "vectra")
  expect_identical(back, ann)

  # Empty-string annotation collapses to NA on the read side.
  ann2 <- c("", "x", "")
  .Call("C_write_vtr_tdc", path, df, 32L, VTR_COMPRESS_FAST, ann2,
        PACKAGE = "vectra")
  back2 <- .Call("C_read_vtr_tdc_annotations", path, PACKAGE = "vectra")
  expect_identical(back2, c(NA_character_, "x", NA_character_))
})

test_that("annotation roundtrip survives multi-rowgroup writes", {
  df <- data.frame(
    x = as.double(1:1000),
    y = rep(c(TRUE, FALSE), 500),
    stringsAsFactors = FALSE
  )
  ann <- c("scale=0.5", "factor|TRUE|FALSE")
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 128L, VTR_COMPRESS_FAST, ann,
        PACKAGE = "vectra")
  back <- .Call("C_read_vtr_tdc_annotations", path, PACKAGE = "vectra")
  expect_identical(back, ann)
  # Body data still round-trips byte-exactly with annotations attached.
  rt <- .Call("C_read_vtr_tdc", path, PACKAGE = "vectra")
  expect_identical(rt$x, df$x)
  expect_identical(rt$y, df$y)
})

# ----- P4a: per-column statistics round-trip ---------------------------------

test_that("stats round-trip min/max/null_count for double/int/bool", {
  df <- data.frame(
    d = c(1.5, 2.5, 3.5, 4.5, 5.5),
    i = c(10L, 20L, 30L, 40L, 50L),
    b = c(TRUE, TRUE, FALSE, TRUE, FALSE),
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 5L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")

  stats <- .Call("C_read_vtr_tdc_stats", path, PACKAGE = "vectra")
  expect_length(stats, 1L)        # one rowgroup
  expect_length(stats[[1]], 3L)   # three columns

  # double
  v <- stats[[1]][[1]]
  expect_equal(v[1], 1)             # has_stats
  expect_equal(v[2], 1.5)           # min
  expect_equal(v[3], 5.5)           # max
  expect_equal(v[4], 0)             # null_count

  # int (stored at int64 granularity in stats)
  v <- stats[[1]][[2]]
  expect_equal(v[1], 1)
  expect_equal(v[2], 10)
  expect_equal(v[3], 50)
  expect_equal(v[4], 0)

  # bool: layout is {has_false, has_true} in the {min, max} slots
  v <- stats[[1]][[3]]
  expect_equal(v[1], 1)
  expect_equal(v[2], 1)             # has_false
  expect_equal(v[3], 1)             # has_true
  expect_equal(v[4], 0)
})

test_that("stats split correctly across multiple rowgroups", {
  df <- data.frame(
    x = c(seq(1, 100), seq(201, 300)) * 1.0,  # rg1 in [1,100], rg2 in [201,300]
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 100L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")

  stats <- .Call("C_read_vtr_tdc_stats", path, PACKAGE = "vectra")
  expect_length(stats, 2L)

  v1 <- stats[[1]][[1]]
  expect_equal(v1[2], 1)
  expect_equal(v1[3], 100)

  v2 <- stats[[2]][[1]]
  expect_equal(v2[2], 201)
  expect_equal(v2[3], 300)
})

test_that("constant column produces equal min and max", {
  df <- data.frame(
    k = rep(42.0, 256),
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 256L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")
  stats <- .Call("C_read_vtr_tdc_stats", path, PACKAGE = "vectra")
  v <- stats[[1]][[1]]
  expect_equal(v[1], 1)
  expect_equal(v[2], 42)
  expect_equal(v[3], 42)
})

test_that("bool-only-true column reports has_false=0", {
  df <- data.frame(
    b = rep(TRUE, 128),
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 128L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")
  stats <- .Call("C_read_vtr_tdc_stats", path, PACKAGE = "vectra")
  v <- stats[[1]][[1]]
  expect_equal(v[1], 1)
  expect_equal(v[2], 0)             # has_false
  expect_equal(v[3], 1)             # has_true
})

# ----- P4b: NA round-trip via validity bitmap --------------------------------

test_that("NA values round-trip for double / int / logical, single rowgroup", {
  df <- data.frame(
    d = c(1.5, NA_real_, 3.5, NA_real_, 5.5),
    i = c(NA_integer_, 20L, 30L, NA_integer_, 50L),
    b = c(TRUE, NA, FALSE, NA, TRUE),
    stringsAsFactors = FALSE
  )
  rt <- vtr_tdc_roundtrip(df, 5L, VTR_COMPRESS_FAST)
  expect_identical(rt$d, df$d)
  expect_identical(rt$i, df$i)
  expect_identical(rt$b, df$b)
})

test_that("NA values round-trip across rowgroup boundaries", {
  set.seed(42)
  n <- 1000L
  d <- rnorm(n)
  d[sample.int(n, 200)] <- NA_real_
  i <- sample.int(.Machine$integer.max, n) - 1L
  i[sample.int(n, 150)] <- NA_integer_
  b <- sample(c(TRUE, FALSE), n, replace = TRUE)
  b[sample.int(n, 100)] <- NA
  df <- data.frame(d = d, i = i, b = b, stringsAsFactors = FALSE)

  for (rg in c(64L, 128L, 333L, n)) {
    for (level in c(VTR_COMPRESS_NONE, VTR_COMPRESS_FAST,
                    VTR_COMPRESS_SMALL)) {
      rt <- vtr_tdc_roundtrip(df, rg, level)
      expect_identical(rt$d, df$d, info = sprintf("rg=%d lvl=%d", rg, level))
      expect_identical(rt$i, df$i, info = sprintf("rg=%d lvl=%d", rg, level))
      expect_identical(rt$b, df$b, info = sprintf("rg=%d lvl=%d", rg, level))
    }
  }
})

test_that("all-NA column round-trips correctly", {
  df <- data.frame(
    d = rep(NA_real_, 256),
    i = rep(NA_integer_, 256),
    b = rep(NA, 256),
    stringsAsFactors = FALSE
  )
  rt <- vtr_tdc_roundtrip(df, 64L, VTR_COMPRESS_FAST)
  expect_identical(rt$d, df$d)
  expect_identical(rt$i, df$i)
  expect_identical(rt$b, df$b)
})

test_that("NA at rowgroup boundary positions survives", {
  # NAs deliberately placed at first / last index of each rowgroup.
  rg <- 32L
  n  <- 128L
  d <- as.double(seq_len(n))
  edges <- c(1L, rg, rg + 1L, 2L * rg, 2L * rg + 1L, n)
  d[edges] <- NA_real_
  df <- data.frame(d = d, stringsAsFactors = FALSE)
  rt <- vtr_tdc_roundtrip(df, rg, VTR_COMPRESS_FAST)
  expect_identical(rt$d, df$d)
})

# ----- P4c: parallel reader correctness --------------------------------------

test_that("many-rowgroup file decodes correctly under parallel reader", {
  # 200 rowgroups exercises OpenMP scheduling and per-thread FILE* handling.
  set.seed(7)
  n <- 20000L
  df <- data.frame(
    d = rnorm(n),
    i = sample.int(.Machine$integer.max, n) - 1L,
    b = sample(c(TRUE, FALSE), n, replace = TRUE),
    stringsAsFactors = FALSE
  )
  # Salt with NAs every 73rd row across all three types.
  na_idx <- seq.int(1L, n, by = 73L)
  df$d[na_idx] <- NA_real_
  df$i[na_idx] <- NA_integer_
  df$b[na_idx] <- NA

  rt <- vtr_tdc_roundtrip(df, 100L, VTR_COMPRESS_FAST)  # 200 rowgroups
  expect_identical(rt$d, df$d)
  expect_identical(rt$i, df$i)
  expect_identical(rt$b, df$b)
})

test_that("null_count stat reflects NA presence", {
  df <- data.frame(
    d = c(1.0, NA_real_, 3.0, NA_real_, 5.0),
    i = c(NA_integer_, NA_integer_, 30L, 40L, 50L),
    b = c(TRUE, NA, NA, NA, FALSE),
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 5L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")
  stats <- .Call("C_read_vtr_tdc_stats", path, PACKAGE = "vectra")
  expect_equal(stats[[1]][[1]][4], 2)  # d: 2 NAs
  expect_equal(stats[[1]][[2]][4], 2)  # i: 2 NAs
  expect_equal(stats[[1]][[3]][4], 3)  # b: 3 NAs

  # min/max ignore NAs.
  expect_equal(stats[[1]][[1]][2], 1)
  expect_equal(stats[[1]][[1]][3], 5)
  expect_equal(stats[[1]][[2]][2], 30)
  expect_equal(stats[[1]][[2]][3], 50)
})

# ---------------------------------------------------------------------------
# P4d — VEC_STRING round-trip
# ---------------------------------------------------------------------------

test_that("string column round-trips byte-exactly across rowgroup sizes", {
  s <- c("alpha", "beta", "", "gamma", "delta", "epsilon", "zeta", "eta")
  df <- data.frame(s = s, stringsAsFactors = FALSE)
  for (rg in c(1L, 2L, 3L, 8L, 64L)) {
    rt <- vtr_tdc_roundtrip(df, rg, VTR_COMPRESS_FAST)
    expect_identical(rt$s, df$s, info = sprintf("rg=%d", rg))
  }
})

test_that("string column with NA_character_ round-trips and counts nulls", {
  df <- data.frame(
    s = c("a", NA_character_, "", "longer string", NA_character_, "z"),
    stringsAsFactors = FALSE
  )
  path <- tempfile(fileext = ".vtdc")
  on.exit(unlink(path), add = TRUE)
  .Call("C_write_vtr_tdc", path, df, 6L, VTR_COMPRESS_FAST, NULL,
        PACKAGE = "vectra")
  out <- .Call("C_read_vtr_tdc", path, PACKAGE = "vectra")
  expect_identical(out$s, df$s)

  stats <- .Call("C_read_vtr_tdc_stats", path, PACKAGE = "vectra")
  expect_equal(stats[[1]][[1]][4], 2)  # 2 NA strings
})

test_that("string column survives many rowgroups (boundary stress)", {
  s <- sprintf("row-%05d", 0:999)
  df <- data.frame(s = s, stringsAsFactors = FALSE)
  rt <- vtr_tdc_roundtrip(df, 137L, VTR_COMPRESS_FAST)  # 8 rowgroups
  expect_identical(rt$s, df$s)
})

test_that("unicode and varied-length strings round-trip", {
  df <- data.frame(
    s = c("café", "naïve", "日本語", "emoji \U1F600",
          strrep("x", 1000), "", "single"),
    stringsAsFactors = FALSE
  )
  rt <- vtr_tdc_roundtrip(df, 3L, VTR_COMPRESS_FAST)
  expect_identical(rt$s, df$s)
})

test_that("string columns mix with numeric/logical columns in one container", {
  set.seed(7)
  n <- 500L
  df <- data.frame(
    name  = sprintf("item-%03d", seq_len(n)),
    value = rnorm(n),
    flag  = sample(c(TRUE, FALSE), n, replace = TRUE),
    stringsAsFactors = FALSE
  )
  rt <- vtr_tdc_roundtrip(df, 73L, VTR_COMPRESS_FAST)
  expect_identical(rt$name,  df$name)
  expect_identical(rt$value, df$value)
  expect_identical(rt$flag,  df$flag)
})
