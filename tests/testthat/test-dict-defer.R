# Tests for the tdc dict-defer decode path (tdc_decode_block_dict), exercised
# through the C_tdc_dict_roundtrip bridge: encode a character vector as a
# DICT_1D block, decode it back as (dictionary + indices), and rebuild the
# vector by interning each unique value once and indexing.

rt <- function(x) .Call("C_tdc_dict_roundtrip", x)

test_that("dict-defer round-trips duplicated values", {
  x <- rep(c("alpha", "beta", "gamma", "delta"), 250)
  expect_identical(rt(x), x)
})

test_that("dict-defer round-trips wide heavily-duplicated strings", {
  uniq <- vapply(1:20, function(i) paste0("cat_", i, strrep("z", 30)), "")
  set.seed(1)
  x <- sample(uniq, 5000, replace = TRUE)
  expect_identical(rt(x), x)
})

test_that("dict-defer round-trips all-unique strings", {
  x <- as.character(1:500)
  expect_identical(rt(x), x)
})

test_that("dict-defer round-trips empty and mixed-empty strings", {
  expect_identical(rt(rep("", 10)), rep("", 10))
  x <- c("a", "", "bb", "a", "ccc", "")
  expect_identical(rt(x), x)
})

test_that("dict-defer round-trips UTF-8 values", {
  x <- rep(c("café", "naïve", "日本語", "α"), 50)
  expect_identical(rt(x), x)
})

test_that("dict-defer handles length-0 and length-1 vectors", {
  expect_identical(rt(character(0)), character(0))
  expect_identical(rt("solo"), "solo")
})

# End-to-end: the collect() direct-read path decodes DICT_1D string columns
# into deferred-dictionary form and fills the R character vector by index.
# These exercise the full write_vtr -> tbl -> collect path (not the isolated
# primitive), including the multi-rowgroup, NA, empty, and parallel-read cases.

collect_rt <- function(df, batch_size = NULL, compress = "fast") {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = batch_size, compress = compress)
  as.data.frame(collect(tbl(f)))
}

test_that("collect dict-defer round-trips duplicated strings over many row groups", {
  set.seed(2)
  vocab <- sprintf("category-label-with-some-width-%02d", 1:8)
  df <- data.frame(grp = sample(vocab, 60000, replace = TRUE),
                   stringsAsFactors = FALSE)
  out <- collect_rt(df, batch_size = 8000)
  expect_identical(out$grp, df$grp)
})

test_that("collect dict-defer preserves NA and empty strings", {
  vals <- c("alpha", "beta", "", NA, "alpha", "", "beta", NA, "gamma")
  df <- data.frame(x = rep(vals, 2000), stringsAsFactors = FALSE)
  out <- collect_rt(df, batch_size = 4000)
  expect_identical(out$x, df$x)
})

test_that("collect dict-defer preserves UTF-8 multibyte values", {
  u <- c("café", "naïve", "中文", "straße")
  df <- data.frame(s = rep(u, 3000), stringsAsFactors = FALSE)
  out <- collect_rt(df, batch_size = 2500)
  expect_identical(enc2utf8(out$s), enc2utf8(df$s))
})

test_that("collect dict-defer handles all-unique and all-NA columns", {
  df <- data.frame(k = sprintf("uniq-%06d", 1:20000),
                   allna = rep(NA_character_, 20000),
                   stringsAsFactors = FALSE)
  out <- collect_rt(df, batch_size = 4000)
  expect_identical(out$k, df$k)
  expect_identical(out$allna, df$allna)
})

test_that("collect dict-defer works with mixed string + numeric schema", {
  set.seed(3)
  df <- data.frame(a = sample(c("yes", "no", "maybe", NA), 30000, TRUE),
                   x = rnorm(30000),
                   b = sample(sprintf("tag-%02d", 1:5), 30000, TRUE),
                   stringsAsFactors = FALSE)
  out <- collect_rt(df, batch_size = 5000)
  expect_identical(out$a, df$a)
  expect_identical(out$b, df$b)
  expect_equal(out$x, df$x)
})

test_that("collect dict-defer matches flat decode under compress = none and small", {
  set.seed(4)
  vocab <- sprintf("val-%02d", 1:10)
  df <- data.frame(g = sample(vocab, 40000, replace = TRUE),
                   stringsAsFactors = FALSE)
  expect_identical(collect_rt(df, compress = "none")$g, df$g)
  expect_identical(collect_rt(df, compress = "small")$g, df$g)
})

test_that("collect dict-defer leaves the filtered (flat) path correct", {
  set.seed(5)
  vocab <- sprintf("lvl-%02d", 1:6)
  df <- data.frame(id = seq_len(50000),
                   g = sample(vocab, 50000, replace = TRUE),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 8000)
  out <- as.data.frame(collect(filter(tbl(f), id <= 200L)))
  expect_identical(out$g, df$g[df$id <= 200L])
})
