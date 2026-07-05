# Streaming BED scan backend (tbl_bed).
#
# Feature lines become rows with the standard BED columns; coordinates are read
# faithfully (0-based start, half-open end). Recovery-tested against a hand-built
# fixture whose columns are known exactly, and -- the headline of phase A4 --
# against GenomicRanges::findOverlaps for the interval-overlap use case that
# tbl_bed + interval_join is meant to serve. Half-open / 0-based boundary cases
# (the classic BED off-by-one) are exercised explicitly.

open_con <- function(path, mode = "w") {
  if (grepl("\\.gz$", path)) gzfile(path, mode) else file(path, mode)
}

write_bed <- function(lines, path) {
  con <- open_con(path); on.exit(close(con))
  writeLines(lines, con)
}

# ---- column schema, types, faithful coordinates ---------------------------

test_that("tbl_bed reads BED3 with faithful coordinates and correct types", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(c("chr1\t100\t200", "chr1\t150\t400", "chr2\t500\t900"), f)
  d <- tbl_bed(f, quiet = TRUE) |> collect()

  expect_named(d, c("chrom", "start", "end"))
  expect_type(d$chrom, "character")
  expect_type(d$start, "double")           # int64 surfaces as double
  expect_type(d$end, "double")
  expect_equal(d$chrom, c("chr1", "chr1", "chr2"))
  expect_equal(d$start, c(100, 150, 500))  # 0-based, unchanged
  expect_equal(d$end, c(200, 400, 900))    # half-open, unchanged
})

test_that("tbl_bed names the standard BED12 columns in order", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(paste("chr1", 0, 100, "n", 0, "+", 0, 100, "255,0,0", 2,
                  "10,20", "0,80", sep = "\t"), f)
  d <- tbl_bed(f, quiet = TRUE) |> collect()
  expect_named(d, c("chrom", "start", "end", "name", "score", "strand",
                    "thickStart", "thickEnd", "itemRgb", "blockCount",
                    "blockSizes", "blockStarts"))
  expect_type(d$score, "double")
  expect_type(d$itemRgb, "character")       # "255,0,0" stays a string
  expect_type(d$blockSizes, "character")
  expect_equal(d$blockSizes, "10,20")
})

test_that("tbl_bed names fields past the twelfth V13, V14, ...", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(paste("chr1", 0, 100, "n", 0, "+", 0, 100, "0", 1, "100", "0",
                  "extraA", "extraB", sep = "\t"), f)
  d <- tbl_bed(f, quiet = TRUE) |> collect()
  expect_equal(tail(names(d), 2), c("V13", "V14"))
  expect_equal(d$V13, "extraA")
  expect_equal(d$V14, "extraB")
})

# ---- dialect: whitespace, comments, missing integer fields ----------------

test_that("tbl_bed accepts both tab- and space-delimited features", {
  ftab <- tempfile(fileext = ".bed"); on.exit(unlink(ftab))
  fsp  <- tempfile(fileext = ".bed"); on.exit(unlink(fsp), add = TRUE)
  write_bed(c("chr1\t100\t200\ta", "chr2\t300\t400\tb"), ftab)
  write_bed(c("chr1 100 200 a",    "chr2 300 400 b"),    fsp)
  expect_equal(tbl_bed(ftab, quiet = TRUE) |> collect(),
               tbl_bed(fsp,  quiet = TRUE) |> collect())
})

test_that("tbl_bed skips blank, comment, track, and browser lines", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(c("browser position chr1:1-1000",
              "track name=peaks description=\"x\"",
              "# a comment",
              "",
              "chr1 100 200 a",
              "chr2 300 400 b"), f)
  d <- tbl_bed(f, quiet = TRUE) |> collect()
  expect_equal(nrow(d), 2L)
  expect_equal(d$name, c("a", "b"))
})

test_that("tbl_bed maps '.' and 'NA' score to NA, keeps strand '.' literal", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(c("chr1 100 200 a 0  +",
              "chr1 150 400 b .  -",
              "chr2 500 900 c NA ."), f)
  d <- tbl_bed(f, quiet = TRUE) |> collect()
  expect_equal(d$score, c(0, NA, NA))
  expect_equal(d$strand, c("+", "-", "."))   # strand is a string; '.' is valid
})

# ---- gzip, streaming invariance -------------------------------------------

test_that("tbl_bed gzip input matches plain", {
  lines <- sprintf("chr%d\t%d\t%d\tf%d", rep(1:3, 10), (1:30) * 10,
                   (1:30) * 10 + 5, 1:30)
  f  <- tempfile(fileext = ".bed");    on.exit(unlink(f))
  fg <- tempfile(fileext = ".bed.gz"); on.exit(unlink(fg), add = TRUE)
  write_bed(lines, f)
  write_bed(lines, fg)
  expect_equal(tbl_bed(f,  quiet = TRUE) |> collect(),
               tbl_bed(fg, quiet = TRUE) |> collect())
})

test_that("tbl_bed is invariant to feature batching", {
  lines <- sprintf("chr%d\t%d\t%d\tf%d", rep(1:4, 75), (1:300) * 3,
                   (1:300) * 3 + 2, 1:300)
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(lines, f)
  one  <- tbl_bed(f, quiet = TRUE) |> collect()
  many <- tbl_bed(f, batch_size = 13, quiet = TRUE) |> collect()
  expect_equal(one, many)
  expect_equal(nrow(one), 300L)
})

# ---- empty input, loud failures -------------------------------------------

test_that("an empty / comment-only BED yields zero rows (BED3 schema)", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(c("# only a comment", "track name=x"), f)
  d <- tbl_bed(f, quiet = TRUE) |> collect()
  expect_equal(nrow(d), 0L)
  expect_named(d, c("chrom", "start", "end"))
})

test_that("an inconsistent field count is a loud error", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed(c("chr1 100 200 a", "chr1 150 400"), f)   # 4 then 3 fields
  expect_error(tbl_bed(f, quiet = TRUE) |> collect(), "fields, expected")
})

test_that("a non-integer start or end is a loud error", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed("chr1 xx 200", f)
  expect_error(tbl_bed(f, quiet = TRUE) |> collect(), "non-integer")

  g <- tempfile(fileext = ".bed"); on.exit(unlink(g), add = TRUE)
  write_bed("chr1 100 yy", g)
  expect_error(tbl_bed(g, quiet = TRUE) |> collect(), "non-integer")
})

test_that("a feature line with fewer than three fields is a loud error", {
  f <- tempfile(fileext = ".bed"); on.exit(unlink(f))
  write_bed("chr1 100", f)
  expect_error(tbl_bed(f, quiet = TRUE) |> collect(), "at least 3")
})

# ---- interval overlap: recovery vs GenomicRanges (the A4 headline) ---------

# BED is 0-based half-open; a GRanges built from it is 1-based closed
# (start + 1, end). interval_join(closed = FALSE) requires a strictly positive
# overlap, which is exactly "share at least one base" for half-open intervals,
# so its matched pairs must equal findOverlaps() on the GRanges.
bed_pairs_from_join <- function(xb, yb, closed) {
  r <- interval_join(tbl_bed(xb, quiet = TRUE), tbl_bed(yb, quiet = TRUE),
                     start = "start", end = "end", by = "chrom",
                     closed = closed) |> collect()
  if (nrow(r) == 0) return(character(0))
  sort(paste(r$name, r$name.y, sep = "|"))
}

test_that("interval_join(closed = FALSE) over BED matches findOverlaps", {
  skip_if_not_installed("GenomicRanges")
  skip_if_not_installed("IRanges")

  set.seed(11)
  mk <- function(prefix, n) {
    chrom <- paste0("chr", sample(1:3, n, replace = TRUE))
    start <- sample(0:1000, n, replace = TRUE)
    width <- sample(1:60, n, replace = TRUE)
    data.frame(chrom = chrom, start = start, end = start + width,
               name = paste0(prefix, seq_len(n)), stringsAsFactors = FALSE)
  }
  X <- mk("x", 40); Y <- mk("y", 40)
  xb <- tempfile(fileext = ".bed"); on.exit(unlink(xb))
  yb <- tempfile(fileext = ".bed"); on.exit(unlink(yb), add = TRUE)
  write_bed(sprintf("%s\t%d\t%d\t%s", X$chrom, X$start, X$end, X$name), xb)
  write_bed(sprintf("%s\t%d\t%d\t%s", Y$chrom, Y$start, Y$end, Y$name), yb)

  ours <- bed_pairs_from_join(xb, yb, closed = FALSE)

  gx <- GenomicRanges::GRanges(X$chrom,
          IRanges::IRanges(start = X$start + 1L, end = X$end))
  gy <- GenomicRanges::GRanges(Y$chrom,
          IRanges::IRanges(start = Y$start + 1L, end = Y$end))
  hits <- GenomicRanges::findOverlaps(gx, gy)
  gr <- sort(paste(X$name[S4Vectors::queryHits(hits)],
                   Y$name[S4Vectors::subjectHits(hits)], sep = "|"))

  expect_equal(ours, gr)
})

test_that("half-open overlap: abutting features do not pair, one shared base does", {
  # x = [100, 200):  y1 = [200, 300) abuts (shares no base),
  #                  y2 = [150, 250) overlaps, y3 = [199, 205) shares base 199.
  xb <- tempfile(fileext = ".bed"); on.exit(unlink(xb))
  yb <- tempfile(fileext = ".bed"); on.exit(unlink(yb), add = TRUE)
  write_bed("chr1\t100\t200\tx", xb)
  write_bed(c("chr1\t200\t300\ty_abut",
              "chr1\t150\t250\ty_overlap",
              "chr1\t199\t205\ty_onebase"), yb)

  # closed = FALSE is the correct half-open semantics: abutting excluded.
  expect_equal(bed_pairs_from_join(xb, yb, closed = FALSE),
               sort(c("x|y_overlap", "x|y_onebase")))

  # closed = TRUE would additionally (incorrectly for BED) pair the abutting
  # feature -- this is the off-by-one the closed = FALSE recipe avoids.
  expect_true("x|y_abut" %in% bed_pairs_from_join(xb, yb, closed = TRUE))
})
