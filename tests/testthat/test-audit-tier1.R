# Regression tests for the Tier-1 audit fixes (2026-07-18):
#   - SQLite text/blob reader length mismatch (heap over-read + 64KB truncation)
#   - CSV UTF-8 BOM in the header; late-appearing types past the inference window
#   - mutate() left-to-right sequencing (reference a column made in the same call)
#   - integer-dtype raster output collapsing NA/nodata to 0

# --- helper: write CSV lines with guaranteed LF (no CRLF translation) ----------
.write_csv_lf <- function(path, lines) {
  con <- file(path, "wb")
  on.exit(close(con))
  writeLines(lines, con, sep = "\n")
}

# ---- SQLite -------------------------------------------------------------------

test_that("SQLite reads a text value larger than 64KB without truncation or over-read", {
  db <- tempfile(fileext = ".sqlite"); on.exit(unlink(db))
  big <- paste(rep("abcdefghij", 12000), collapse = "")   # 120000 chars
  df <- data.frame(id = 1:2, txt = c(big, "short"), stringsAsFactors = FALSE)
  write_sqlite(df, db, "t")
  r <- tbl_sqlite(db, "t") |> collect()
  expect_equal(nchar(r$txt[1]), nchar(big))               # was capped at 65535
  expect_equal(r$txt[1], big)
  expect_equal(r$txt[2], "short")
})

# ---- CSV ----------------------------------------------------------------------

test_that("CSV UTF-8 BOM is stripped from the first column name", {
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f))
  con <- file(f, "wb")
  writeBin(as.raw(c(0xEF, 0xBB, 0xBF)), con)              # UTF-8 BOM
  writeBin(charToRaw("id,name\n1,alice\n2,bob\n"), con)
  close(con)
  r <- collect(tbl_csv(f))
  expect_true("id" %in% names(r))
  expect_false(any(grepl("﻿", names(r), useBytes = TRUE)))
  expect_equal(r$id, c(1, 2))
})

test_that("guess_max controls a type that only appears past the inference window", {
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f))
  .write_csv_lf(f, c("x", as.character(1:1200), "3.5"))
  # default guess_max = 1000: column inferred int64, the trailing 3.5 -> NA
  # (and now a warning names the offending column).
  expect_warning(d1 <- collect(tbl_csv(f)), "do not match the column type")
  expect_true(is.na(d1$x[1201]))
  # whole-file inference widens the column to double, preserving 3.5
  d2 <- collect(tbl_csv(f, guess_max = Inf))
  expect_equal(d2$x[1201], 3.5)
})

test_that("a non-bool value past guess_max becomes NA, not silently FALSE", {
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f))
  .write_csv_lf(f, c("b", rep("TRUE", 1200), "maybe"))
  expect_warning(d <- collect(tbl_csv(f)), "do not match the column type")
  expect_true(all(d$b[1:1200]))
  expect_true(is.na(d$b[1201]))
})

# ---- mutate() sequencing ------------------------------------------------------

test_that("mutate can reference a column created earlier in the same call", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = c(1, 2, 3)), f)
  r <- collect(mutate(tbl(f), a = x + 1, b = a * 2, c = b + a))
  expect_equal(r$a, c(2, 3, 4))
  expect_equal(r$b, c(4, 6, 8))
  expect_equal(r$c, c(6, 9, 12))
})

test_that("mutate uses a same-call column, not a same-named binding in scope", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = c(10, 20)), f)
  a <- 999
  r <- collect(mutate(tbl(f), a = x + 1, b = a * 2))
  expect_equal(r$b, c(22, 42))          # (x+1)*2, not 999*2
})

test_that("mutate reassigning a column then using it sees the new value", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = c(1, 2, 3)), f)
  r <- collect(mutate(tbl(f), x = x + 10, y = x * 2))
  expect_equal(r$x, c(11, 12, 13))
  expect_equal(r$y, c(22, 24, 26))
})

test_that("independent mutate columns are unaffected", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = c(1, 2), y = c(3, 4)), f)
  r <- collect(mutate(tbl(f), a = x + 1, b = y + 1))
  expect_equal(r$a, c(2, 3))
  expect_equal(r$b, c(4, 5))
})

# ---- raster integer nodata ----------------------------------------------------

test_that("integer-dtype raster round-trips NA instead of collapsing to 0", {
  f <- tempfile(fileext = ".vec"); on.exit(unlink(f))
  m <- matrix(c(1, NA, 3, 4, NA, 6), nrow = 2, byrow = TRUE)
  vec_write_raster(m, f, dtype = "i16", extent = c(0, 0, 3, 2))
  r <- vec_open_raster(f)
  w <- vec_read_window(r)
  expect_equal(sum(is.na(w)), 2L)                     # the two NA cells survive
  expect_equal(sort(w[!is.na(w)]), c(1, 3, 4, 6))     # data unchanged
})
