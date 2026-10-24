# File size of a store grown by appends, and stores grown by earlier versions.

stored_schema <- function(path) .Call(vectra:::C_node_schema, tbl(path)$.node)

test_that("many small row appends give a file the size of one write of the same rows", {
  f <- tempfile(fileext = ".vtr"); g <- tempfile(fileext = ".vtr")
  h <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, g, h)))

  rows <- 200L
  n_app <- 500L
  mk <- function(i) {
    k <- (i - 1L) * rows + seq_len(rows)
    data.frame(id = k, x = k / 7, s = paste0("r", k %% 37L),
               stringsAsFactors = FALSE)
  }
  write_vtr(mk(1L), f, col_types = c(id = "int32"))
  for (i in 2:n_app) append_vtr(mk(i), f)

  all <- do.call(rbind, lapply(seq_len(n_app), mk))
  write_vtr(all, g, batch_size = rows, col_types = c(id = "int32"))
  write_vtr(all, h, col_types = c(id = "int32"))

  # Same row groups: the appended store holds one index, as a single write does.
  # (The default single write, h, packs the rows into one row group and is
  # smaller by the per-row-group cost of 500 small groups, which an append of
  # 200 rows cannot avoid.)
  expect_lte(file.size(f), 1.01 * file.size(g))

  expect_identical(collect(tbl(f)), collect(tbl(g)))
  expect_identical(collect(tbl(f)), collect(tbl(h)))
  expect_identical(stored_schema(f), stored_schema(g))
})

test_that("file size grows linearly with the number of appends", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  mk <- function(i) data.frame(a = (i - 1L) * 50L + 1:50, b = i * 1.5)
  write_vtr(mk(1L), f)
  size_at <- numeric(0)
  for (i in 2:400) {
    append_vtr(mk(i), f)
    if (i %in% c(100L, 200L, 400L)) size_at[as.character(i)] <- file.size(f)
  }
  per_100_early <- size_at[["200"]] - size_at[["100"]]
  per_100_late  <- (size_at[["400"]] - size_at[["200"]]) / 2
  expect_lte(per_100_late, 1.1 * per_100_early)
  expect_equal(nrow(collect(tbl(f))), 400L * 50L)
})

test_that("a column append to a store grown by row appends keeps one index", {
  f <- tempfile(fileext = ".vtr"); g <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, g)))
  mk <- function(i) data.frame(a = (i - 1L) * 20L + 1:20)
  write_vtr(mk(1L), f)
  for (i in 2:50) append_vtr(mk(i), f)
  append_vtr(data.frame(b = as.double(1000:1)), f, along = "cols")

  write_vtr(data.frame(a = 1:1000, b = as.double(1000:1)), g, batch_size = 20L)
  expect_identical(collect(tbl(f)), collect(tbl(g)))
  expect_lte(file.size(f), 1.1 * file.size(g))
})

test_that("a store grown by appends in vectra 0.13.4 reads and appends correctly", {
  # Written by 0.13.4: write_vtr() of 5 rows, then 5 append_vtr() calls of 5
  # rows each, which left one superseded row-group index behind per append.
  fixture <- test_path("fixtures", "appended-0.13.4.vtr")
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  file.copy(fixture, f)

  mk <- function(i) data.frame(id = (i - 1L) * 5L + 1:5,
                               x = (i - 1L) * 5 + (1:5) / 4,
                               s = paste0("r", (i - 1L) * 5L + 1:5),
                               stringsAsFactors = FALSE)
  want <- function(n) {
    w <- do.call(rbind, lapply(seq_len(n), mk))
    w$id <- as.double(w$id)
    w
  }

  expect_equal(collect(tbl(f)), want(6L), ignore_attr = TRUE)
  expect_equal(nrow(tbl(f)), 30)

  size_old <- file.size(f)
  for (i in 7:16) append_vtr(mk(i), f)
  expect_equal(collect(tbl(f)), want(16L), ignore_attr = TRUE)
  expect_identical(stored_schema(f), stored_schema(fixture))

  # The appends reuse the space of the index in use rather than add one each:
  # the file grows by about the size of the same rows written on their own.
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(g), add = TRUE)
  write_vtr(do.call(rbind, lapply(1:10, mk)), g, batch_size = 5L)
  expect_lte(file.size(f) - size_old, file.size(g))

  # Filters and an index still work on the grown store.
  expect_equal(collect(filter(tbl(f), id > 75))$id, as.double(76:80))
  create_index(f, "id")
  expect_true(has_index(f, "id"))
  expect_equal(collect(filter(tbl(f), id == 42))$s, "r42")
})

test_that("an append that fails part way leaves the store as it was, and the next one reuses its bytes", {
  f <- tempfile(fileext = ".vtr"); src <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, src, paste0(f, ".a.vtri"))))
  write_vtr(data.frame(a = 1:1000, b = (1:1000) / 3), f, batch_size = 250L)
  create_index(f, "a")
  before <- collect(tbl(f))
  fp_before <- vectra:::.store_fingerprint(f)
  size_before <- file.size(f)

  # A source whose first row group decodes and whose later ones do not, so the
  # append has written a row group when it stops.
  set.seed(1)
  write_vtr(data.frame(a = 1:6000, b = runif(6000)), src, batch_size = 2000L)
  r <- readBin(src, "raw", file.size(src))
  w <- readBin(r[17:24], "integer", n = 2L, size = 4L, endian = "little")
  r[floor(w[1] * 0.45):(w[1] - 1)] <- as.raw(0xAB)
  writeBin(r, src)

  expect_error(append_vtr(tbl(src), f), "decode")
  expect_gt(file.size(f), size_before)

  # Same rows, same fingerprint, so the index is still used.
  expect_identical(collect(tbl(f)), before)
  expect_identical(vectra:::.store_fingerprint(f), fp_before)
  expect_true(has_index(f, "a"))
  expect_equal(collect(filter(tbl(f), a == 777))$b, 777 / 3)

  append_vtr(data.frame(a = 1001:1010, b = 0), f)
  expect_equal(nrow(collect(tbl(f))), 1010L)
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(g), add = TRUE)
  write_vtr(data.frame(a = 1001:1010, b = 0), g)
  expect_lte(file.size(f), size_before + file.size(g))
})
