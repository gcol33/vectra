test_that("create_index and has_index work", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df <- data.frame(
    name = c("alice", "bob", "charlie", "diana", "eve"),
    val = 1:5,
    stringsAsFactors = FALSE
  )
  write_vtr(df, f)

  expect_false(has_index(f, "name"))
  create_index(f, "name")
  expect_true(has_index(f, "name"))
})

test_that("hash index accelerates equality lookups on strings", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".genus.vtri"))))

  df <- data.frame(
    genus = rep(c("Quercus", "Pinus", "Fagus", "Betula", "Acer"), each = 100),
    val = seq_len(500),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 100L)
  create_index(f, "genus")

  result <- tbl(f) |> filter(genus == "Pinus") |> collect()
  expect_equal(nrow(result), 100L)
  expect_true(all(result$genus == "Pinus"))
})

test_that("hash index works on integer columns", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".id.vtri"))))

  df <- data.frame(id = 1:500, val = runif(500))
  write_vtr(df, f, batch_size = 50L)
  create_index(f, "id")

  result <- tbl(f) |> filter(id == 250) |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$id, 250)
})

test_that("hash index with case-insensitive flag", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df <- data.frame(
    name = c("Alice", "BOB", "Charlie"),
    val = 1:3,
    stringsAsFactors = FALSE
  )
  write_vtr(df, f)
  create_index(f, "name", ci = TRUE)

  # Lookup should match case-insensitively via the hash index
  # (the index provides the row groups, then filter does exact matching)
  # Since the index is CI, "alice" hashes to the same bucket as "Alice"
  expect_true(has_index(f, "name"))
})

test_that("hash index handles empty result", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df <- data.frame(
    name = c("a", "b", "c"),
    val = 1:3,
    stringsAsFactors = FALSE
  )
  write_vtr(df, f)
  create_index(f, "name")

  result <- tbl(f) |> filter(name == "zzz") |> collect()
  expect_equal(nrow(result), 0L)
})

test_that("hash index with multiple row groups returns correct results", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".key.vtri"))))

  # Key values are spread across row groups
  df <- data.frame(
    key = rep(letters[1:10], times = 50),
    val = seq_len(500),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 100L)
  create_index(f, "key")

  result <- tbl(f) |> filter(key == "e") |> collect()
  expect_equal(nrow(result), 50L)
  expect_true(all(result$key == "e"))
})

test_that("create_index errors on non-existent column", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(data.frame(x = 1:10), f)
  expect_error(create_index(f, "nonexistent"), "not found")
})

test_that("create_index errors on a column named twice", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(data.frame(x = 1:10, y = 1:10), f)
  expect_error(create_index(f, c("x", "x")), "twice")
})

test_that("index size tracks distinct keys, not row count", {
  # A single row group each time, so an index covers exactly the store's distinct
  # keys. Holding one entry per row instead would make the size follow `reps`.
  build <- function(n_keys, reps) {
    f <- tempfile(fileext = ".vtr")
    d <- data.frame(k = rep(sprintf("k%03d", seq_len(n_keys)), times = reps),
                    v = seq_len(n_keys * reps), stringsAsFactors = FALSE)
    write_vtr(d, f, batch_size = n_keys * reps)
    create_index(f, "k")
    on.exit(unlink(c(f, paste0(f, ".k.vtri"))), add = TRUE, after = FALSE)
    file.size(paste0(f, ".k.vtri"))
  }

  # 4x the rows over the same keys: unchanged.
  expect_equal(build(100L, 40L), build(100L, 10L))
  # 4x the keys over the same rows: larger.
  expect_gt(build(400L, 10L), build(100L, 40L))
})

test_that("an index left behind by a store rewrite is ignored, not probed", {
  f <- tempfile(fileext = ".vtr")
  ix <- paste0(f, ".k.vtri")
  on.exit(unlink(c(f, ix)))

  d1 <- data.frame(k = rep(c("a", "b"), each = 50), v = 1:100,
                   stringsAsFactors = FALSE)
  write_vtr(d1, f, batch_size = 25L)
  create_index(f, "k")
  saved <- readBin(ix, "raw", file.size(ix))

  # Rewrite the store with different data, then put the old index back.
  d2 <- data.frame(k = rep(c("a", "b", "c"), each = 50), v = 1:150,
                   stringsAsFactors = FALSE)
  write_vtr(d2, f, batch_size = 25L)
  writeBin(saved, ix)

  expect_false(has_index(f, "k"))
  expect_equal(nrow(collect(filter(tbl(f), k == "a"))), 50L)
  expect_equal(nrow(collect(filter(tbl(f), k == "c"))), 50L)
})

test_that("a row append rebuilds the index and stays complete", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".k.vtri"))))

  write_vtr(data.frame(k = c("a", "b", "c"), v = 1:3, stringsAsFactors = FALSE), f)
  create_index(f, "k")
  append_vtr(data.frame(k = c("a", "d"), v = 4:5, stringsAsFactors = FALSE), f)

  expect_true(has_index(f, "k"))
  expect_equal(nrow(collect(tbl(f))), 5L)
  expect_equal(nrow(collect(filter(tbl(f), k == "a"))), 2L)   # one old, one new
  expect_equal(nrow(collect(filter(tbl(f), k == "d"))), 1L)   # only in the append
})

test_that("a column append leaves the index usable", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".k.vtri"))))

  d <- data.frame(k = rep(c("a", "b"), each = 25), v = 1:50,
                  stringsAsFactors = FALSE)
  write_vtr(d, f, batch_size = 10L)
  create_index(f, "k")
  append_vtr(data.frame(w = seq_len(50) * 2), f, along = "cols")

  expect_true(has_index(f, "k"))
  r <- collect(filter(tbl(f), k == "b"))
  expect_equal(nrow(r), 25L)
  expect_true("w" %in% names(r))
})

test_that("each indexed column of a store is reachable", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, c(".ka.vtri", ".kb.vtri")))))

  set.seed(4)
  d <- data.frame(ka = sprintf("a%02d", rep(1:20, each = 25)),
                  kb = sprintf("b%02d", rep(1:25, times = 20)),
                  v = seq_len(500), stringsAsFactors = FALSE)
  write_vtr(d, f, batch_size = 50L)
  create_index(f, "ka")
  create_index(f, "kb")

  expect_true(has_index(f, "ka"))
  expect_true(has_index(f, "kb"))
  expect_match(paste(capture.output(explain(filter(tbl(f), ka == "a03"))),
                     collapse = " "), "hash index (ka)", fixed = TRUE)
  expect_match(paste(capture.output(explain(filter(tbl(f), kb == "b03"))),
                     collapse = " "), "hash index (kb)", fixed = TRUE)
  expect_equal(nrow(collect(filter(tbl(f), ka == "a03"))), 25L)
  expect_equal(nrow(collect(filter(tbl(f), kb == "b03"))), 20L)
})

test_that("explain reports no index where none applies", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".k.vtri"))))

  write_vtr(data.frame(k = letters, v = 1:26, stringsAsFactors = FALSE), f)
  create_index(f, "k")

  expect_false(grepl("hash index",
                     paste(capture.output(explain(filter(tbl(f), v > 3))),
                           collapse = " "), fixed = TRUE))
  expect_false(grepl("hash index",
                     paste(capture.output(explain(tbl(f))), collapse = " "),
                     fixed = TRUE))
})

test_that("%in% is answered through the index", {
  f <- tempfile(fileext = ".vtr")
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, g, paste0(f, ".k.vtri"))))

  d <- data.frame(k = rep(sprintf("k%02d", 1:20), each = 25), v = seq_len(500),
                  stringsAsFactors = FALSE)
  write_vtr(d, f, batch_size = 50L)
  write_vtr(d, g, batch_size = 50L)
  create_index(f, "k")

  keys <- c("k03", "k11")
  expect_equal(collect(filter(tbl(f), k %in% keys)),
               collect(filter(tbl(g), k %in% keys)))
  expect_equal(nrow(collect(filter(tbl(f), k %in% keys))), 50L)
})

test_that("composite index columns may be named in any order", {
  f <- tempfile(fileext = ".vtr")
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, g, paste0(f, ".ka_kb.vtri"))))

  d <- data.frame(ka = rep(c("a", "b"), each = 50),
                  kb = rep(c("x", "y"), times = 50),
                  v = seq_len(100), stringsAsFactors = FALSE)
  write_vtr(d, f, batch_size = 20L)
  write_vtr(d, g, batch_size = 20L)
  create_index(f, c("kb", "ka"))            # reverse of schema order

  expect_true(has_index(f, c("kb", "ka")))
  expect_true(has_index(f, c("ka", "kb")))
  expect_match(paste(capture.output(explain(filter(tbl(f), ka == "a", kb == "x"))),
                     collapse = " "), "hash index (ka + kb)", fixed = TRUE)
  expect_equal(collect(filter(tbl(f), ka == "a", kb == "x")),
               collect(filter(tbl(g), ka == "a", kb == "x")))
})

test_that("a composite index is preferred over a single-column one", {
  f <- tempfile(fileext = ".vtr")
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, g, paste0(f, c(".site.vtri", ".site_year.vtri")))))

  d <- data.frame(site = rep(sprintf("s%02d", 1:10), each = 40),
                  year = rep(2000:2009, times = 40),
                  v = seq_len(400), stringsAsFactors = FALSE)
  write_vtr(d, f, batch_size = 20L)
  write_vtr(d, g, batch_size = 20L)
  create_index(f, "site")
  create_index(f, c("site", "year"))

  # Both apply to this predicate; the composite prunes at least as well, since
  # it encodes the two keys co-occurring in a row group.
  expect_match(paste(capture.output(explain(filter(tbl(f), site == "s03",
                                                   year == 2005))),
                     collapse = " "), "hash index (site + year)", fixed = TRUE)
  # Only the single-column index covers a filter on site alone.
  expect_match(paste(capture.output(explain(filter(tbl(f), site == "s03"))),
                     collapse = " "), "hash index (site)", fixed = TRUE)

  expect_equal(collect(filter(tbl(f), site == "s03", year == 2005)),
               collect(filter(tbl(g), site == "s03", year == 2005)))
  expect_gt(nrow(collect(filter(tbl(f), site == "s03", year == 2005))), 0L)
  expect_equal(collect(filter(tbl(f), site == "s03")),
               collect(filter(tbl(g), site == "s03")))
})

test_that("a superseded index format reads as absent but names its columns", {
  f <- tempfile(fileext = ".vtr")
  ix <- paste0(f, ".k.vtri")
  on.exit(unlink(c(f, ix)))

  write_vtr(data.frame(k = letters, v = 1:26, stringsAsFactors = FALSE), f)

  con <- file(ix, "wb")                     # a v1 header: col_idx then ci
  writeBin(charToRaw("VTRI"), con)
  writeBin(1L, con, size = 2L)
  writeBin(0L, con, size = 2L)
  writeBin(as.raw(0), con)
  close(con)

  expect_false(has_index(f, "k"))
  expect_equal(nrow(collect(filter(tbl(f), k == "m"))), 1L)

  spec <- vectra:::.index_specs(f)
  expect_length(spec, 1L)
  expect_identical(spec[[1]]$columns, "k")

  vectra:::.rebuild_indexes(f)
  expect_true(has_index(f, "k"))
  expect_equal(nrow(collect(filter(tbl(f), k == "m"))), 1L)
})

test_that("index survives re-creation after data change", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df1 <- data.frame(name = c("a", "b"), val = 1:2, stringsAsFactors = FALSE)
  write_vtr(df1, f)
  create_index(f, "name")

  # Overwrite with new data
  df2 <- data.frame(name = c("x", "y", "z"), val = 1:3, stringsAsFactors = FALSE)
  write_vtr(df2, f)

  # Old index is stale — re-create
  create_index(f, "name")

  result <- tbl(f) |> filter(name == "y") |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$name, "y")
})

# ── an index after a row append ───────────────────────────────────────────────

test_that("a row append leaves each index usable and covering the new rows", {
  # A row append moves no existing row group, so an index's entries stay true
  # and it only has to take in the appended groups. What matters here is the
  # result: keys that appear only in the appended rows are still found, and
  # keys on both sides find both.
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  write_vtr(data.frame(name = c("a", "b", "c"), val = 1:3,
                       stringsAsFactors = FALSE), f)
  create_index(f, "name")
  expect_true(has_index(f, "name"))

  append_vtr(data.frame(name = c("d", "a"), val = 4:5,
                        stringsAsFactors = FALSE), f)

  expect_true(has_index(f, "name"))

  only_new <- tbl(f) |> filter(name == "d") |> collect()
  expect_equal(nrow(only_new), 1L)
  expect_equal(only_new$val, 4)

  both <- tbl(f) |> filter(name == "a") |> collect()
  expect_equal(sort(both$val), c(1, 5))

  absent <- tbl(f) |> filter(name == "zzz") |> collect()
  expect_equal(nrow(absent), 0L)
})

test_that("indexes stay correct across repeated row appends", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  write_vtr(data.frame(name = "k0", val = 0L, stringsAsFactors = FALSE), f)
  create_index(f, "name")

  for (i in 1:8)
    append_vtr(data.frame(name = c(paste0("k", i), "shared"),
                          val = c(i, 100L + i), stringsAsFactors = FALSE), f)

  expect_true(has_index(f, "name"))
  expect_equal(nrow(tbl(f) |> filter(name == "k5") |> collect()), 1L)
  expect_equal(tbl(f) |> filter(name == "k5") |> collect() |> getElement("val"), 5)
  expect_equal(nrow(tbl(f) |> filter(name == "shared") |> collect()), 8L)
  expect_equal(nrow(tbl(f) |> collect()), 17L)
})

test_that("a composite index survives a row append", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".a.b.vtri"))))

  write_vtr(data.frame(a = c("x", "y"), b = c("p", "q"), val = 1:2,
                       stringsAsFactors = FALSE), f)
  create_index(f, c("a", "b"))
  expect_true(has_index(f, c("a", "b")))

  append_vtr(data.frame(a = c("z", "x"), b = c("r", "p"), val = 3:4,
                        stringsAsFactors = FALSE), f)

  expect_true(has_index(f, c("a", "b")))
  expect_equal(nrow(tbl(f) |> filter(a == "z", b == "r") |> collect()), 1L)
  expect_equal(sort(tbl(f) |> filter(a == "x", b == "p") |> collect() |>
                      getElement("val")), c(1, 4))
})

# ── an unusable sidecar costs speed, never rows ───────────────────────────────

test_that("a malformed .vtri degrades to a scan rather than failing the read", {
  # An index only ever saves a scan work, so nothing about a query's
  # correctness rests on one. A sidecar that cannot be read has to report as
  # absent, not turn a readable store into an unopenable one.
  f <- tempfile(fileext = ".vtr")
  idx <- paste0(f, ".name.vtri")
  on.exit(unlink(c(f, idx)))

  df <- data.frame(name = c("a", "b", "c"), val = 1:3, stringsAsFactors = FALSE)
  write_vtr(df, f)
  create_index(f, "name")
  expect_true(has_index(f, "name"))

  # Truncate the sidecar: its header now promises far more than the file holds,
  # which is what the size guard rejects.
  raw <- readBin(idx, "raw", file.size(idx))
  writeBin(raw[seq_len(length(raw) %/% 2L)], idx)

  expect_false(has_index(f, "name"))
  res <- tbl(f) |> filter(name == "b") |> collect()
  expect_equal(nrow(res), 1L)
  expect_equal(res$val, 2)
  expect_equal(nrow(tbl(f) |> collect()), 3L)
})

test_that("a .vtri of garbage degrades to a scan", {
  f <- tempfile(fileext = ".vtr")
  idx <- paste0(f, ".name.vtri")
  on.exit(unlink(c(f, idx)))

  write_vtr(data.frame(name = c("a", "b", "c"), val = 1:3,
                       stringsAsFactors = FALSE), f)
  create_index(f, "name")

  # Keep the magic, wreck the counts that follow it.
  raw <- readBin(idx, "raw", file.size(idx))
  raw[5:length(raw)] <- as.raw(0xff)
  writeBin(raw, idx)

  expect_false(has_index(f, "name"))
  res <- tbl(f) |> filter(name == "c") |> collect()
  expect_equal(nrow(res), 1L)
  expect_equal(res$val, 3)
})
