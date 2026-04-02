## Tests for append_vtr, delete_vtr, and diff_vtr

# ── append_vtr ────────────────────────────────────────────────────────────────

test_that("append_vtr adds rows after existing rows", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  df1 <- data.frame(x = 1:5, y = letters[1:5], stringsAsFactors = FALSE)
  df2 <- data.frame(x = 6:10, y = letters[6:10], stringsAsFactors = FALSE)

  write_vtr(df1, f)
  append_vtr(df2, f)

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 10L)
  expect_equal(result$x, as.double(1:10))
  expect_equal(result$y, letters[1:10])
})

test_that("append_vtr works with data.frame input", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(mtcars[1:10, ], f)
  append_vtr(mtcars[11:20, ], f)

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 20L)
  expect_equal(result$mpg, mtcars$mpg[1:20])
})

test_that("append_vtr works with vectra_node input", {
  f  <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))

  df1 <- data.frame(v = as.double(1:4))
  df2 <- data.frame(v = as.double(5:8))

  write_vtr(df1, f)
  write_vtr(df2, f2)

  append_vtr(tbl(f2), f)

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 8L)
  expect_equal(result$v, as.double(1:8))
})

test_that("append_vtr rejects schema mismatch", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(data.frame(a = 1:3), f)
  expect_error(append_vtr(data.frame(b = 4:6), f), "mismatch")
})

test_that("append_vtr preserves multiple row groups", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  df <- data.frame(x = as.double(1:100))
  write_vtr(df, f, batch_size = 30L)   # 4 row groups
  append_vtr(data.frame(x = as.double(101:110)), f)  # 1 more row group

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 110L)
  expect_equal(result$x, as.double(1:110))
})

# ── delete_vtr ────────────────────────────────────────────────────────────────

test_that("delete_vtr removes specified rows", {
  f <- tempfile(fileext = ".vtr")
  del <- paste0(f, ".del")
  on.exit(unlink(c(f, del)))

  df <- data.frame(id = 1:5, stringsAsFactors = FALSE)
  write_vtr(df, f)

  # 0-based: index 0 = id 1, index 2 = id 3, index 4 = id 5
  delete_vtr(f, c(0, 2, 4))

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 2L)
  expect_equal(result$id, c(2, 4))
})

test_that("delete_vtr tombstone file is created", {
  f <- tempfile(fileext = ".vtr")
  del <- paste0(f, ".del")
  on.exit(unlink(c(f, del)))

  write_vtr(data.frame(x = 1:3), f)
  delete_vtr(f, 1L)

  expect_true(file.exists(del))
})

test_that("delete_vtr is cumulative across calls", {
  f <- tempfile(fileext = ".vtr")
  del <- paste0(f, ".del")
  on.exit(unlink(c(f, del)))

  df <- data.frame(id = 1:6)
  write_vtr(df, f)

  delete_vtr(f, 0L)   # delete row 1
  delete_vtr(f, 2L)   # delete row 3

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 4L)
  expect_equal(result$id, c(2, 4, 5, 6))
})

test_that("delete_vtr deduplicates tombstone entries", {
  f <- tempfile(fileext = ".vtr")
  del <- paste0(f, ".del")
  on.exit(unlink(c(f, del)))

  write_vtr(data.frame(x = 1:5), f)
  delete_vtr(f, c(1, 1, 1))   # same row multiple times

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 4L)
})

test_that("delete_vtr works across multiple row groups", {
  f <- tempfile(fileext = ".vtr")
  del <- paste0(f, ".del")
  on.exit(unlink(c(f, del)))

  df <- data.frame(id = 1:9)
  write_vtr(df, f, batch_size = 3L)   # 3 row groups of 3 rows

  # Delete last row of group 0 (idx 2), first of group 1 (idx 3)
  delete_vtr(f, c(2, 3))

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 7L)
  expect_equal(result$id, c(1, 2, 5, 6, 7, 8, 9))
})

test_that("delete all rows in a group skips it cleanly", {
  f <- tempfile(fileext = ".vtr")
  del <- paste0(f, ".del")
  on.exit(unlink(c(f, del)))

  df <- data.frame(id = 1:6)
  write_vtr(df, f, batch_size = 3L)   # 2 row groups: [0,1,2] and [3,4,5]

  delete_vtr(f, c(0, 1, 2))   # delete entire first row group

  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 3L)
  expect_equal(result$id, c(4, 5, 6))
})

test_that("tbl with no .del file works as before", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  df <- data.frame(x = 1:10)
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 10L)
})

# ── diff_vtr ──────────────────────────────────────────────────────────────────

test_that("diff_vtr detects added and deleted rows", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  df1 <- data.frame(id = 1:5, val = letters[1:5], stringsAsFactors = FALSE)
  df2 <- data.frame(id = c(3L, 4L, 5L, 6L, 7L),
                    val = c("c", "d", "e", "f", "g"),
                    stringsAsFactors = FALSE)
  write_vtr(df1, f1)
  write_vtr(df2, f2)

  d <- diff_vtr(f1, f2, "id")

  expect_true(is.list(d))
  expect_true(inherits(d$added, "vectra_node"))
  expect_true(is.vector(d$deleted))

  # IDs 1 and 2 were deleted
  expect_equal(sort(d$deleted), c(1, 2))
  # IDs 6 and 7 were added
  added_df <- collect(d$added)
  expect_equal(sort(added_df$id), c(6, 7))
  # All columns from B are present
  expect_true(all(c("id", "val") %in% names(added_df)))
})

test_that("diff_vtr returns empty added/deleted when files are identical", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  df <- data.frame(id = 1:5)
  write_vtr(df, f1)
  write_vtr(df, f2)

  d <- diff_vtr(f1, f2, "id")
  expect_equal(nrow(collect(d$added)), 0L)
  expect_equal(length(d$deleted), 0L)
})

test_that("diff_vtr errors on missing key column", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(data.frame(a = 1:3), f1)
  write_vtr(data.frame(a = 2:4), f2)

  expect_error(diff_vtr(f1, f2, "no_such_col"), "key_col")
})

test_that("diff_vtr handles all rows added (old is subset)", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(data.frame(id = 1:3), f1)
  write_vtr(data.frame(id = 1:6), f2)

  d <- diff_vtr(f1, f2, "id")
  expect_equal(nrow(collect(d$added)), 3L)
  expect_equal(length(d$deleted), 0L)
})

test_that("diff_vtr handles all rows deleted (new is empty subset)", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(data.frame(id = 1:5), f1)
  write_vtr(data.frame(id = integer(0)), f2)

  d <- diff_vtr(f1, f2, "id")
  expect_equal(length(d$deleted), 5L)
  expect_equal(nrow(collect(d$added)), 0L)
})
