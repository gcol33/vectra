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

test_that("append_vtr rejects a column-count mismatch", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(a = 1:3, b = 1:3), f)
  expect_error(append_vtr(data.frame(a = 4:6), f), "count mismatch")
})

test_that("append_vtr rejects a column-type mismatch", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(a = 1:3), f)
  expect_error(append_vtr(data.frame(a = c("x", "y", "z")), f),
               "type mismatch")
})

# ── append_vtr(along = "cols") ────────────────────────────────────────────────

# Helper: the bytes of a .vtr below the 64-byte container header. A column
# append must leave every one of them exactly where it found them -- that is
# the property that makes the operation cost the appended columns rather
# than the size of the store.
vtr_body <- function(path) {
  n <- file.size(path)
  raw_all <- readBin(path, "raw", n = n)
  raw_all[65:n]
}

test_that("append_vtr(along = 'cols') attaches columns without touching existing bytes", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  set.seed(42)
  n <- 500L
  base <- data.frame(id = 1:n,
                     v = runif(n),
                     s = sample(letters, n, TRUE),
                     stringsAsFactors = FALSE)
  write_vtr(base, f, batch_size = 64L)   # 8 row groups

  body_before <- vtr_body(f)

  extra <- data.frame(w = rnorm(n),
                      lab = paste0("r", seq_len(n)),
                      flag = rep(c(TRUE, FALSE), length.out = n),
                      stringsAsFactors = FALSE)
  append_vtr(extra, f, along = "cols")

  # The original body is a strict prefix of the widened one.
  body_after <- vtr_body(f)
  expect_gt(length(body_after), length(body_before))
  expect_identical(body_after[seq_along(body_before)], body_before)

  got <- tbl(f) |> collect()
  expect_identical(names(got), c("id", "v", "s", "w", "lab", "flag"))
  expect_equal(nrow(got), n)
  # Pre-existing columns are byte-for-byte the same data.
  expect_equal(got$id, as.double(1:n))
  expect_equal(got$v, base$v)
  expect_identical(got$s, base$s)
  # Appended columns land against the right rows.
  expect_equal(got$w, extra$w)
  expect_identical(got$lab, extra$lab)
  expect_identical(got$flag, extra$flag)
})

test_that("append_vtr(along = 'cols') round-trips NAs in every appended type", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  base <- data.frame(id = 1:6)
  write_vtr(base, f, batch_size = 4L)   # boundary falls mid-column

  extra <- data.frame(
    d = c(1.5, NA, 3.5, NA, 5.5, 6.5),
    i = c(1L, 2L, NA, 4L, 5L, NA),
    s = c("a", NA, "c", "d", NA, "f"),
    b = c(TRUE, NA, FALSE, TRUE, NA, FALSE),
    stringsAsFactors = FALSE
  )
  append_vtr(extra, f, along = "cols")

  got <- tbl(f) |> collect()
  expect_equal(got$d, extra$d)
  expect_equal(got$i, as.double(extra$i))
  expect_identical(got$s, extra$s)
  expect_identical(got$b, extra$b)
})

test_that("append_vtr(along = 'cols') works when row groups do not divide the input batches", {
  f <- tempfile(fileext = ".vtr")
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, g)))

  n <- 250L
  write_vtr(data.frame(id = 1:n), f, batch_size = 37L)  # ragged final group
  # Source the new column from a lazy node whose own batching (100) lines up
  # with neither 37 nor the total.
  write_vtr(data.frame(w = as.double(n:1)), g, batch_size = 100L)

  append_vtr(tbl(g), f, along = "cols")

  got <- tbl(f) |> collect()
  expect_equal(got$id, as.double(1:n))
  expect_equal(got$w, as.double(n:1))
})

test_that("append_vtr(along = 'cols') can be repeated", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  n <- 40L
  write_vtr(data.frame(id = 1:n), f, batch_size = 16L)
  for (k in 1:4) {
    col <- data.frame(x = as.double(seq_len(n) * k))
    names(col) <- paste0("v", k)
    append_vtr(col, f, along = "cols")
  }

  got <- tbl(f) |> collect()
  expect_identical(names(got), c("id", "v1", "v2", "v3", "v4"))
  for (k in 1:4)
    expect_equal(got[[paste0("v", k)]], as.double(seq_len(n) * k))
})

test_that("append_vtr(along = 'cols') honours compress and keeps the data identical", {
  n <- 300L
  base <- data.frame(id = 1:n)
  extra <- data.frame(w = as.double(seq_len(n) %% 7))

  out <- lapply(c("fast", "small", "none"), function(cmp) {
    f <- tempfile(fileext = ".vtr")
    on.exit(unlink(f), add = TRUE)
    write_vtr(base, f, batch_size = 50L)
    append_vtr(extra, f, along = "cols", compress = cmp)
    tbl(f) |> collect()
  })
  for (got in out) {
    expect_equal(got$id, as.double(1:n))
    expect_equal(got$w, extra$w)
  }
})

test_that("append_vtr(along = 'cols') rejects a row-count mismatch and leaves the store intact", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  n <- 100L
  write_vtr(data.frame(id = 1:n, v = as.double(1:n)), f, batch_size = 32L)
  before <- tbl(f) |> collect()
  body_before <- vtr_body(f)

  # Too few rows.
  expect_error(append_vtr(data.frame(w = 1:10), f, along = "cols"),
               "row count mismatch")
  expect_identical(tbl(f) |> collect(), before)

  # Too many rows.
  expect_error(append_vtr(data.frame(w = 1:(n + 25L)), f, along = "cols"),
               "row count mismatch")
  expect_identical(tbl(f) |> collect(), before)

  # An aborted append truncates what it wrote, so repeated failures cannot
  # grow the file.
  expect_identical(vtr_body(f), body_before)
})

test_that("append_vtr(along = 'cols') rejects colliding and duplicated names", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(data.frame(id = 1:5, v = as.double(1:5)), f)
  before <- tbl(f) |> collect()

  expect_error(append_vtr(data.frame(v = as.double(6:10)), f, along = "cols"),
               "already exists")
  expect_identical(tbl(f) |> collect(), before)

  df <- data.frame(a = 1:5, b = 1:5)
  names(df) <- c("a", "a")
  expect_error(append_vtr(df, f, along = "cols"), "duplicate|already exists")
  expect_identical(tbl(f) |> collect(), before)
})

test_that("a widened store still supports the verbs, indexes, and further appends", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".vtri"))))

  n <- 200L
  write_vtr(data.frame(id = 1:n, grp = rep(1:4, length.out = n)), f,
            batch_size = 40L)
  append_vtr(data.frame(w = as.double(seq_len(n) * 3)), f, along = "cols")

  # Filter/select over a widened store, including on an appended column.
  expect_equal(nrow(tbl(f) |> filter(id > 195) |> collect()), 5L)
  expect_equal((tbl(f) |> filter(w == 30) |> collect())$id, 10)

  # Grouped aggregation reaching an appended column.
  agg <- tbl(f) |> group_by(grp) |> summarise(total = sum(w)) |> collect()
  expect_equal(nrow(agg), 4L)

  # An index built after widening prunes correctly.
  create_index(f, "id")
  expect_true(has_index(f, "id"))
  expect_equal((tbl(f) |> filter(id == 77) |> collect())$w, 231)

  # Row append onto a widened store: schema is the widened one.
  append_vtr(data.frame(id = n + 1L, grp = 1L, w = 999), f)
  got <- tbl(f) |> collect()
  expect_equal(nrow(got), n + 1L)
  expect_equal(got$w[n + 1L], 999)

  # And the widened store can be rewritten wholesale.
  g <- tempfile(fileext = ".vtr")
  on.exit(unlink(g), add = TRUE)
  tbl(f) |> write_vtr(g)
  expect_identical(tbl(g) |> collect(), got)
})

test_that("append_vtr(along = 'cols') widens a zero-row store", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(data.frame(id = integer(0)), f)
  append_vtr(data.frame(w = double(0)), f, along = "cols")

  got <- tbl(f) |> collect()
  expect_identical(names(got), c("id", "w"))
  expect_equal(nrow(got), 0L)

  # A zero-row store still holds its columns to a row count, so a non-empty
  # append is a mismatch like any other.
  expect_error(append_vtr(data.frame(z = 1:3), f, along = "cols"),
               "row count mismatch")
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
