# Helper to create two tables for join tests
make_join_tables <- function() {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
  list(f1 = f1, f2 = f2)
}

# --- inner_join ---

test_that("inner_join keeps only matching rows", {
  ft <- make_join_tables()
  on.exit(unlink(c(ft$f1, ft$f2)))
  result <- inner_join(tbl(ft$f1), tbl(ft$f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(sort(result$id), c(1, 2))
  expect_equal(result$x[result$id == 1], 10)
  expect_equal(result$y[result$id == 1], 100)
})

# --- left_join ---

test_that("left_join keeps all left rows, NAs for unmatched", {
  ft <- make_join_tables()
  on.exit(unlink(c(ft$f1, ft$f2)))
  result <- left_join(tbl(ft$f1), tbl(ft$f2), by = "id") |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$y[result$id == 1], 100)
  expect_equal(result$y[result$id == 2], 200)
  expect_true(is.na(result$y[result$id == 3]))
})

# --- semi_join ---

test_that("semi_join keeps left rows with matches, no right cols", {
  ft <- make_join_tables()
  on.exit(unlink(c(ft$f1, ft$f2)))
  result <- semi_join(tbl(ft$f1), tbl(ft$f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(sort(result$id), c(1, 2))
  expect_equal(names(result), c("id", "x"))
})

# --- anti_join ---

test_that("anti_join keeps left rows without matches", {
  ft <- make_join_tables()
  on.exit(unlink(c(ft$f1, ft$f2)))
  result <- anti_join(tbl(ft$f1), tbl(ft$f2), by = "id") |> collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$id, 3)
  expect_equal(result$x, 30)
})

# --- named by (different column names) ---

test_that("join with named by works", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(a = c(1, 2, 3), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(b = c(1, 2, 4), y = c(100, 200, 400)), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = c("a" = "b")) |> collect()
  expect_equal(nrow(result), 2)
  expect_true("x" %in% names(result))
  expect_true("y" %in% names(result))
})

# --- natural join (no by) ---

test_that("natural join finds common columns", {
  ft <- make_join_tables()
  on.exit(unlink(c(ft$f1, ft$f2)))
  expect_message(
    result <- inner_join(tbl(ft$f1), tbl(ft$f2)) |> collect(),
    "Joining by"
  )
  expect_equal(nrow(result), 2)
})

# --- duplicate handling ---

test_that("inner_join with duplicates on right produces multiple rows", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, 2), x = c(10, 20)), f1)
  write_vtr(data.frame(id = c(1, 1, 2), y = c(100, 101, 200)), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(sum(result$id == 1), 2)
})

# --- NA key handling ---

test_that("NAs in join keys do not match", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, NA, 3), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(1, NA, 4), y = c(100, 200, 400)), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$id, 1)
})

test_that("left_join with NA keys: unmatched NAs get NA right cols", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, NA), x = c(10, 20)), f1)
  write_vtr(data.frame(id = c(1, NA), y = c(100, 200)), f2)
  result <- left_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$y[which(result$id == 1)], 100)
  expect_true(is.na(result$y[which(is.na(result$id))]))
})

# --- suffix for name collisions ---

test_that("suffix disambiguates columns with same name", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, 2), val = c(10, 20)), f1)
  write_vtr(data.frame(id = c(1, 2), val = c(100, 200)), f2)
  result <- left_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_true("val.x" %in% names(result))
  expect_true("val.y" %in% names(result))
})

# --- string keys ---

test_that("join works with string keys", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(name = c("alice", "bob"), x = c(1, 2),
                       stringsAsFactors = FALSE), f1)
  write_vtr(data.frame(name = c("bob", "carol"), y = c(20, 30),
                       stringsAsFactors = FALSE), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "name") |> collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$name, "bob")
  expect_equal(result$y, 20)
})

# --- composite keys ---

test_that("join works with composite keys", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(a = c(1, 1, 2), b = c("x", "y", "x"),
                       v1 = c(10, 20, 30), stringsAsFactors = FALSE), f1)
  write_vtr(data.frame(a = c(1, 2), b = c("x", "x"),
                       v2 = c(100, 300), stringsAsFactors = FALSE), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = c("a", "b")) |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(sort(result$v1), c(10, 30))
})

# --- join after filter ---

test_that("string-key join survives many unique keys", {
  # 200 unique string keys forces hash table to work hard.
  # Regression test for join key lifetime / string data integrity.
  set.seed(42)
  n_keys <- 200
  keys <- paste0("key_", seq_len(n_keys))
  left_df <- data.frame(
    id = rep(keys, each = 3),
    x = rnorm(n_keys * 3),
    stringsAsFactors = FALSE
  )
  right_df <- data.frame(
    id = keys,
    y = rnorm(n_keys),
    stringsAsFactors = FALSE
  )
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(left_df, f1)
  write_vtr(right_df, f2)

  # left_join: all left rows should have a match
  result <- tbl(f1) |> left_join(tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), nrow(left_df))
  expect_false(any(is.na(result$y)))

  # full_join with partial overlap
  partial_right <- data.frame(
    id = keys[1:100],
    z = rnorm(100),
    stringsAsFactors = FALSE
  )
  f3 <- tempfile(fileext = ".vtr")
  on.exit(unlink(f3), add = TRUE)
  write_vtr(partial_right, f3)
  result2 <- tbl(f1) |> full_join(tbl(f3), by = "id") |> collect()
  # All left rows present, no extra right-only rows (right keys are subset)
  expect_equal(nrow(result2), nrow(left_df))
  # Keys 101-200 have NA z
  unmatched <- result2[result2$id %in% keys[101:200], ]
  expect_true(all(is.na(unmatched$z)))

  # Chain a downstream op on the result to verify string integrity
  f4 <- tempfile(fileext = ".vtr")
  on.exit(unlink(f4), add = TRUE)
  write_vtr(result, f4)
  roundtrip <- tbl(f4) |> filter(y > 0) |> collect()
  expect_true(all(roundtrip$y > 0))
  expect_true(nrow(roundtrip) > 0)
})

# --- key type mismatch ---

test_that("join auto-coerces compatible key types (int64 vs double)", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # integer keys on left, double keys on right -> should auto-coerce
  write_vtr(data.frame(id = c(1L, 2L, 3L), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(sort(result$id), c(1, 2))
})

test_that("join errors on key type mismatch (string vs double)", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c("a", "b"), x = c(10, 20),
                       stringsAsFactors = FALSE), f1)
  write_vtr(data.frame(id = c(1, 2), y = c(100, 200)), f2)
  expect_error(
    left_join(tbl(f1), tbl(f2), by = "id") |> collect(),
    "left_join key type mismatch: x\\.id \\(string\\) vs y\\.id \\(double\\)"
  )
})

test_that("composite key auto-coerces mixed numeric types", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # First key (a) matches (both double), second key (b) int vs double
  write_vtr(data.frame(a = c(1, 2), b = c(10L, 20L), x = c(1, 2)), f1)
  write_vtr(data.frame(a = c(1, 2), b = c(10, 20), y = c(3, 4)), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = c("a", "b")) |> collect()
  expect_equal(nrow(result), 2)
})

test_that("join works when both sides have same numeric type", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # Both double
  write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  # Both integer
  write_vtr(data.frame(id = c(1L, 2L, 3L), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(1L, 2L, 4L), y = c(100, 200, 400)), f2)
  result2 <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result2), 2)
})

# --- long string keys ---

test_that("join handles very long string keys", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # Keys with 100k+ bytes each
  long_a <- paste(rep("a", 100000), collapse = "")
  long_b <- paste(rep("b", 100000), collapse = "")
  long_c <- paste(rep("c", 100000), collapse = "")
  write_vtr(data.frame(key = c(long_a, long_b, long_c),
                       x = c(1, 2, 3), stringsAsFactors = FALSE), f1)
  write_vtr(data.frame(key = c(long_b, long_c, "short"),
                       y = c(20, 30, 99), stringsAsFactors = FALSE), f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "key") |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$x[result$key == long_b], 2)
  expect_equal(result$y[result$key == long_c], 30)
})

test_that("join handles many distinct long string keys", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  n <- 500
  # Each key is ~1000 chars with a unique suffix
  base <- paste(rep("x", 1000), collapse = "")
  keys <- paste0(base, "_", seq_len(n))
  write_vtr(data.frame(key = keys, x = seq_len(n),
                       stringsAsFactors = FALSE), f1)
  write_vtr(data.frame(key = keys[1:250], y = seq_len(250),
                       stringsAsFactors = FALSE), f2)
  result <- left_join(tbl(f1), tbl(f2), by = "key") |> collect()
  expect_equal(nrow(result), n)
  expect_equal(sum(!is.na(result$y)), 250)
  expect_equal(sum(is.na(result$y)), 250)
})

# --- join after filter ---

# --- streaming probe tests (multi-batch left side) ---

test_that("streaming inner_join across multiple probe batches", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # 100 left rows in 10 batches of 10 rows each (all integer)
  left_df <- data.frame(id = rep(1L:10L, each = 10L),
                        x = seq_len(100L))
  right_df <- data.frame(id = c(1L, 5L, 10L), y = c(10L, 50L, 100L))
  write_vtr(left_df, f1, batch_size = 10L)
  write_vtr(right_df, f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 30)  # 3 matching ids * 10 rows each
  expect_equal(sort(unique(result$id)), c(1, 5, 10))
})

test_that("streaming left_join with unmatched rows across batches", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  left_df <- data.frame(id = 1L:50L, x = seq_len(50L))
  right_df <- data.frame(id = c(1L, 25L, 50L), y = c(100L, 250L, 500L))
  write_vtr(left_df, f1, batch_size = 10L)  # 5 batches
  write_vtr(right_df, f2)
  result <- left_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 50)
  expect_equal(sum(!is.na(result$y)), 3)
  expect_equal(sum(is.na(result$y)), 47)
  expect_equal(result$y[result$id == 25], 250)
})

test_that("streaming semi_join across multiple batches", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  left_df <- data.frame(id = 1L:30L, x = seq_len(30L))
  right_df <- data.frame(id = c(5L, 15L, 25L), y = c(1L, 2L, 3L))
  write_vtr(left_df, f1, batch_size = 10L)  # 3 batches
  write_vtr(right_df, f2)
  result <- semi_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(sort(result$id), c(5, 15, 25))
  expect_equal(names(result), c("id", "x"))
})

test_that("streaming anti_join across batches where some batches produce no output", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # Batch 1 (ids 1-10): all matched -> 0 output rows
  # Batch 2 (ids 11-20): none matched -> 10 output rows
  left_df <- data.frame(id = 1L:20L, x = seq_len(20L))
  right_df <- data.frame(id = 1L:10L, y = seq_len(10L))
  write_vtr(left_df, f1, batch_size = 10L)
  write_vtr(right_df, f2)
  result <- anti_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 10)
  expect_equal(sort(result$id), 11:20)
})

test_that("streaming full_join emits unmatched build rows after probe", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  left_df <- data.frame(id = 1L:20L, x = seq_len(20L))
  right_df <- data.frame(id = c(5L, 15L, 99L, 100L),
                         y = c(50L, 150L, 990L, 1000L))
  write_vtr(left_df, f1, batch_size = 10L)
  write_vtr(right_df, f2)
  result <- full_join(tbl(f1), tbl(f2), by = "id") |> collect()
  # 20 left rows + 2 unmatched right rows (99, 100)
  expect_equal(nrow(result), 22)
  # Unmatched right rows have NA x
  unmatched <- result[result$id %in% c(99, 100), ]
  expect_equal(nrow(unmatched), 2)
  expect_true(all(is.na(unmatched$x)))
  expect_equal(sort(unmatched$y), c(990, 1000))
})

test_that("streaming many-to-many join works across batches", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  # 30 left rows, 3 batches of 10 (all integer)
  # Right side has 5 rows per key for keys 1-3
  left_df <- data.frame(id = rep(1L:3L, each = 10L),
                        x = seq_len(30L))
  right_df <- data.frame(id = rep(1L:3L, each = 5L),
                         y = seq_len(15L))
  write_vtr(left_df, f1, batch_size = 10L)
  write_vtr(right_df, f2)
  result <- inner_join(tbl(f1), tbl(f2), by = "id") |> collect()
  # Each of 30 left rows matches 5 right rows -> 150
  expect_equal(nrow(result), 150)
})

# --- join after filter ---

test_that("join composes with filter", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(1, 2, 3), y = c(100, 200, 300)), f2)
  result <- tbl(f1) |>
    filter(x > 10) |>
    inner_join(tbl(f2), by = "id") |>
    collect()
  expect_equal(nrow(result), 2)
  expect_true(all(result$x > 10))
})
