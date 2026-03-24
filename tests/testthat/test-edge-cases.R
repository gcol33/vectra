# --- empty filter results ---

test_that("filter producing zero rows returns empty data.frame", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 100) |> collect()
  expect_equal(nrow(result), 0)
  expect_equal(names(result), "x")
})

# --- group_by on zero-row input ---

test_that("summarise on filtered-to-empty input works", {
  df <- data.frame(g = c("a", "b"), x = c(1.0, 2.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 100) |> group_by(g) |> summarise(s = sum(x)) |> collect()
  expect_equal(nrow(result), 0)
})

# --- join with empty right table ---

test_that("left_join with empty right table preserves all left rows", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
  write_vtr(data.frame(id = c(99.0), y = c(999.0)), f2)
  result <- left_join(tbl(f1), tbl(f2) |> filter(id > 1000), by = "id") |> collect()
  expect_equal(nrow(result), 3)
  expect_true(all(is.na(result$y)))
})

test_that("inner_join with empty right table returns zero rows", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1, 2), x = c(10, 20)), f1)
  write_vtr(data.frame(id = c(99.0), y = c(999.0)), f2)
  result <- inner_join(tbl(f1), tbl(f2) |> filter(id > 1000), by = "id") |> collect()
  expect_equal(nrow(result), 0)
})

# --- distinct with .keep_all fallback ---

test_that("distinct with .keep_all = TRUE and column subset falls back to R", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  expect_message(
    result <- tbl(f) |> distinct(g, .keep_all = TRUE),
    "falling back"
  )
  expect_equal(nrow(result), 2)
  expect_true("x" %in% names(result))
})

# --- NA handling in window functions ---

test_that("lead with NA values propagates NAs correctly", {
  df <- data.frame(x = c(1.0, NA, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(nxt = lead(x)) |> collect()
  expect_true(is.na(result$nxt[1]))   # lead shifts up: value at position 2 is NA
  expect_equal(result$nxt[2], 3)      # value at position 3
  expect_equal(result$nxt[3], 4)      # value at position 4
  expect_true(is.na(result$nxt[4]))   # last row has no next value
})

# --- bind_rows with different schemas ---

test_that("bind_rows with different columns fills missing with NA", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(a = c(1.0, 2.0), b = c(3.0, 4.0)), f1)
  write_vtr(data.frame(b = c(5.0, 6.0), c = c(7.0, 8.0)), f2)
  result <- bind_rows(tbl(f1), tbl(f2))
  expect_equal(nrow(result), 4)
  expect_true(all(c("a", "b", "c") %in% names(result)))
  expect_true(is.na(result$a[3]))
  expect_true(is.na(result$c[1]))
})

# --- empty input to bind_rows ---

test_that("bind_rows with single input returns that input", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(1.0, 2.0)), f)
  result <- bind_rows(tbl(f))
  expect_equal(nrow(result), 2)
})

# --- reframe ---

test_that("reframe returns variable-length output", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> reframe(range_x = range(x))
  # group a: range(1,2) = c(1,2) -> 2 rows; group b: range(3) = c(3,3) -> 2 rows
  expect_equal(nrow(result), 4)
})

# --- across in summarise ---

test_that("across in summarise applies function to multiple columns", {
  df <- data.frame(x = c(1.0, 2.0, 3.0), y = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> summarise(across(c(x, y), mean)) |> collect()
  expect_equal(result$x, 2)
  expect_equal(result$y, 20)
})

# --- string edge cases in filter ---

test_that("filter with empty string comparison works", {
  df <- data.frame(s = c("", "a", "b"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(s == "") |> collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$s, "")
})

test_that("grepl with fixed string match works", {
  df <- data.frame(s = c("hello world", "foo", "hello there"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(grepl("hello", s)) |> collect()
  expect_equal(nrow(result), 2)
})
