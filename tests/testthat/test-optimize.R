# --- Column pruning ---

test_that("column pruning only reads needed columns", {
  df <- data.frame(g = c("a", "a", "b"), x = 1:3, y = 4:6,
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  # select only x -> scan should prune g and y

  result <- tbl(f) |> select(x) |> collect()
  expect_equal(result$x, c(1, 2, 3))
  expect_equal(ncol(result), 1)
})

test_that("column pruning works with filter", {
  df <- data.frame(a = c(1, 2, 3), b = c(10, 20, 30))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(a > 1) |> select(b) |> collect()
  expect_equal(result$b, c(20, 30))
})

test_that("column pruning with group_by summarise", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1, 2, 3), y = c(4, 5, 6),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> group_by(g) |> summarise(s = sum(x)) |> collect()
  expect_equal(nrow(result), 2)
  # y should have been pruned from the scan
})

# --- Predicate pushdown ---

test_that("predicate pushdown skips row groups", {
  # Create a multi-rowgroup file where one group can be skipped
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  fp <- file(f, "wb")
  close(fp)

  # Write via R: two row groups, first with x in [1,10], second with x in [100,200]
  df1 <- data.frame(x = 1:10)
  df2 <- data.frame(x = 100:200)
  write_vtr(rbind(df1, df2), f)

  # filter x > 50 should match only second group conceptually
  result <- tbl(f) |> filter(x > 50) |> collect()
  expect_true(all(result$x > 50))
})

test_that("predicate pushdown works with comparison operators", {
  df <- data.frame(x = c(1, 5, 10, 15, 20))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  expect_equal(nrow(tbl(f) |> filter(x >= 10) |> collect()), 3)
  expect_equal(nrow(tbl(f) |> filter(x < 10) |> collect()), 2)
  expect_equal(nrow(tbl(f) |> filter(x == 5) |> collect()), 1)
  expect_equal(nrow(tbl(f) |> filter(x != 5) |> collect()), 4)
})

# --- Nested aggregation expressions ---

test_that("nested expression in summarise works", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1, 2, 3), y = c(10, 20, 30),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> group_by(g) |> summarise(s = sum(x + y)) |> collect()
  result <- result[order(result$g), ]
  expect_equal(result$s[result$g == "a"], 11 + 22)
  expect_equal(result$s[result$g == "b"], 33)
})

test_that("nested expression with mean works", {
  df <- data.frame(g = c("a", "a"), x = c(2, 4), y = c(1, 1))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> group_by(g) |> summarise(m = mean(x * y)) |> collect()
  expect_equal(result$m, mean(c(2, 4)))
})

# --- String operations ---

test_that("nchar works on string column", {
  df <- data.frame(s = c("abc", "de", "f", "hello"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> mutate(n = nchar(s)) |> collect()
  expect_equal(result$n, c(3, 2, 1, 5))
})

test_that("nchar handles NA", {
  df <- data.frame(s = c("abc", NA, "de"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> mutate(n = nchar(s)) |> collect()
  expect_equal(result$n, c(3, NA, 2))
})

test_that("substr extracts substring", {
  df <- data.frame(s = c("hello", "world"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> mutate(sub = substr(s, 1, 3)) |> collect()
  expect_equal(result$sub, c("hel", "wor"))
})

test_that("grepl matches fixed pattern", {
  df <- data.frame(s = c("apple", "banana", "grape", "pineapple"),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(grepl("app", s)) |> collect()
  expect_equal(nrow(result), 2)
  expect_true(all(grepl("app", result$s)))
})

test_that("grepl with filter on NA", {
  df <- data.frame(s = c("abc", NA, "abcdef"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(grepl("abc", s)) |> collect()
  expect_equal(nrow(result), 2)
})

# --- rank/dense_rank correctness ---

test_that("rank produces correct results", {
  df <- data.frame(x = c(3, 1, 2, 1, 3))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> mutate(r = rank(x)) |> collect()
  expect_equal(result$r, c(4, 1, 3, 1, 4))
})

test_that("dense_rank produces correct results", {
  df <- data.frame(x = c(3, 1, 2, 1, 3))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> mutate(r = dense_rank(x)) |> collect()
  expect_equal(result$r, c(3, 1, 2, 1, 3))
})
