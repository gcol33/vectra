# --- rename ---

test_that("rename changes column names", {
  df <- data.frame(a = 1:3, b = 4:6)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> rename(alpha = a, beta = b) |> collect()
  expect_equal(names(result), c("alpha", "beta"))
  expect_equal(result$alpha, c(1, 2, 3))
  expect_equal(result$beta, c(4, 5, 6))
})

test_that("rename errors on missing column", {
  df <- data.frame(a = 1:3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  expect_error(tbl(f) |> rename(x = nonexistent))
})

# --- relocate ---

test_that("relocate moves columns to front by default", {
  df <- data.frame(a = 1, b = 2, c = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> relocate(c) |> collect()
  expect_equal(names(result), c("c", "a", "b"))
})

test_that("relocate with .before", {
  df <- data.frame(a = 1, b = 2, c = 3, d = 4)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> relocate(d, .before = b) |> collect()
  expect_equal(names(result), c("a", "d", "b", "c"))
})

test_that("relocate with .after", {
  df <- data.frame(a = 1, b = 2, c = 3, d = 4)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> relocate(a, .after = c) |> collect()
  expect_equal(names(result), c("b", "c", "a", "d"))
})

test_that("relocate with .after last column", {
  df <- data.frame(a = 1, b = 2, c = 3)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> relocate(a, .after = c) |> collect()
  expect_equal(names(result), c("b", "c", "a"))
})

# --- transmute ---

test_that("transmute keeps only computed columns", {
  df <- data.frame(x = c(1.0, 2.0, 3.0), y = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> transmute(total = x + y) |> collect()
  expect_equal(names(result), "total")
  expect_equal(result$total, c(11, 22, 33))
})

# --- distinct ---

test_that("distinct removes duplicate rows", {
  df <- data.frame(a = c("x", "x", "y", "y"), b = c(1.0, 1.0, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> distinct() |> collect()
  expect_equal(nrow(result), 3)
})

test_that("distinct on specific columns", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> distinct(g) |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(sort(result$g), c("a", "b"))
})

# --- ungroup ---

test_that("ungroup removes grouping", {
  df <- data.frame(g = c("a", "b"), x = 1:2)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  node <- tbl(f) |> group_by(g)
  expect_equal(node$.groups, "g")
  node2 <- ungroup(node)
  expect_null(node2$.groups)
})

# --- count ---

test_that("count counts by group", {
  df <- data.frame(g = c("a", "a", "b"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> count(g) |> collect()
  expect_equal(result$n[result$g == "a"], 2)
  expect_equal(result$n[result$g == "b"], 1)
})

test_that("count with custom name", {
  df <- data.frame(g = c("a", "a", "b"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> count(g, name = "cnt") |> collect()
  expect_true("cnt" %in% names(result))
})

# --- tally ---

test_that("tally counts rows in grouped data", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> tally() |> collect()
  expect_equal(result$n[result$g == "a"], 2)
  expect_equal(result$n[result$g == "b"], 1)
})

# --- pull ---

test_that("pull extracts a column as vector", {
  df <- data.frame(x = c(1.0, 2.0, 3.0), y = c(4.0, 5.0, 6.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> pull(x)
  expect_equal(result, c(1, 2, 3))
})

test_that("pull with negative index gets last column", {
  df <- data.frame(x = c(1.0, 2.0), y = c(3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> pull(-1)
  expect_equal(result, c(3, 4))
})

# --- head ---

test_that("head returns first n rows", {
  df <- data.frame(x = 1:10 + 0.0)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- head(tbl(f), 3)
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(1, 2, 3))
})

# --- arrange ---

test_that("arrange sorts ascending by default", {
  df <- data.frame(x = c(3.0, 1.0, 2.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> arrange(x) |> collect()
  expect_equal(result$x, c(1, 2, 3))
})

test_that("arrange desc() sorts descending", {
  df <- data.frame(x = c(3.0, 1.0, 2.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> arrange(desc(x)) |> collect()
  expect_equal(result$x, c(3, 2, 1))
})

test_that("arrange by multiple columns", {
  df <- data.frame(g = c("a", "b", "a", "b"),
                   x = c(2.0, 1.0, 1.0, 2.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> arrange(g, x) |> collect()
  expect_equal(result$g, c("a", "a", "b", "b"))
  expect_equal(result$x, c(1, 2, 1, 2))
})

test_that("arrange puts NA last in ascending order", {
  df <- data.frame(x = c(2.0, NA, 1.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> arrange(x) |> collect()
  expect_equal(result$x[1:3], c(1, 2, 3))
  expect_true(is.na(result$x[4]))
})

test_that("arrange with strings", {
  df <- data.frame(s = c("banana", "apple", "cherry"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> arrange(s) |> collect()
  expect_equal(result$s, c("apple", "banana", "cherry"))
})

# --- slice_head / slice_tail ---

test_that("slice_head returns first n rows", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0, 50.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_head(n = 3) |> collect()
  expect_equal(result$x, c(10, 20, 30))
})

test_that("slice_tail returns last n rows", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0, 50.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_tail(n = 2)
  expect_equal(result$x, c(40, 50))
})

# --- slice_min / slice_max ---

test_that("slice_min returns rows with smallest values", {
  df <- data.frame(x = c(5.0, 1.0, 3.0, 2.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 2) |> collect()
  expect_equal(result$x, c(1, 2))
})

test_that("slice_max returns rows with largest values", {
  df <- data.frame(x = c(5.0, 1.0, 3.0, 2.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_max(order_by = x, n = 2) |> collect()
  expect_equal(result$x, c(5, 4))
})

# --- .groups argument in summarise ---

test_that("summarise with .groups = 'drop' drops all groups", {
  df <- data.frame(g1 = c("a", "a", "b"), g2 = c("x", "y", "x"),
                   v = c(1.0, 2.0, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g1, g2) |>
    summarise(s = sum(v), .groups = "drop")
  expect_null(result$.groups)
})

test_that("summarise with .groups = 'keep' preserves groups", {
  df <- data.frame(g1 = c("a", "a", "b"), g2 = c("x", "y", "x"),
                   v = c(1.0, 2.0, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g1, g2) |>
    summarise(s = sum(v), .groups = "keep")
  expect_equal(result$.groups, c("g1", "g2"))
})

test_that("summarise default .groups = 'drop_last' drops last group", {
  df <- data.frame(g1 = c("a", "a", "b"), g2 = c("x", "y", "x"),
                   v = c(1.0, 2.0, 3.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g1, g2) |> summarise(s = sum(v))
  expect_equal(result$.groups, "g1")
})
