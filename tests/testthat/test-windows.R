# --- lag ---

test_that("lag shifts values down by 1", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(prev = lag(x)) |> collect()
  expect_true(is.na(result$prev[1]))
  expect_equal(result$prev[2:4], c(10, 20, 30))
})

test_that("lag with n = 2", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(prev2 = lag(x, 2)) |> collect()
  expect_true(is.na(result$prev2[1]))
  expect_true(is.na(result$prev2[2]))
  expect_equal(result$prev2[3:5], c(1, 2, 3))
})

test_that("lag with default value", {
  df <- data.frame(x = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(prev = lag(x, 1, default = 0)) |> collect()
  expect_equal(result$prev, c(0, 10, 20))
})

# --- lead ---

test_that("lead shifts values up by 1", {
  df <- data.frame(x = c(10.0, 20.0, 30.0, 40.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(nxt = lead(x)) |> collect()
  expect_equal(result$nxt[1:3], c(20, 30, 40))
  expect_true(is.na(result$nxt[4]))
})

test_that("lead with default value", {
  df <- data.frame(x = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(nxt = lead(x, 1, default = 99)) |> collect()
  expect_equal(result$nxt, c(20, 30, 99))
})

# --- row_number ---

test_that("row_number assigns sequential integers", {
  df <- data.frame(x = c(5.0, 3.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(rn = row_number()) |> collect()
  expect_equal(result$rn, c(1, 2, 3))
})

# --- cumsum ---

test_that("cumsum computes cumulative sum", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs, c(1, 3, 6, 10))
})

test_that("cumsum propagates NA", {
  df <- data.frame(x = c(1.0, NA, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs[1], 1)
  expect_true(is.na(result$cs[2]))
  expect_true(is.na(result$cs[3]))
})

# --- cummean ---

test_that("cummean computes cumulative mean", {
  df <- data.frame(x = c(2.0, 4.0, 6.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cm = cummean(x)) |> collect()
  expect_equal(result$cm, c(2, 3, 4))
})

# --- cummin / cummax ---

test_that("cummin computes running minimum", {
  df <- data.frame(x = c(5.0, 3.0, 4.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cmi = cummin(x)) |> collect()
  expect_equal(result$cmi, c(5, 3, 3, 1))
})

test_that("cummax computes running maximum", {
  df <- data.frame(x = c(1.0, 4.0, 2.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cmx = cummax(x)) |> collect()
  expect_equal(result$cmx, c(1, 4, 4, 5))
})

# --- grouped windows ---

test_that("lag works within groups", {
  df <- data.frame(
    g = c("a", "a", "a", "b", "b"),
    x = c(1.0, 2.0, 3.0, 10.0, 20.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(prev = lag(x)) |> collect()
  # Group a: NA, 1, 2; Group b: NA, 10
  expect_true(is.na(result$prev[1]))
  expect_equal(result$prev[2], 1)
  expect_equal(result$prev[3], 2)
  expect_true(is.na(result$prev[4]))
  expect_equal(result$prev[5], 10)
})

test_that("row_number resets within groups", {
  df <- data.frame(
    g = c("a", "a", "b", "b", "b"),
    x = c(1.0, 2.0, 3.0, 4.0, 5.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(rn = row_number()) |> collect()
  expect_equal(result$rn, c(1, 2, 1, 2, 3))
})

test_that("cumsum resets within groups", {
  df <- data.frame(
    g = c("a", "a", "b", "b"),
    x = c(1.0, 2.0, 10.0, 20.0),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(cs = cumsum(x)) |> collect()
  expect_equal(result$cs, c(1, 3, 10, 30))
})

# --- mixed window + regular mutate ---

test_that("window and regular mutate in same call", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(cs = cumsum(x), doubled = x * 2) |> collect()
  expect_equal(result$cs, c(1, 3, 6))
  expect_equal(result$doubled, c(2, 4, 6))
})

# --- rank ---

test_that("rank assigns min rank with gaps for ties", {
  df <- data.frame(x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(r = rank(x)) |> collect()
  expect_equal(result$r, c(4, 1, 4, 3, 1))
})

test_that("rank works with groups", {
  df <- data.frame(g = c("a", "a", "a", "b", "b"),
                   x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(r = rank(x)) |> collect()
  expect_equal(result$r[result$g == "a"], c(2, 1, 2))
  expect_equal(result$r[result$g == "b"], c(2, 1))
})

# --- dense_rank ---

test_that("dense_rank assigns consecutive ranks without gaps", {
  df <- data.frame(x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(dr = dense_rank(x)) |> collect()
  expect_equal(result$dr, c(3, 1, 3, 2, 1))
})

test_that("dense_rank works with groups", {
  df <- data.frame(g = c("a", "a", "a", "b", "b"),
                   x = c(3.0, 1.0, 3.0, 2.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> mutate(dr = dense_rank(x)) |> collect()
  expect_equal(result$dr[result$g == "a"], c(2, 1, 2))
  expect_equal(result$dr[result$g == "b"], c(2, 1))
})
