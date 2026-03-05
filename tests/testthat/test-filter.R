test_that("filter with simple comparison", {
  df <- data.frame(x = c(1L, 2L, 3L, 4L, 5L), y = c(10.0, 20.0, 30.0, 40.0, 50.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 3) |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(4, 5))
  expect_equal(result$y, c(40, 50))
})

test_that("filter with equality", {
  df <- data.frame(x = c(1L, 2L, 3L), s = c("a", "b", "c"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x == 2) |> collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$s, "b")
})

test_that("filter with AND (multiple args)", {
  df <- data.frame(x = 1:10, y = rep(c(TRUE, FALSE), 5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 3, x < 8) |> collect()
  expect_equal(result$x, c(4, 5, 6, 7))
})

test_that("filter with boolean operators", {
  df <- data.frame(x = 1:6)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x < 3 | x > 4) |> collect()
  expect_equal(result$x, c(1, 2, 5, 6))
})

test_that("filter with NOT", {
  df <- data.frame(x = c(TRUE, FALSE, TRUE, FALSE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(!x) |> collect()
  expect_equal(result$x, c(FALSE, FALSE))
})

test_that("filter with is.na", {
  df <- data.frame(x = c(1L, NA, 3L, NA, 5L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(!is.na(x)) |> collect()
  expect_equal(result$x, c(1, 3, 5))
})

test_that("filter with NAs in predicate (treated as FALSE)", {
  df <- data.frame(x = c(1L, NA, 3L), y = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x > 1) |> collect()
  # NA > 1 is NA, treated as FALSE
  expect_equal(nrow(result), 1)
  expect_equal(result$x, 3)
})

# ---- Selection vector tests ----

test_that("filter -> collect with selection vector", {
  df <- data.frame(
    x = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    y = c("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"),
    z = c(TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, TRUE, FALSE),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 5) |> collect()
  expect_equal(nrow(result), 5)
  expect_equal(result$x, c(6, 7, 8, 9, 10))
  expect_equal(result$y, c("f", "g", "h", "i", "j"))
  expect_equal(result$z, c(FALSE, TRUE, FALSE, TRUE, FALSE))
})

test_that("double filter composes selection vectors", {
  df <- data.frame(x = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 3) |> filter(x < 8) |> collect()
  expect_equal(result$x, c(4, 5, 6, 7))
})

test_that("filter -> select with selection vector", {
  df <- data.frame(
    x = c(1, 2, 3, 4, 5),
    y = c(10, 20, 30, 40, 50),
    z = c("a", "b", "c", "d", "e"),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x >= 3) |> select(x, z) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(3, 4, 5))
  expect_equal(result$z, c("c", "d", "e"))
  expect_equal(ncol(result), 2)
})

test_that("filter -> mutate with selection vector", {
  df <- data.frame(x = c(1, 2, 3, 4, 5), y = c(10, 20, 30, 40, 50))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 2) |> mutate(z = x + y) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(3, 4, 5))
  expect_equal(result$z, c(33, 44, 55))
})

test_that("filter -> inner_join with selection vector on probe side", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(data.frame(id = c(1, 2, 3, 4, 5), x = c(10, 20, 30, 40, 50)), f1)
  write_vtr(data.frame(id = c(2, 4, 6), y = c(200, 400, 600)), f2)

  result <- tbl(f1) |> filter(id > 1) |>
    inner_join(tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$id, c(2, 4))
  expect_equal(result$x, c(20, 40))
  expect_equal(result$y, c(200, 400))
})

test_that("filter -> left_join with selection vector", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(data.frame(id = c(1, 2, 3, 4, 5), x = c(10, 20, 30, 40, 50)), f1)
  write_vtr(data.frame(id = c(2, 4), y = c(200, 400)), f2)

  result <- tbl(f1) |> filter(id >= 2, id <= 4) |>
    left_join(tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 3)
  # Sort by id for comparison (hash join order: matched first, then unmatched)
  result <- result[order(result$id), ]
  expect_equal(result$id, c(2, 3, 4))
  expect_equal(result$x, c(20, 30, 40))
  expect_equal(result$y, c(200, NA, 400))
})

test_that("empty filter result propagates correctly", {
  df <- data.frame(x = c(1, 2, 3))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 100) |> collect()
  expect_equal(nrow(result), 0)
})

# ---- Selection vector v1.5 tests (ProjectNode gather) ----

test_that("filter -> select on wide table gathers only needed cols", {
  # Wide table: 20 columns, selective filter, narrow select
  cols <- lapply(1:20, function(i) seq(1, 100))
  names(cols) <- paste0("col", 1:20)
  df <- as.data.frame(cols)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(col1 > 95) |> select(col1, col20) |> collect()
  expect_equal(nrow(result), 5)
  expect_equal(ncol(result), 2)
  expect_equal(result$col1, 96:100)
  expect_equal(result$col20, 96:100)
})

test_that("filter -> mutate with expr referencing one col", {
  df <- data.frame(a = c(1, 2, 3, 4, 5), b = c(10, 20, 30, 40, 50))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(a > 2) |> mutate(c = a * 2) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$a, c(3, 4, 5))
  expect_equal(result$b, c(30, 40, 50))
  expect_equal(result$c, c(6, 8, 10))
})

test_that("sequential mutate with selection: a2 = a+1, a3 = a2+1", {
  df <- data.frame(a = c(1, 2, 3, 4, 5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(a > 2) |>
    mutate(a2 = a + 1, a3 = a2 + 1) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$a, c(3, 4, 5))
  expect_equal(result$a2, c(4, 5, 6))
  expect_equal(result$a3, c(5, 6, 7))
})

test_that("double filter then mutate", {
  df <- data.frame(x = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 3) |> filter(x < 8) |>
    mutate(y = x * 10) |> collect()
  expect_equal(result$x, c(4, 5, 6, 7))
  expect_equal(result$y, c(40, 50, 60, 70))
})

test_that("filter -> mutate -> left_join correctness", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))

  write_vtr(data.frame(id = c(1, 2, 3, 4, 5), x = c(10, 20, 30, 40, 50)), f1)
  write_vtr(data.frame(id = c(2, 4, 6), y = c(200, 400, 600)), f2)

  result <- tbl(f1) |> filter(id > 1) |> mutate(x2 = x * 2) |>
    left_join(tbl(f2), by = "id") |> collect()
  result <- result[order(result$id), ]
  expect_equal(result$id, c(2, 3, 4, 5))
  expect_equal(result$x, c(20, 30, 40, 50))
  expect_equal(result$x2, c(40, 60, 80, 100))
  expect_equal(result$y, c(200, NA, 400, NA))
})

test_that("filter -> group_by -> summarise with selection vector (no compact)", {
  df <- data.frame(
    g = c("a", "a", "b", "b", "c", "c"),
    x = c(1, 2, 3, 4, 5, 6),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 2) |>
    group_by(g) |> summarise(total = sum(x)) |> collect()
  # After filter: b(3,4), c(5,6)
  expect_equal(nrow(result), 2)
  expect_true("b" %in% result$g)
  expect_true("c" %in% result$g)
  expect_equal(result$total[result$g == "b"], 7)
  expect_equal(result$total[result$g == "c"], 11)
})

test_that("filter -> group_by with multiple aggs and sel", {
  df <- data.frame(
    g = c("x", "x", "x", "y", "y", "y", "z", "z", "z"),
    val = c(1, 2, 3, 4, 5, 6, 7, 8, 9)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(val > 1, val < 9) |>
    group_by(g) |>
    summarise(n = n(), s = sum(val), m = mean(val)) |> collect()
  result <- result[order(result$g), ]
  expect_equal(result$g, c("x", "y", "z"))
  expect_equal(result$n, c(2, 3, 2))
  expect_equal(result$s, c(5, 15, 15))
  expect_equal(result$m, c(2.5, 5, 7.5))
})

test_that("filter -> arrange with selection vector (no compact)", {
  df <- data.frame(x = c(5, 3, 1, 4, 2, 8, 7, 6))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 3) |> arrange(x) |> collect()
  expect_equal(result$x, c(4, 5, 6, 7, 8))
})

test_that("filter -> slice_head with selection vector (no compact)", {
  df <- data.frame(x = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 3) |> slice_head(n = 3) |> collect()
  expect_equal(nrow(result), 3)
  # After filter: 4,5,6,7,8,9,10 -> first 3 = 4,5,6
  expect_equal(result$x, c(4, 5, 6))
})

test_that("filter -> window function with selection vector", {
  df <- data.frame(
    g = c("a", "a", "a", "b", "b", "b"),
    x = c(10, 20, 30, 40, 50, 60)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |> filter(x > 10) |>
    group_by(g) |> mutate(rn = row_number()) |> collect()
  result <- result[order(result$g, result$rn), ]
  expect_equal(result$x, c(20, 30, 40, 50, 60))
  expect_equal(result$rn, c(1, 2, 1, 2, 3))
})
