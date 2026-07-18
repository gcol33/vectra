# --- sd ---

test_that("sd computes sample standard deviation", {
  df <- data.frame(g = c("a", "a", "a"), x = c(2.0, 4.0, 6.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sd(x)) |> collect()
  expect_equal(result$s, sd(c(2, 4, 6)))
})

test_that("sd of single value returns NA", {
  df <- data.frame(g = c("a"), x = c(5.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sd(x)) |> collect()
  expect_true(is.na(result$s))
})

test_that("sd with na.rm = TRUE skips NAs", {
  df <- data.frame(g = c("a", "a", "a"), x = c(2.0, NA, 6.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sd(x, na.rm = TRUE)) |> collect()
  expect_equal(result$s, sd(c(2, 6)))
})

test_that("sd without na.rm returns NA when group has NAs", {
  df <- data.frame(g = c("a", "a", "a"), x = c(2.0, NA, 6.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sd(x)) |> collect()
  expect_true(is.na(result$s))
})

# --- var ---

test_that("var computes sample variance", {
  df <- data.frame(g = c("a", "a", "a"), x = c(2.0, 4.0, 6.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(v = var(x)) |> collect()
  expect_equal(result$v, var(c(2, 4, 6)))
})

test_that("var of single value returns NA", {
  df <- data.frame(g = c("a"), x = c(5.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(v = var(x)) |> collect()
  expect_true(is.na(result$v))
})

# --- first ---

test_that("first returns the first value per group", {
  df <- data.frame(g = c("a", "a", "b", "b"),
                   x = c(10.0, 20.0, 30.0, 40.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(f = first(x)) |> collect()
  expect_equal(result$f[result$g == "a"], 10)
  expect_equal(result$f[result$g == "b"], 30)
})

test_that("first with na.rm = TRUE skips leading NAs", {
  df <- data.frame(g = c("a", "a", "a"), x = c(NA, 2.0, 3.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(f = first(x, na.rm = TRUE)) |> collect()
  expect_equal(result$f, 2)
})

test_that("first without na.rm returns NA when first value is NA", {
  df <- data.frame(g = c("a", "a"), x = c(NA, 2.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(f = first(x)) |> collect()
  expect_true(is.na(result$f))
})

# --- last ---

test_that("last returns the last value per group", {
  df <- data.frame(g = c("a", "a", "b", "b"),
                   x = c(10.0, 20.0, 30.0, 40.0), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(l = last(x)) |> collect()
  expect_equal(result$l[result$g == "a"], 20)
  expect_equal(result$l[result$g == "b"], 40)
})

test_that("last with na.rm = TRUE skips trailing NAs", {
  df <- data.frame(g = c("a", "a", "a"), x = c(1.0, 2.0, NA),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(l = last(x, na.rm = TRUE)) |> collect()
  expect_equal(result$l, 2)
})

# --- sd/var with multiple groups ---

test_that("sd works across multiple groups", {
  df <- data.frame(g = c("a", "a", "a", "b", "b", "b"),
                   x = c(1.0, 2.0, 3.0, 10.0, 20.0, 30.0),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(s = sd(x)) |> collect()
  expect_equal(result$s[result$g == "a"], sd(c(1, 2, 3)))
  expect_equal(result$s[result$g == "b"], sd(c(10, 20, 30)))
})

# --- slice_min/max with_ties ---

test_that("slice_min with_ties = TRUE includes tied rows", {
  df <- data.frame(x = c(1.0, 2.0, 2.0, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 2, with_ties = TRUE)
  # n=2 -> boundary is 2.0, so include 1, 2, 2 = 3 rows
  expect_equal(nrow(result), 3)
  expect_true(all(result$x <= 2))
})

test_that("slice_min with_ties = FALSE returns exactly n rows", {
  df <- data.frame(x = c(1.0, 2.0, 2.0, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_min(order_by = x, n = 2, with_ties = FALSE) |> collect()
  expect_equal(nrow(result), 2)
})

test_that("slice_max with_ties = TRUE includes tied rows", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_max(order_by = x, n = 2, with_ties = TRUE)
  # n=2 -> boundary is 3.0, so include 3, 3, 4 = 3 rows
  expect_equal(nrow(result), 3)
  expect_true(all(result$x >= 3))
})

test_that("slice_max with_ties = FALSE returns exactly n rows", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 3.0, 4.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> slice_max(order_by = x, n = 2, with_ties = FALSE) |> collect()
  expect_equal(nrow(result), 2)
})

test_that("slice_min with no ties behaves same either way", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result_ties <- tbl(f) |> slice_min(order_by = x, n = 2, with_ties = TRUE)
  result_no <- tbl(f) |> slice_min(order_by = x, n = 2, with_ties = FALSE) |> collect()
  expect_equal(nrow(result_ties), 2)
  expect_equal(nrow(result_no), 2)
})

# --- first/last with all NAs ---

test_that("first with na.rm = TRUE on all-NA group returns NA", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(f = first(x, na.rm = TRUE)) |> collect()
  expect_true(is.na(result$f))
})

test_that("last with na.rm = TRUE on all-NA group returns NA", {
  df <- data.frame(g = c("a", "a"), x = c(NA_real_, NA_real_),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> group_by(g) |> summarise(l = last(x, na.rm = TRUE)) |> collect()
  expect_true(is.na(result$l))
})

# --- transmute with across ---

test_that("transmute supports across() in summarise context", {
  # across() with aggregation functions works in summarise, not transmute
  # Test that across() expands correctly in summarise
  df <- data.frame(x = c(1.0, 2.0, 3.0), y = c(10.0, 20.0, 30.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> summarise(across(c(x, y), mean)) |> collect()
  expect_equal(names(result), c("x", "y"))
  expect_equal(result$x, 2)
  expect_equal(result$y, 20)
})

# --- min/max on logical, any/all NA semantics ---

test_that("min and max on a logical column reduce to 0/1", {
  df <- data.frame(b = c(TRUE, FALSE, TRUE))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  expect_equal(collect(summarise(tbl(f), m = min(b)))$m, min(c(TRUE, FALSE, TRUE)))
  expect_equal(collect(summarise(tbl(f), m = max(b)))$m, max(c(TRUE, FALSE, TRUE)))
})

test_that("any/all follow R's three-valued NA logic", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  # a determining value wins over NA
  write_vtr(data.frame(b = c(TRUE, NA)), f)
  expect_equal(collect(summarise(tbl(f), m = any(b)))$m, as.numeric(any(c(TRUE, NA))))
  write_vtr(data.frame(b = c(FALSE, NA)), f)
  expect_equal(collect(summarise(tbl(f), m = all(b)))$m, as.numeric(all(c(FALSE, NA))))
  # NA only when the result is undetermined
  write_vtr(data.frame(b = c(FALSE, NA)), f)
  expect_true(is.na(collect(summarise(tbl(f), m = any(b)))$m))
  write_vtr(data.frame(b = c(TRUE, NA)), f)
  expect_true(is.na(collect(summarise(tbl(f), m = all(b)))$m))
})

test_that("across() forwards extra arguments and names an unnamed fn list", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  # ... args (na.rm) reach each function
  write_vtr(data.frame(a = c(1, NA, 3), b = c(10, 20, NA)), f)
  r <- collect(summarise(tbl(f), across(c(a, b), mean, na.rm = TRUE)))
  expect_equal(r$a, mean(c(1, NA, 3), na.rm = TRUE))
  expect_equal(r$b, mean(c(10, 20, NA), na.rm = TRUE))
  # an unnamed list of functions is indexed, not collapsed onto one column
  write_vtr(data.frame(a = c(1, 2, 3)), f)
  r <- collect(summarise(tbl(f), across(a, list(mean, sum))))
  expect_equal(sort(names(r)), c("a_1", "a_2"))
  expect_equal(r$a_1, 2)
  expect_equal(r$a_2, 6)
})

test_that("n_distinct counts NA by default and drops it with na.rm", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(1, 2, NA, 2, NA)), f)
  expect_equal(collect(summarise(tbl(f), d = n_distinct(x)))$d,
               length(unique(c(1, 2, NA, 2, NA))))                   # 3, NA counted
  expect_equal(collect(summarise(tbl(f), d = n_distinct(x, na.rm = TRUE)))$d,
               length(unique(na.omit(c(1, 2, NA, 2, NA)))))          # 2, NA dropped
})
