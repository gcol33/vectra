# --- math functions ---

test_that("abs works in mutate", {
  df <- data.frame(x = c(-1.0, 2.0, -3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(a = abs(x)) |> collect()
  expect_equal(result$a, c(1, 2, 3))
})

test_that("sqrt works in mutate", {
  df <- data.frame(x = c(4.0, 9.0, 16.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(s = sqrt(x)) |> collect()
  expect_equal(result$s, c(2, 3, 4))
})

test_that("log and exp are inverses", {
  df <- data.frame(x = c(1.0, 2.0, 10.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(lx = log(x), elx = exp(log(x))) |> collect()
  expect_equal(result$elx, result$x, tolerance = 1e-10)
})

test_that("floor and ceiling work", {
  df <- data.frame(x = c(1.2, 2.8, -1.5))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(fl = floor(x), ce = ceiling(x)) |> collect()
  expect_equal(result$fl, c(1, 2, -2))
  expect_equal(result$ce, c(2, 3, -1))
})

test_that("round works", {
  df <- data.frame(x = c(1.3, 2.7, 3.4))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(r = round(x)) |> collect()
  expect_equal(result$r, c(1, 3, 3))
})

test_that("math functions propagate NA", {
  df <- data.frame(x = c(4.0, NA, 9.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(s = sqrt(x)) |> collect()
  expect_equal(result$s[1], 2)
  expect_true(is.na(result$s[2]))
  expect_equal(result$s[3], 3)
})

# --- if_else ---

test_that("if_else works in mutate", {
  df <- data.frame(x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(label = if_else(x > 2, 10.0, 0.0)) |> collect()
  expect_equal(result$label, c(0, 0, 10))
})

test_that("if_else with NA condition returns NA", {
  df <- data.frame(x = c(1.0, NA, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(label = if_else(x > 2, 1.0, 0.0)) |> collect()
  expect_equal(result$label[1], 0)
  expect_true(is.na(result$label[2]))
  expect_equal(result$label[3], 1)
})

# --- arithmetic semantics (match base R) ---

test_that("division is always double", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(a = c(1L, 7L, 0L), b = c(2L, 0L, 0L)), f)
  z <- collect(mutate(tbl(f), z = a / b))$z
  expect_equal(z, c(1L, 7L, 0L) / c(2L, 0L, 0L))  # 0.5, Inf, NaN
})

test_that("modulo takes the divisor's sign (floored)", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(a = c(-7L, 7L, 8L), b = c(3L, -3L, 3L)), f)
  expect_equal(collect(mutate(tbl(f), z = a %% b))$z,
               c(-7L, 7L, 8L) %% c(3L, -3L, 3L))
  write_vtr(data.frame(a = c(-7.5, 7.5), b = c(3, -3)), f)
  expect_equal(collect(mutate(tbl(f), z = a %% b))$z, c(-7.5, 7.5) %% c(3, -3))
})

test_that("round(x, digits) honours the digit count", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(3.14159, 2.71828, 123.456)), f)
  expect_equal(collect(mutate(tbl(f), z = round(x, 2)))$z,
               round(c(3.14159, 2.71828, 123.456), 2))
  expect_equal(collect(mutate(tbl(f), z = round(x, 0)))$z,
               round(c(3.14159, 2.71828, 123.456), 0))
})

test_that("log(x, base) honours the base", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(1, 10, 100, 8)), f)
  expect_equal(collect(mutate(tbl(f), z = log(x, 10)))$z, log(c(1, 10, 100, 8), 10))
  expect_equal(collect(mutate(tbl(f), z = log(x, 2)))$z, log(c(1, 10, 100, 8), 2))
})

test_that("grepl treats the pattern as a regex by default", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(s = c("Apple", "Banana", "Cherry")), f)
  expect_equal(collect(filter(tbl(f), grepl("^A|^B", s)))$s, c("Apple", "Banana"))
  # fixed = TRUE restores literal matching
  expect_equal(collect(filter(tbl(f), grepl(".", s, fixed = TRUE)))$s, character(0))
})

# --- %in% ---

test_that("%in% works in filter", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(x %in% c(2, 4)) |> collect()
  expect_equal(result$x, c(2, 4))
})

test_that("%in% works with strings", {
  df <- data.frame(s = c("a", "b", "c", "d"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(s %in% c("a", "c")) |> collect()
  expect_equal(result$s, c("a", "c"))
})

test_that("%in% coerces a mismatched operand and set type (no crash)", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  # double column against an integer set -- previously a segfault
  write_vtr(data.frame(x = c(1.0, 2.0, 5.0)), f)
  expect_equal(nrow(collect(filter(tbl(f), x %in% 1:3))), 2L)
  expect_equal(collect(mutate(tbl(f), h = x %in% 1:3))$h, c(1, 2, 5) %in% 1:3)
  # integer column against a double set
  write_vtr(data.frame(x = c(1L, 2L, 5L)), f)
  expect_equal(collect(mutate(tbl(f), h = x %in% c(2, 4)))$h,
               c(1L, 2L, 5L) %in% c(2, 4))
  # large mismatched set takes the hash path
  write_vtr(data.frame(x = as.numeric(1:30)), f)
  expect_equal(collect(mutate(tbl(f), h = x %in% 5:25))$h, (1:30) %in% 5:25)
  # logical set against a logical column
  write_vtr(data.frame(x = c(TRUE, FALSE, TRUE)), f)
  expect_equal(collect(mutate(tbl(f), h = x %in% c(TRUE)))$h,
               c(TRUE, FALSE, TRUE) %in% c(TRUE))
})

test_that("%in% treats -0.0 and 0.0 as equal in the hash path", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(-0.0, 3.0)), f)
  expect_equal(collect(mutate(tbl(f), h = x %in% c(0, 2:18)))$h,
               c(-0.0, 3.0) %in% c(0, 2:18))
})

test_that("%in% rejects a string column against a numeric set", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c("a", "b")), f)
  expect_error(collect(filter(tbl(f), x %in% c(1, 2))))
})

# --- case_when / coalesce mixed value types ---

test_that("case_when promotes to the common value type", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(1L, 3L)), f)
  # integer branch, double default -> double, not a bit reinterpret
  expect_equal(collect(mutate(tbl(f), z = case_when(x > 2 ~ 1L, .default = 2.5)))$z,
               c(2.5, 1))
  expect_equal(collect(mutate(tbl(f), z = case_when(x > 2 ~ 2.5, .default = 1L)))$z,
               c(1, 2.5))
})

test_that("coalesce promotes to the common argument type", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(a = c(NA, 1.5), b = c(7L, 9L)), f)
  expect_equal(collect(mutate(tbl(f), z = coalesce(a, b)))$z, c(7, 1.5))
  write_vtr(data.frame(a = c(NA_integer_, 3L), b = c(4.5, 9.5)), f)
  expect_equal(collect(mutate(tbl(f), z = coalesce(a, b)))$z, c(4.5, 3))
})

# --- between ---

test_that("between works in filter", {
  df <- data.frame(x = c(1.0, 2.0, 3.0, 4.0, 5.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(between(x, 2, 4)) |> collect()
  expect_equal(result$x, c(2, 3, 4))
})

# --- type casting ---

test_that("as.numeric works in mutate", {
  df <- data.frame(x = c(1L, 2L, 3L))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = as.numeric(x)) |> collect()
  expect_true(is.double(result$y))
})

# --- string functions ---

test_that("tolower and toupper work", {
  df <- data.frame(s = c("Hello", "WORLD"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(lo = tolower(s), up = toupper(s)) |> collect()
  expect_equal(result$lo, c("hello", "world"))
  expect_equal(result$up, c("HELLO", "WORLD"))
})

test_that("trimws removes whitespace", {
  df <- data.frame(s = c("  hello  ", "\tfoo\n"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(t = trimws(s)) |> collect()
  expect_equal(result$t, c("hello", "foo"))
})

# --- additional math functions ---

test_that("log2 and log10 work", {
  df <- data.frame(x = c(8.0, 100.0, 1.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(l2 = log2(x), l10 = log10(x)) |> collect()
  expect_equal(result$l2, log2(c(8, 100, 1)))
  expect_equal(result$l10, log10(c(8, 100, 1)))
})

test_that("sign works", {
  df <- data.frame(x = c(-5.0, 0.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(s = sign(x)) |> collect()
  expect_equal(result$s, c(-1, 0, 1))
})

test_that("trunc works", {
  df <- data.frame(x = c(1.7, -2.3, 0.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(t = trunc(x)) |> collect()
  expect_equal(result$t, c(1, -2, 0))
})

# --- pmin / pmax ---

test_that("pmin returns element-wise minimum", {
  df <- data.frame(x = c(1.0, 5.0, 3.0), y = c(4.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(m = pmin(x, y)) |> collect()
  expect_equal(result$m, c(1, 2, 3))
})

test_that("pmax returns element-wise maximum", {
  df <- data.frame(x = c(1.0, 5.0, 3.0), y = c(4.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(m = pmax(x, y)) |> collect()
  expect_equal(result$m, c(4, 5, 3))
})

# --- paste0 ---

test_that("paste0 concatenates two string columns", {
  df <- data.frame(a = c("hello", "foo"), b = c(" world", "bar"),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(c = paste0(a, b)) |> collect()
  expect_equal(result$c, c("hello world", "foobar"))
})

test_that("paste0 with NA returns NA", {
  df <- data.frame(a = c("hello", NA), b = c(" world", "bar"),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(c = paste0(a, b)) |> collect()
  expect_equal(result$c[1], "hello world")
  expect_true(is.na(result$c[2]))
})

# --- startsWith / endsWith ---

test_that("startsWith works in filter", {
  df <- data.frame(s = c("apple", "banana", "avocado"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(startsWith(s, "a")) |> collect()
  expect_equal(result$s, c("apple", "avocado"))
})

test_that("endsWith works in filter", {
  df <- data.frame(s = c("cat", "dog", "rat"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(endsWith(s, "at")) |> collect()
  expect_equal(result$s, c("cat", "rat"))
})

# --- gsub / sub ---

test_that("gsub replaces all occurrences", {
  df <- data.frame(s = c("aXbXc", "XdX"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(r = gsub("X", "_", s)) |> collect()
  expect_equal(result$r, c("a_b_c", "_d_"))
})

test_that("sub replaces only first occurrence", {
  df <- data.frame(s = c("aXbXc", "XdX"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(r = sub("X", "_", s)) |> collect()
  expect_equal(result$r, c("a_bXc", "_dX"))
})

# --- pmin/pmax multi-arg, paste collapse ---

test_that("pmin/pmax accept more than two arguments", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(a = c(5, 1), b = c(3, 4), c = c(2, 9)), f)
  expect_equal(collect(mutate(tbl(f), z = pmin(a, b, c)))$z, pmin(c(5, 1), c(3, 4), c(2, 9)))
  expect_equal(collect(mutate(tbl(f), z = pmax(a, b, c)))$z, pmax(c(5, 1), c(3, 4), c(2, 9)))
})

test_that("paste(collapse=) errors rather than silently dropping", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c("a", "b")), f)
  expect_error(collect(mutate(tbl(f), z = paste(x, collapse = ","))), "collapse")
})
