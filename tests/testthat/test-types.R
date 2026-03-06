# --- Date roundtrip ---

test_that("Date columns roundtrip through write_vtr/collect", {
  df <- data.frame(d = as.Date(c("2024-01-01", "2024-06-15", NA)),
                   x = c(1, 2, 3))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_s3_class(result$d, "Date")
  expect_equal(result$d, df$d)
  expect_equal(result$x, df$x)
})

# --- POSIXct roundtrip ---

test_that("POSIXct columns roundtrip through write_vtr/collect", {
  df <- data.frame(
    ts = as.POSIXct(c("2024-01-01 12:00:00", "2024-06-15 18:30:00", NA),
                    tz = "UTC"),
    x = c(1, 2, 3)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_s3_class(result$ts, "POSIXct")
  expect_equal(result$ts, df$ts)
})

# --- Factor roundtrip ---

test_that("factor columns roundtrip through write_vtr/collect", {
  df <- data.frame(g = factor(c("a", "b", "a", NA), levels = c("a", "b", "c")),
                   x = c(1, 2, 3, 4))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> collect()
  expect_s3_class(result$g, "factor")
  expect_equal(levels(result$g), c("a", "b", "c"))
  expect_equal(as.character(result$g), c("a", "b", "a", NA))
})

# --- where() in select ---

test_that("select with where(is.numeric)", {
  df <- data.frame(a = 1.0, b = "x", c = 2L, stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(where(is.numeric)) |> collect()
  expect_true(all(c("a", "c") %in% names(result)))
  expect_false("b" %in% names(result))
})

test_that("select with where(is.character)", {
  df <- data.frame(a = 1.0, b = "x", c = 2L, stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> select(where(is.character)) |> collect()
  expect_equal(names(result), "b")
})

# --- bind_rows coercion ---

test_that("bind_rows coerces int to double across nodes", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1L, 2L)), f1)
  write_vtr(data.frame(x = c(3.5, 4.5)), f2)
  result <- bind_rows(tbl(f1), tbl(f2)) |> collect()
  expect_equal(nrow(result), 4)
  expect_equal(result$x, c(1, 2, 3.5, 4.5))
})

# --- across with where() ---

test_that("across with where(is.numeric) in summarise", {
  df <- data.frame(g = c("a", "a", "b"), x = c(1.0, 2.0, 3.0),
                   y = c("p", "q", "r"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    group_by(g) |>
    summarise(across(where(is.numeric), sum)) |>
    collect()
  expect_equal(result$x[result$g == "a"], 3)
})

# --- Date with multi-rowgroup ---

test_that("Date roundtrip with multiple row groups", {
  dates <- as.Date("2020-01-01") + 0:99
  df <- data.frame(d = dates, x = seq_along(dates))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f, batch_size = 30)
  result <- tbl(f) |> collect()
  expect_s3_class(result$d, "Date")
  expect_equal(result$d, df$d)
})
