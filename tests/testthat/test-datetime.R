# --- year/month/day extraction from Date ---

test_that("year extracts year from Date column", {
  df <- data.frame(d = as.Date(c("2020-03-15", "2021-12-01", "1999-06-30")))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = year(d)) |> collect()
  expect_equal(result$y, c(2020, 2021, 1999))
})

test_that("month extracts month from Date column", {
  df <- data.frame(d = as.Date(c("2020-03-15", "2021-12-01")))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(m = month(d)) |> collect()
  expect_equal(result$m, c(3, 12))
})

test_that("day extracts day from Date column", {
  df <- data.frame(d = as.Date(c("2020-03-15", "2021-12-01")))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(dy = day(d)) |> collect()
  expect_equal(result$dy, c(15, 1))
})

# --- date filtering with as.Date literal ---

test_that("filter with as.Date literal works", {
  df <- data.frame(d = as.Date(c("2019-01-01", "2020-06-15", "2021-12-31")),
                   x = c(1.0, 2.0, 3.0))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(d >= as.Date("2020-01-01")) |> collect()
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(2, 3))
})

# --- as.Date from string column ---

test_that("as.Date converts string column to date numeric", {
  df <- data.frame(s = c("2020-01-15", "2021-06-30"), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = as.Date(s)) |> collect()
  expected <- as.double(as.Date(c("2020-01-15", "2021-06-30")))
  expect_equal(result$d, expected)
})

# --- NA handling in date functions ---

test_that("year handles NA dates", {
  df <- data.frame(d = as.Date(c("2020-03-15", NA)))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(y = year(d)) |> collect()
  expect_equal(result$y[1], 2020)
  expect_true(is.na(result$y[2]))
})

# --- POSIXct extraction ---

test_that("hour/minute/second extract from POSIXct", {
  df <- data.frame(t = as.POSIXct(c("2020-03-15 14:30:45", "2021-12-01 08:15:00"), tz = "UTC"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(h = hour(t), mi = minute(t), s = second(t)) |> collect()
  expect_equal(result$h, c(14, 8))
  expect_equal(result$mi, c(30, 15))
  expect_equal(result$s, c(45, 0))
})

# --- date arithmetic ---

test_that("date arithmetic works (add days)", {
  df <- data.frame(d = as.Date(c("2020-01-01", "2020-06-15")))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d2 = d + 30) |> collect()
  expected <- as.double(as.Date(c("2020-01-01", "2020-06-15")) + 30)
  expect_equal(result$d2, expected)
})

# --- group_by with year ---

test_that("group_by year works with Date column", {
  df <- data.frame(
    d = as.Date(c("2020-03-15", "2020-07-01", "2021-01-15", "2021-06-30")),
    x = c(1.0, 2.0, 3.0, 4.0)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(yr = year(d)) |>
    group_by(yr) |>
    summarise(total = sum(x)) |>
    collect()
  expect_equal(result$total[result$yr == 2020], 3)
  expect_equal(result$total[result$yr == 2021], 7)
})

# --- as.POSIXct literal in filter ---

test_that("filter with as.POSIXct literal works", {
  df <- data.frame(
    t = as.POSIXct(c("2020-01-01 00:00:00", "2020-06-15 12:00:00", "2021-01-01 00:00:00"), tz = "UTC"),
    x = c(1.0, 2.0, 3.0)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(t >= as.POSIXct("2020-06-01", tz = "UTC")) |> collect()
  expect_equal(nrow(result), 2)
})
