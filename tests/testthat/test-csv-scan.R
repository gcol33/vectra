test_that("tbl_csv reads basic CSV and returns correct data.frame", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  write.csv(mtcars[1:10, ], f, row.names = FALSE)

  result <- tbl_csv(f) |> collect()
  expect_equal(nrow(result), 10)
  expect_equal(ncol(result), ncol(mtcars))
  expect_equal(names(result), names(mtcars))
  expect_equal(result$mpg, mtcars$mpg[1:10])
})

test_that("tbl_csv infers integer columns correctly", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  df <- data.frame(x = 1:5, y = c(1.5, 2.5, 3.5, 4.5, 5.5))
  write.csv(df, f, row.names = FALSE)

  result <- tbl_csv(f) |> collect()
  # x should be int64 (returned as double by default)
  expect_type(result$x, "double")
  expect_equal(result$x, c(1, 2, 3, 4, 5))
  expect_equal(result$y, df$y)
})

test_that("tbl_csv handles string columns", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  df <- data.frame(name = c("Alice", "Bob", "Charlie"), score = c(90, 85, 95),
                   stringsAsFactors = FALSE)
  write.csv(df, f, row.names = FALSE)

  result <- tbl_csv(f) |> collect()
  expect_equal(result$name, c("Alice", "Bob", "Charlie"))
  expect_equal(result$score, c(90, 85, 95))
})

test_that("tbl_csv handles NA values", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  writeLines("x,y,z\n1,hello,TRUE\nNA,NA,NA\n3,world,FALSE", f)

  result <- tbl_csv(f) |> collect()
  expect_equal(result$x[1], 1)
  expect_true(is.na(result$x[2]))
  expect_equal(result$x[3], 3)
  expect_true(is.na(result$y[2]))
  expect_true(is.na(result$z[2]))
})

test_that("tbl_csv handles logical columns", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  writeLines("a,b\nTRUE,1\nFALSE,2\nTRUE,3", f)

  result <- tbl_csv(f) |> collect()
  expect_equal(result$a, c(TRUE, FALSE, TRUE))
})

test_that("tbl_csv handles quoted fields with commas", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  writeLines('name,city\n"Smith, John","New York"\n"Doe, Jane",Boston', f)

  result <- tbl_csv(f) |> collect()
  expect_equal(result$name, c("Smith, John", "Doe, Jane"))
  expect_equal(result$city, c("New York", "Boston"))
})

test_that("tbl_csv works with filter pipeline", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  write.csv(mtcars, f, row.names = FALSE)

  result <- tbl_csv(f) |>
    filter(cyl == 6) |>
    select(mpg, cyl, hp) |>
    collect()

  expected <- mtcars[mtcars$cyl == 6, c("mpg", "cyl", "hp")]
  expect_equal(nrow(result), nrow(expected))
  expect_equal(result$mpg, expected$mpg)
})

test_that("tbl_csv works with group_by + summarise", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  write.csv(mtcars, f, row.names = FALSE)

  result <- tbl_csv(f) |>
    group_by(cyl) |>
    summarise(mean_mpg = mean(mpg)) |>
    arrange(cyl) |>
    collect()

  expect_equal(nrow(result), 3)
  expect_equal(result$cyl, c(4, 6, 8))
})

test_that("tbl_csv respects batch_size parameter", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  write.csv(mtcars, f, row.names = FALSE)

  # Small batch size should still give correct results
  result <- tbl_csv(f, batch_size = 5L) |> collect()
  expect_equal(nrow(result), nrow(mtcars))
  expect_equal(result$mpg, mtcars$mpg)
})

test_that("tbl_csv handles empty fields as NA", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  writeLines("x,y\n1,hello\n2,\n3,world", f)

  result <- tbl_csv(f) |> collect()
  expect_true(is.na(result$y[2]))
  expect_equal(result$y[c(1, 3)], c("hello", "world"))
})

test_that("tbl_csv explain shows CsvScanNode", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f))
  write.csv(mtcars[1:5, ], f, row.names = FALSE)

  node <- tbl_csv(f)
  plan <- capture.output(explain(node))
  expect_true(any(grepl("CsvScanNode", plan)))
})

test_that("tbl_csv pipe to write_csv round-trips", {
  f_in <- tempfile(fileext = ".csv")
  f_out <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f_in, f_out)))

  df <- data.frame(a = 1:3, b = c("x", "y", "z"), stringsAsFactors = FALSE)
  write.csv(df, f_in, row.names = FALSE)

  tbl_csv(f_in) |> write_csv(f_out)

  result <- tbl_csv(f_out) |> collect()
  expect_equal(result$a, c(1, 2, 3))
  expect_equal(result$b, c("x", "y", "z"))
})
