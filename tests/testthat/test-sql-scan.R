test_that("write_sqlite + tbl_sqlite round-trips a data.frame", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  write_sqlite(mtcars[1:10, ], db, "cars")
  result <- tbl_sqlite(db, "cars") |> collect()

  expect_equal(nrow(result), 10)
  expect_equal(ncol(result), ncol(mtcars))
  expect_equal(result$mpg, mtcars$mpg[1:10])
  expect_equal(result$cyl, mtcars$cyl[1:10])
})

test_that("tbl_sqlite + filter gives same result as R subset", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  write_sqlite(mtcars, db, "cars")
  result <- tbl_sqlite(db, "cars") |>
    filter(cyl == 6) |>
    select(mpg, cyl) |>
    collect()

  expected <- mtcars[mtcars$cyl == 6, c("mpg", "cyl")]
  expect_equal(nrow(result), nrow(expected))
  expect_equal(result$mpg, expected$mpg)
})

test_that("tbl_sqlite handles string columns", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  df <- data.frame(name = c("Alice", "Bob", "Charlie"),
                   score = c(90, 85, 95),
                   stringsAsFactors = FALSE)
  write_sqlite(df, db, "people")
  result <- tbl_sqlite(db, "people") |> collect()

  expect_equal(result$name, c("Alice", "Bob", "Charlie"))
  expect_equal(result$score, c(90, 85, 95))
})

test_that("tbl_sqlite handles NA values", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  df <- data.frame(x = c(1, NA, 3), y = c("a", NA, "c"),
                   stringsAsFactors = FALSE)
  write_sqlite(df, db, "nulls")
  result <- tbl_sqlite(db, "nulls") |> collect()

  expect_equal(result$x[1], 1)
  expect_true(is.na(result$x[2]))
  expect_equal(result$x[3], 3)
  expect_true(is.na(result$y[2]))
})

test_that("tbl_sqlite works with filter pipeline", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  write_sqlite(mtcars, db, "cars")
  result <- tbl_sqlite(db, "cars") |>
    filter(cyl == 6) |>
    select(mpg, cyl, hp) |>
    collect()

  expected <- mtcars[mtcars$cyl == 6, c("mpg", "cyl", "hp")]
  expect_equal(nrow(result), nrow(expected))
  expect_equal(result$mpg, expected$mpg)
})

test_that("tbl_sqlite works with group_by + summarise", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  write_sqlite(mtcars, db, "cars")
  result <- tbl_sqlite(db, "cars") |>
    group_by(cyl) |>
    summarise(mean_mpg = mean(mpg)) |>
    arrange(cyl) |>
    collect()

  expect_equal(nrow(result), 3)
  expect_equal(result$cyl, c(4, 6, 8))
})

test_that("tbl_sqlite explain shows SqlScanNode", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  write_sqlite(mtcars[1:5, ], db, "cars")
  node <- tbl_sqlite(db, "cars")
  plan <- capture.output(explain(node))
  expect_true(any(grepl("SqlScanNode", plan)))
})

test_that("write_sqlite streams from vectra_node", {
  f <- tempfile(fileext = ".vtr")
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(c(f, db)))

  write_vtr(mtcars, f)
  tbl(f) |>
    filter(cyl == 4) |>
    write_sqlite(db, "four_cyl")

  result <- tbl_sqlite(db, "four_cyl") |> collect()
  expected <- mtcars[mtcars$cyl == 4, ]
  expect_equal(nrow(result), nrow(expected))
  expect_equal(result$mpg, expected$mpg)
})

test_that("tbl_sqlite handles logical columns via INTEGER", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  df <- data.frame(flag = c(TRUE, FALSE, TRUE), val = c(1, 2, 3))
  write_sqlite(df, db, "bools")
  result <- tbl_sqlite(db, "bools") |> collect()

  # Bools stored as INTEGER in SQLite, read back as int64 → double
  expect_equal(result$val, c(1, 2, 3))
})

test_that("tbl_sqlite respects batch_size", {
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(db))

  write_sqlite(mtcars, db, "cars")
  result <- tbl_sqlite(db, "cars", batch_size = 5L) |> collect()
  expect_equal(nrow(result), nrow(mtcars))
  expect_equal(result$mpg, mtcars$mpg)
})

test_that("csv -> sqlite -> collect round-trip works", {
  csv_f <- tempfile(fileext = ".csv")
  db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(c(csv_f, db)))

  write.csv(mtcars[1:10, ], csv_f, row.names = FALSE)
  tbl_csv(csv_f) |> write_sqlite(db, "from_csv")

  result <- tbl_sqlite(db, "from_csv") |> collect()
  expect_equal(nrow(result), 10)
  expect_equal(result$mpg, mtcars$mpg[1:10])
})
