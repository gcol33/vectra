test_that("write_vtr streams VTR -> VTR", {
  f <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))
  write_vtr(mtcars, f)
  tbl(f) |> write_vtr(f2)
  result <- tbl(f2) |> collect()
  expect_equal(nrow(result), 32L)
  expect_equal(ncol(result), 11L)
  expect_equal(result$mpg, mtcars$mpg)
})

test_that("write_vtr streams CSV -> VTR", {
  csv <- tempfile(fileext = ".csv")
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(csv, f)))
  write.csv(mtcars, csv, row.names = FALSE)
  tbl_csv(csv) |> write_vtr(f)
  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 32L)
  expect_equal(result$mpg, mtcars$mpg)
})

test_that("write_vtr streams SQLite -> VTR", {
  f <- tempfile(fileext = ".vtr")
  db <- tempfile(fileext = ".sqlite")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, db, f2)))
  write_vtr(mtcars, f)
  tbl(f) |> write_sqlite(db, "cars")
  tbl_sqlite(db, "cars") |> write_vtr(f2)
  result <- tbl(f2) |> collect()
  expect_equal(nrow(result), 32L)
})

test_that("write_vtr streams filtered node", {
  f <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))
  write_vtr(mtcars, f)
  tbl(f) |> filter(mpg > 20) |> write_vtr(f2)
  result <- tbl(f2) |> collect()
  expect_equal(nrow(result), sum(mtcars$mpg > 20))
})

test_that("write_vtr streams mutated node", {
  f <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))
  write_vtr(mtcars, f)
  tbl(f) |> mutate(kpl = mpg * 0.425144) |> write_vtr(f2)
  result <- tbl(f2) |> collect()
  expect_equal(ncol(result), 12L)
  expect_true("kpl" %in% names(result))
})

test_that("write_vtr atomic write leaves no temp file", {
  f <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))
  write_vtr(mtcars, f)
  tbl(f) |> write_vtr(f2)
  expect_false(file.exists(paste0(f2, ".~writing")))
  expect_true(file.exists(f2))
})

test_that("write_vtr.data.frame still works", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(iris, f)
  result <- tbl(f) |> collect()
  expect_equal(nrow(result), 150L)
  expect_equal(ncol(result), 5L)
})

test_that("character columns round-trip through a streamed node write (#5)", {
  f <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))
  df <- data.frame(id = 1:3, name = c("Alpha Range", "Beta Range", "Gamma"),
                   stringsAsFactors = FALSE)
  write_vtr(df, f)
  tbl(f) |> filter(id >= 1L) |> select(id, name) |> write_vtr(f2)
  result <- tbl(f2) |> collect()
  expect_type(result$name, "character")
  expect_equal(result$name, df$name)
})

test_that("a query is consumed once: reusing a collected node errors (#5)", {
  # The #5 symptom was silent garbage: a node was collect()ed for inspection
  # and then written; the second terminal op drained an exhausted plan. A query
  # must run exactly once and say so, rather than return reinterpreted bytes.
  f <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, f2)))
  write_vtr(data.frame(id = 1:3, name = c("a", "b", "c"),
                       stringsAsFactors = FALSE), f)

  n <- tbl(f) |> filter(id >= 1L)
  invisible(collect(n))
  expect_error(write_vtr(n, f2), "already been consumed")

  n2 <- tbl(f) |> filter(id >= 1L)
  invisible(collect(n2))
  expect_error(collect(n2), "already been consumed")

  n3 <- tbl(f) |> select(id, name)
  write_vtr(n3, f2)
  expect_error(collect(n3), "already been consumed")
})
