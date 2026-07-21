test_that("nrow/ncol/dim read a .vtr table's shape from metadata", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_equal(dim(tbl(f)), c(nrow(mtcars), ncol(mtcars)))
  expect_equal(nrow(tbl(f)), nrow(mtcars))
  expect_equal(ncol(tbl(f)), ncol(mtcars))
  expect_type(dim(tbl(f)), "integer")
})

test_that("counting a table does not consume it", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  node <- tbl(f)
  expect_equal(nrow(node), 32L)
  expect_equal(nrow(node), 32L)      # repeatable
  expect_equal(nrow(collect(node)), 32L)
})

test_that("row count survives multiple row groups", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  df <- data.frame(x = 1:5000, y = as.double(1:5000))
  write_vtr(df, f, batch_size = 512)

  expect_equal(nrow(tbl(f)), 5000L)
})

test_that("row-preserving verbs carry the count through", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_equal(nrow(tbl(f) |> select(mpg, cyl)), 32L)
  expect_equal(ncol(tbl(f) |> select(mpg, cyl)), 2L)
  expect_equal(nrow(tbl(f) |> mutate(z = mpg * 2)), 32L)
  expect_equal(ncol(tbl(f) |> mutate(z = mpg * 2)), ncol(mtcars) + 1L)
  expect_equal(nrow(tbl(f) |> rename(miles = mpg)), 32L)
  expect_equal(nrow(tbl(f) |> arrange(mpg)), 32L)
  expect_equal(nrow(tbl(f) |> relocate(cyl)), 32L)
})

test_that("limit-shaped verbs clamp the count", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_equal(nrow(tbl(f) |> slice_head(n = 5)), 5L)
  expect_equal(nrow(tbl(f) |> slice_head(n = 100)), 32L)   # cap above the input
  expect_equal(nrow(tbl(f) |> slice_min(mpg, n = 3, with_ties = FALSE)), 3L)
  expect_equal(nrow(tbl(f) |> slice_max(mpg, n = 3, with_ties = FALSE)), 3L)
})

test_that("window functions preserve the count", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_equal(nrow(tbl(f) |> mutate(r = row_number())), 32L)
  expect_equal(nrow(tbl(f) |> mutate(cs = cumsum(mpg))), 32L)
  expect_equal(nrow(tbl(f) |> group_by(cyl) |> mutate(r = rank(mpg))), 32L)
})

test_that("bind_rows sums the counts of counted inputs", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)), add = TRUE)
  write_vtr(mtcars, f1)
  write_vtr(mtcars, f2)

  expect_equal(nrow(bind_rows(tbl(f1), tbl(f2))), 64L)
  # one uncounted input makes the total uncountable
  expect_true(is.na(nrow(bind_rows(tbl(f1), tbl(f2) |> filter(cyl == 4)))))
})

test_that("deletions subtract from the count", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".del"))), add = TRUE)
  write_vtr(mtcars, f)

  delete_vtr(f, c(1L, 2L, 3L))
  expect_equal(nrow(tbl(f)), 29L)
  expect_equal(nrow(collect(tbl(f))), 29L)
})

test_that("data-dependent verbs report NA rather than a wrong count", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_true(is.na(nrow(tbl(f) |> filter(cyl == 4))))
  expect_true(is.na(nrow(tbl(f) |> distinct(cyl))))
  expect_true(is.na(nrow(tbl(f) |> group_by(cyl) |> summarise(n = n()))))
  expect_true(is.na(nrow(tbl(f) |> count(cyl))))

  # ncol stays exact for all of them
  expect_equal(ncol(tbl(f) |> filter(cyl == 4)), ncol(mtcars))
  expect_equal(ncol(tbl(f) |> count(cyl)), 2L)
})

test_that("a filtered plan still counts once it is run", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  counted <- tbl(f) |> filter(cyl == 4) |> count() |> collect()
  expect_equal(counted$n, sum(mtcars$cyl == 4))
})

test_that("nrow formats into a message instead of vanishing", {
  # The reported bug: nrow() returned NULL, so sprintf() produced character(0)
  # and cat() printed nothing at all.
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_equal(sprintf("%d rows", nrow(tbl(f))), "32 rows")
  expect_equal(sprintf("%d rows", nrow(tbl(f) |> filter(cyl == 4))), "NA rows")
})

test_that("a source without a stored row count reports NA", {
  f <- tempfile(fileext = ".csv")
  on.exit(unlink(f), add = TRUE)
  write.csv(mtcars, f, row.names = FALSE)

  expect_true(is.na(nrow(tbl_csv(f))))
  expect_equal(ncol(tbl_csv(f)), ncol(mtcars))
})

test_that("glimpse shows the row count when it is known", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  write_vtr(mtcars, f)

  expect_output(glimpse(tbl(f)), "\\[32 x 11\\]")
  expect_output(glimpse(tbl(f) |> filter(cyl == 4)), "\\[\\? x 11\\]")
})

test_that("glimpse names each column type", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f), add = TRUE)
  df <- data.frame(a = 1:3, b = c(1.5, 2.5, 3.5),
                   s = c("x", "y", "z"), l = c(TRUE, FALSE, TRUE),
                   stringsAsFactors = FALSE)
  write_vtr(df, f)

  out <- capture.output(glimpse(tbl(f)))
  expect_false(any(grepl("<NA>", out, fixed = TRUE)))
  expect_true(any(grepl("^\\$ a\\s+<int64>", out)))
  expect_true(any(grepl("^\\$ b\\s+<double>", out)))
  expect_true(any(grepl("^\\$ s\\s+<string>", out)))
  expect_true(any(grepl("^\\$ l\\s+<bool>", out)))
})
