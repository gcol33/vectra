test_that("tbl_xlsx reads basic xlsx and returns correct data.frame", {
  skip_if_not_installed("openxlsx2")

  f <- tempfile(fileext = ".xlsx")
  on.exit(unlink(f))
  df <- data.frame(x = 1:5, y = c(10.1, 20.2, 30.3, 40.4, 50.5),
                   name = c("a", "b", "c", "d", "e"),
                   stringsAsFactors = FALSE)
  openxlsx2::write_xlsx(df, f)

  result <- tbl_xlsx(f) |> collect()
  expect_equal(nrow(result), 5)
  expect_equal(ncol(result), 3)
  expect_equal(names(result), c("x", "y", "name"))
  expect_equal(result$name, c("a", "b", "c", "d", "e"))
  expect_equal(result$y, c(10.1, 20.2, 30.3, 40.4, 50.5))
})

test_that("tbl_xlsx selects sheet by index", {
  skip_if_not_installed("openxlsx2")

  f <- tempfile(fileext = ".xlsx")
  on.exit(unlink(f))
  wb <- openxlsx2::wb_workbook()
  wb$add_worksheet("First")
  wb$add_data(sheet = "First", x = data.frame(a = 1:3))
  wb$add_worksheet("Second")
  wb$add_data(sheet = "Second", x = data.frame(b = 4:6))
  wb$save(f)

  result1 <- tbl_xlsx(f, sheet = 1L) |> collect()
  result2 <- tbl_xlsx(f, sheet = 2L) |> collect()

  expect_equal(names(result1), "a")
  expect_equal(names(result2), "b")
  expect_equal(result2$b, c(4, 5, 6))
})

test_that("tbl_xlsx selects sheet by name", {
  skip_if_not_installed("openxlsx2")

  f <- tempfile(fileext = ".xlsx")
  on.exit(unlink(f))
  wb <- openxlsx2::wb_workbook()
  wb$add_worksheet("Alpha")
  wb$add_data(sheet = "Alpha", x = data.frame(val = 10:12))
  wb$add_worksheet("Beta")
  wb$add_data(sheet = "Beta", x = data.frame(val = 20:22))
  wb$save(f)

  result <- tbl_xlsx(f, sheet = "Beta") |> collect()
  expect_equal(result$val, c(20, 21, 22))
})

test_that("tbl_xlsx errors on missing file", {
  skip_if_not_installed("openxlsx2")
  expect_error(tbl_xlsx(tempfile(fileext = ".xlsx")))
})

test_that("tbl_xlsx errors when openxlsx2 not available", {
  # Mock requireNamespace to return FALSE
  mockr <- function(...) FALSE
  with_mocked_bindings(
    {
      f <- tempfile(fileext = ".xlsx")
      expect_error(tbl_xlsx(f), "openxlsx2")
    },
    requireNamespace = mockr,
    .package = "base"
  )
})

test_that("tbl_xlsx validates path argument", {
  skip_if_not_installed("openxlsx2")
  expect_error(tbl_xlsx(123), "path must be a single character string")
  expect_error(tbl_xlsx(c("a.xlsx", "b.xlsx")),
               "path must be a single character string")
})

test_that("tbl_xlsx validates sheet argument", {
  skip_if_not_installed("openxlsx2")

  f <- tempfile(fileext = ".xlsx")
  on.exit(unlink(f))
  openxlsx2::write_xlsx(data.frame(x = 1), f)

  expect_error(tbl_xlsx(f, sheet = TRUE),
               "sheet must be a single character string or integer")
  expect_error(tbl_xlsx(f, sheet = c(1L, 2L)),
               "sheet must be a single character string or integer")
})

test_that("tbl_xlsx works with filter pipeline", {
  skip_if_not_installed("openxlsx2")

  f <- tempfile(fileext = ".xlsx")
  on.exit(unlink(f))
  df <- data.frame(x = 1:10, y = c(rep("a", 5), rep("b", 5)),
                   stringsAsFactors = FALSE)
  openxlsx2::write_xlsx(df, f)

  result <- tbl_xlsx(f) |>
    filter(x > 7) |>
    collect()

  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(8, 9, 10))
})
