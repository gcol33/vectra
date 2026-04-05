test_that("tiff_extract_points extracts correct values at pixel centers", {
  nx <- 3; ny <- 2
  df <- expand.grid(col = 0:(nx - 1), row = 0:(ny - 1))
  df$x <- 0.5 + df$col
  df$y <- (ny - 0.5) - df$row
  df$band1 <- c(10, 20, 30, 40, 50, 60)
  df <- df[, c("x", "y", "band1")]

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  # Query exact pixel centers
  pts <- data.frame(x = c(0.5, 1.5, 2.5), y = c(1.5, 1.5, 0.5))
  result <- tiff_extract_points(tmp, pts)

  expect_equal(nrow(result), 3)
  expect_equal(names(result), c("x", "y", "band1"))
  expect_equal(result$band1, c(10, 20, 60))
})

test_that("tiff_extract_points returns NA for out-of-bounds points", {
  df <- data.frame(x = c(0.5, 1.5), y = c(0.5, 0.5), band1 = c(1, 2))
  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  result <- tiff_extract_points(tmp,
    x = c(0.5, 99.0, -1.0),
    y = c(0.5, 0.5,   0.5))

  expect_equal(nrow(result), 3)
  expect_equal(result$band1[1], 1)
  expect_true(is.na(result$band1[2]))
  expect_true(is.na(result$band1[3]))
})

test_that("tiff_extract_points works with multi-band rasters", {
  df <- data.frame(
    x = c(0.5, 1.5, 0.5, 1.5),
    y = c(1.5, 1.5, 0.5, 0.5),
    band1 = c(1, 2, 3, 4),
    band2 = c(10, 20, 30, 40)
  )
  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  result <- tiff_extract_points(tmp, x = c(0.5, 1.5), y = c(1.5, 0.5))

  expect_equal(names(result), c("x", "y", "band1", "band2"))
  expect_equal(result$band1, c(1, 4))
  expect_equal(result$band2, c(10, 40))
})

test_that("tiff_extract_points accepts separate x, y vectors", {
  df <- data.frame(x = c(0.5, 1.5), y = c(0.5, 0.5), band1 = c(5, 6))
  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  result <- tiff_extract_points(tmp, x = 0.5, y = 0.5)
  expect_equal(nrow(result), 1)
  expect_equal(result$band1, 5)
})

test_that("tiff_extract_points validates inputs", {
  expect_error(tiff_extract_points(123, x = 1, y = 1),
               "path must be a single character string")

  tmp <- tempfile(fileext = ".tif")
  df <- data.frame(x = 0.5, y = 0.5, band1 = 1)
  write_tiff(df, tmp)
  on.exit(unlink(tmp))

  expect_error(tiff_extract_points(tmp, x = 1),
               "y is required")

  expect_error(tiff_extract_points(tmp, x = c(1, 2), y = 1),
               "same length")

  expect_error(tiff_extract_points(tmp, data.frame(a = 1, b = 2)),
               "must have columns")
})

test_that("tiff_extract_points with near-center offsets snaps to correct pixel", {
  df <- data.frame(
    x = c(0.5, 1.5, 2.5),
    y = c(0.5, 0.5, 0.5),
    band1 = c(100, 200, 300)
  )
  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  # Slightly off-center — should still snap to same pixel
  result <- tiff_extract_points(tmp, x = c(0.6, 1.4, 2.5), y = c(0.5, 0.5, 0.5))
  expect_equal(result$band1, c(100, 200, 300))
})
