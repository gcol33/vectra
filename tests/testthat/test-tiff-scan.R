test_that("tiff round-trip: write then read back", {
  # Create a small test raster as a data.frame with x, y, band1
  nx <- 4; ny <- 3
  xres <- 0.5; yres <- 0.5
  xmin <- 10; ymax <- 50

  df <- expand.grid(
    col = seq_len(nx) - 1,
    row = seq_len(ny) - 1
  )
  df$x <- xmin + (df$col + 0.5) * xres
  df$y <- ymax - (df$row + 0.5) * yres
  df$band1 <- as.double(seq_len(nrow(df)))
  df <- df[, c("x", "y", "band1")]

  # Write via write_tiff
  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))

  vtr_tmp <- tempfile(fileext = ".vtr")
  on.exit(unlink(vtr_tmp), add = TRUE)
  write_vtr(df, vtr_tmp)
  tbl(vtr_tmp) |> write_tiff(tmp)

  # Read back via tbl_tiff
  node <- tbl_tiff(tmp)
  result <- node |> collect()

  expect_equal(ncol(result), 3)
  expect_true("x" %in% names(result))
  expect_true("y" %in% names(result))
  expect_true("band1" %in% names(result))
  expect_equal(nrow(result), nx * ny)
})

test_that("tiff round-trip preserves values", {
  nx <- 3; ny <- 2
  xres <- 1; yres <- 1

  df <- expand.grid(col = 0:(nx-1), row = 0:(ny-1))
  df$x <- 0.5 + df$col * xres
  df$y <- (ny - 0.5) - df$row * yres
  df$band1 <- c(1.5, 2.5, 3.5, 4.5, 5.5, 6.5)
  df <- df[, c("x", "y", "band1")]

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))

  vtr_tmp <- tempfile(fileext = ".vtr")
  on.exit(unlink(vtr_tmp), add = TRUE)
  write_vtr(df, vtr_tmp)
  tbl(vtr_tmp) |> write_tiff(tmp)

  result <- tbl_tiff(tmp) |> collect()

  # Sort both by x then y for comparison
  df_sorted <- df[order(df$y, df$x), ]
  res_sorted <- result[order(result$y, result$x), ]

  expect_equal(res_sorted$band1, df_sorted$band1, tolerance = 1e-10)
})

test_that("tiff filter works (extent crop)", {
  nx <- 4; ny <- 4
  df <- expand.grid(col = 0:(nx-1), row = 0:(ny-1))
  df$x <- 0.5 + df$col
  df$y <- (ny - 0.5) - df$row
  df$band1 <- as.double(seq_len(nrow(df)))
  df <- df[, c("x", "y", "band1")]

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))

  vtr_tmp <- tempfile(fileext = ".vtr")
  on.exit(unlink(vtr_tmp), add = TRUE)
  write_vtr(df, vtr_tmp)
  tbl(vtr_tmp) |> write_tiff(tmp)

  # Crop to x >= 1.5 (should get cols 1,2,3 = 3 cols)
  result <- tbl_tiff(tmp) |>
    filter(x >= 1.5) |>
    collect()

  expect_true(all(result$x >= 1.5))
  expect_equal(nrow(result), 3 * ny)
})

test_that("tiff multi-band round-trip", {
  nx <- 3; ny <- 2
  df <- expand.grid(col = 0:(nx-1), row = 0:(ny-1))
  df$x <- 0.5 + df$col
  df$y <- (ny - 0.5) - df$row
  df$band1 <- as.double(1:6)
  df$band2 <- as.double(7:12)
  df <- df[, c("x", "y", "band1", "band2")]

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  result <- tbl_tiff(tmp) |> collect()

  expect_equal(ncol(result), 4)
  expect_true("band1" %in% names(result))
  expect_true("band2" %in% names(result))
  expect_equal(nrow(result), 6)
})

test_that("tiff explain shows TiffScanNode", {
  nx <- 2; ny <- 2
  df <- data.frame(x = c(0.5, 1.5, 0.5, 1.5),
                   y = c(1.5, 1.5, 0.5, 0.5),
                   band1 = c(1, 2, 3, 4))

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  node <- tbl_tiff(tmp)
  out <- capture.output(explain(node))
  expect_true(any(grepl("TiffScanNode", out)))
})

test_that("tbl_tiff returns metadata", {
  df <- data.frame(x = c(0.5, 1.5, 0.5, 1.5),
                   y = c(1.5, 1.5, 0.5, 0.5),
                   band1 = c(10, 20, 30, 40))

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp)

  node <- tbl_tiff(tmp)
  expect_true(!is.null(node$.tiff_meta))
  expect_equal(node$.tiff_meta$width, 2)
  expect_equal(node$.tiff_meta$height, 2)
  expect_equal(node$.tiff_meta$nbands, 1L)
})

test_that("write_tiff with deflate compression", {
  df <- data.frame(x = c(0.5, 1.5, 0.5, 1.5),
                   y = c(1.5, 1.5, 0.5, 0.5),
                   band1 = c(1, 2, 3, 4))

  tmp <- tempfile(fileext = ".tif")
  on.exit(unlink(tmp))
  write_tiff(df, tmp, compress = TRUE)

  result <- tbl_tiff(tmp) |> collect()
  expect_equal(nrow(result), 4)

  res_sorted <- result[order(result$y, result$x), ]
  df_sorted <- df[order(df$y, df$x), ]
  expect_equal(res_sorted$band1, df_sorted$band1)
})
