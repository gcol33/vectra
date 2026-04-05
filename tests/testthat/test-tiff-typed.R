test_that("int16 round-trip preserves values", {
  df <- data.frame(
    x = rep(seq(0.5, 4.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 5),
    band1 = as.double(1:15)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, compress = TRUE, pixel_type = "int16")
  result <- tbl_tiff(f) |> collect()

  expect_equal(nrow(result), 15)
  expect_equal(sort(result$band1), 1:15)
})

test_that("uint8 round-trip preserves 0-255 values", {
  df <- data.frame(
    x = rep(seq(0.5, 4.5, by = 1), 2),
    y = rep(c(1.5, 0.5), each = 5),
    band1 = as.double(c(0, 50, 100, 200, 255, 10, 20, 30, 40, 128))
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, pixel_type = "uint8")
  result <- tbl_tiff(f) |> collect()

  expect_equal(nrow(result), 10)
  # all values except nodata (255) should be present
  expect_true(all(c(0, 10, 20, 30, 40, 50, 100, 128, 200) %in% result$band1))
})

test_that("uint16 round-trip preserves values", {
  df <- data.frame(
    x = rep(seq(0.5, 2.5, by = 1), 2),
    y = rep(c(1.5, 0.5), each = 3),
    band1 = as.double(c(100, 1000, 30000, 200, 500, 60000))
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, pixel_type = "uint16")
  result <- tbl_tiff(f) |> collect()

  expect_equal(nrow(result), 6)
  expect_true(all(c(100, 1000, 30000, 200, 500) %in% result$band1))
})

test_that("float32 round-trip preserves approximate values", {
  df <- data.frame(
    x = rep(seq(0.5, 2.5, by = 1), 2),
    y = rep(c(1.5, 0.5), each = 3),
    band1 = c(1.5, 2.7, 3.14, -1.0, 0.0, 100.5)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, pixel_type = "float32")
  result <- tbl_tiff(f) |> collect()

  expect_equal(nrow(result), 6)
  expect_equal(sort(result$band1), sort(df$band1), tolerance = 1e-5)
})

test_that("int16 with NaN writes nodata and reads as NA", {
  df <- data.frame(
    x = rep(seq(0.5, 2.5, by = 1), 2),
    y = rep(c(1.5, 0.5), each = 3),
    band1 = c(10, NA, 30, 40, 50, NA)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, pixel_type = "int16")
  result <- tbl_tiff(f) |> collect()

  expect_equal(nrow(result), 6)
  # nodata pixels read as NA
  valid <- result[!is.na(result$band1), ]
  expect_true(all(c(10, 30, 40, 50) %in% valid$band1))
})

test_that("GDAL_METADATA write and read round-trip", {
  df <- data.frame(
    x = rep(seq(0.5, 2.5, by = 1), 2),
    y = rep(c(1.5, 0.5), each = 3),
    band1 = as.double(1:6)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  xml <- '<GDALMetadata>\n  <Item name="scale">0.01</Item>\n  <Item name="offset">-273.15</Item>\n</GDALMetadata>'
  write_tiff(df, f, pixel_type = "int16", metadata = xml)

  meta <- tiff_metadata(f)
  expect_false(is.na(meta))
  expect_true(grepl("scale", meta))
  expect_true(grepl("0.01", meta))
  expect_true(grepl("offset", meta))
  expect_true(grepl("-273.15", meta))
})

test_that("tiff_metadata returns NA for files without metadata", {
  df <- data.frame(
    x = c(0.5, 1.5), y = c(0.5, 0.5),
    band1 = c(1.0, 2.0)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f)
  meta <- tiff_metadata(f)
  expect_true(is.na(meta))
})

test_that("int16 point extraction returns correct values", {
  df <- data.frame(
    x = rep(seq(0.5, 4.5, by = 1), 3),
    y = rep(seq(2.5, 0.5, by = -1), each = 5),
    band1 = as.double(1:15)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, compress = TRUE, pixel_type = "int16")

  pts <- tiff_extract_points(f, x = c(0.5, 2.5, 4.5), y = c(2.5, 1.5, 0.5))
  expect_equal(nrow(pts), 3)
  expect_equal(pts$band1, c(1, 8, 15))
})

test_that("int32 round-trip with large values", {
  df <- data.frame(
    x = c(0.5, 1.5), y = c(0.5, 0.5),
    band1 = c(100000.0, -100000.0)
  )
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  write_tiff(df, f, pixel_type = "int32")
  result <- tbl_tiff(f) |> collect()
  expect_equal(sort(result$band1), c(-100000, 100000))
})

test_that("pixel_type validation rejects invalid types", {
  df <- data.frame(x = 0.5, y = 0.5, band1 = 1.0)
  f <- tempfile(fileext = ".tif")
  on.exit(unlink(f))

  expect_error(write_tiff(df, f, pixel_type = "int8"),
               "pixel_type must be one of")
})
