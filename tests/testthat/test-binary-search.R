test_that("binary search narrows range on sorted int column", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  # Write sorted data with small batch size to create multiple row groups
  df <- data.frame(id = 1:1000, val = runif(1000))
  write_vtr(df, f, batch_size = 100L)

  # Point lookup should find the correct row
  result <- tbl(f) |> filter(id == 500) |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$id, 500)

  # Range lookup
  result <- tbl(f) |> filter(id >= 900) |> collect()
  expect_equal(nrow(result), 101L)
  expect_true(all(result$id >= 900))

  result <- tbl(f) |> filter(id <= 50) |> collect()
  expect_equal(nrow(result), 50L)
  expect_true(all(result$id <= 50))

  # Combined range (AND)
  result <- tbl(f) |> filter(id >= 200, id <= 300) |> collect()
  expect_equal(nrow(result), 101L)
  expect_true(all(result$id >= 200 & result$id <= 300))
})

test_that("binary search works on sorted string column", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  # Create sorted string data
  df <- data.frame(
    name = sort(paste0("name_", sprintf("%04d", 1:500))),
    val = seq_len(500),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 50L)

  result <- tbl(f) |> filter(name == "name_0250") |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$name, "name_0250")
})

test_that("binary search handles unsorted data correctly", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  # Write unsorted data — binary search should not engage
  df <- data.frame(id = sample(1:500), val = runif(500))
  write_vtr(df, f, batch_size = 50L)

  result <- tbl(f) |> filter(id == 100) |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$id, 100)
})

test_that("binary search handles single row group", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  df <- data.frame(id = 1:100, val = runif(100))
  write_vtr(df, f)

  result <- tbl(f) |> filter(id == 50) |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$id, 50)
})

test_that("binary search handles empty result", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  df <- data.frame(id = 1:1000, val = runif(1000))
  write_vtr(df, f, batch_size = 100L)

  result <- tbl(f) |> filter(id == 9999) |> collect()
  expect_equal(nrow(result), 0L)
})

test_that("binary search on double column", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  df <- data.frame(x = seq(0, 10, by = 0.01))
  write_vtr(df, f, batch_size = 100L)

  result <- tbl(f) |> filter(x == 5.0) |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$x, 5.0)

  result <- tbl(f) |> filter(x > 9.5) |> collect()
  expect_true(nrow(result) > 0)
  expect_true(all(result$x > 9.5))
})
