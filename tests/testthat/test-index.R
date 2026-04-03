test_that("create_index and has_index work", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df <- data.frame(
    name = c("alice", "bob", "charlie", "diana", "eve"),
    val = 1:5,
    stringsAsFactors = FALSE
  )
  write_vtr(df, f)

  expect_false(has_index(f, "name"))
  create_index(f, "name")
  expect_true(has_index(f, "name"))
})

test_that("hash index accelerates equality lookups on strings", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".genus.vtri"))))

  df <- data.frame(
    genus = rep(c("Quercus", "Pinus", "Fagus", "Betula", "Acer"), each = 100),
    val = seq_len(500),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 100L)
  create_index(f, "genus")

  result <- tbl(f) |> filter(genus == "Pinus") |> collect()
  expect_equal(nrow(result), 100L)
  expect_true(all(result$genus == "Pinus"))
})

test_that("hash index works on integer columns", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".id.vtri"))))

  df <- data.frame(id = 1:500, val = runif(500))
  write_vtr(df, f, batch_size = 50L)
  create_index(f, "id")

  result <- tbl(f) |> filter(id == 250) |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$id, 250)
})

test_that("hash index with case-insensitive flag", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df <- data.frame(
    name = c("Alice", "BOB", "Charlie"),
    val = 1:3,
    stringsAsFactors = FALSE
  )
  write_vtr(df, f)
  create_index(f, "name", ci = TRUE)

  # Lookup should match case-insensitively via the hash index
  # (the index provides the row groups, then filter does exact matching)
  # Since the index is CI, "alice" hashes to the same bucket as "Alice"
  expect_true(has_index(f, "name"))
})

test_that("hash index handles empty result", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df <- data.frame(
    name = c("a", "b", "c"),
    val = 1:3,
    stringsAsFactors = FALSE
  )
  write_vtr(df, f)
  create_index(f, "name")

  result <- tbl(f) |> filter(name == "zzz") |> collect()
  expect_equal(nrow(result), 0L)
})

test_that("hash index with multiple row groups returns correct results", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".key.vtri"))))

  # Key values are spread across row groups
  df <- data.frame(
    key = rep(letters[1:10], times = 50),
    val = seq_len(500),
    stringsAsFactors = FALSE
  )
  write_vtr(df, f, batch_size = 100L)
  create_index(f, "key")

  result <- tbl(f) |> filter(key == "e") |> collect()
  expect_equal(nrow(result), 50L)
  expect_true(all(result$key == "e"))
})

test_that("create_index errors on non-existent column", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))

  write_vtr(data.frame(x = 1:10), f)
  expect_error(create_index(f, "nonexistent"), "not found")
})

test_that("index survives re-creation after data change", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, paste0(f, ".name.vtri"))))

  df1 <- data.frame(name = c("a", "b"), val = 1:2, stringsAsFactors = FALSE)
  write_vtr(df1, f)
  create_index(f, "name")

  # Overwrite with new data
  df2 <- data.frame(name = c("x", "y", "z"), val = 1:3, stringsAsFactors = FALSE)
  write_vtr(df2, f)

  # Old index is stale — re-create
  create_index(f, "name")

  result <- tbl(f) |> filter(name == "y") |> collect()
  expect_equal(nrow(result), 1L)
  expect_equal(result$name, "y")
})
