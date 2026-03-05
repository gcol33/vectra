# --- right_join ---

test_that("right_join keeps all right rows", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1.0, 2.0, 3.0), x = c(10.0, 20.0, 30.0)), f1)
  write_vtr(data.frame(id = c(2.0, 3.0, 4.0), y = c(200.0, 300.0, 400.0)), f2)
  result <- right_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 3)
  expect_true(all(c(2, 3, 4) %in% result$id))
  # id=4 should have NA for x
  expect_true(is.na(result$x[result$id == 4]))
  expect_equal(result$y[result$id == 2], 200)
})

test_that("right_join with named by", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(a = c(1.0, 2.0), x = c(10.0, 20.0)), f1)
  write_vtr(data.frame(b = c(2.0, 3.0), y = c(200.0, 300.0)), f2)
  result <- right_join(tbl(f1), tbl(f2), by = c("a" = "b")) |> collect()
  expect_equal(nrow(result), 2)
  expect_true("a" %in% names(result))
  expect_true("y" %in% names(result))
})

# --- full_join ---

test_that("full_join keeps all rows from both sides", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1.0, 2.0, 3.0), x = c(10.0, 20.0, 30.0)), f1)
  write_vtr(data.frame(id = c(2.0, 3.0, 4.0), y = c(200.0, 300.0, 400.0)), f2)
  result <- full_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 4)
  expect_true(all(c(1, 2, 3, 4) %in% result$id))
  # id=1 has NA y, id=4 has NA x
  expect_true(is.na(result$y[result$id == 1]))
  expect_true(is.na(result$x[result$id == 4]))
  # matching rows
  expect_equal(result$x[result$id == 2], 20)
  expect_equal(result$y[result$id == 2], 200)
})

test_that("full_join with no overlap", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1.0, 2.0), x = c(10.0, 20.0)), f1)
  write_vtr(data.frame(id = c(3.0, 4.0), y = c(30.0, 40.0)), f2)
  result <- full_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 4)
  expect_true(all(is.na(result$y[result$id %in% c(1, 2)])))
  expect_true(all(is.na(result$x[result$id %in% c(3, 4)])))
})

test_that("full_join with complete overlap", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(id = c(1.0, 2.0), x = c(10.0, 20.0)), f1)
  write_vtr(data.frame(id = c(1.0, 2.0), y = c(100.0, 200.0)), f2)
  result <- full_join(tbl(f1), tbl(f2), by = "id") |> collect()
  expect_equal(nrow(result), 2)
  expect_false(any(is.na(result$x)))
  expect_false(any(is.na(result$y)))
})

# --- bind_rows ---

test_that("bind_rows combines same-schema tables (streaming)", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0)), f1)
  write_vtr(data.frame(x = c(3.0, 4.0)), f2)
  node <- bind_rows(tbl(f1), tbl(f2))
  expect_s3_class(node, "vectra_node")
  result <- collect(node)
  expect_equal(nrow(result), 4)
  expect_equal(result$x, c(1, 2, 3, 4))
})

test_that("bind_rows fills missing columns with NA", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0), y = c(10.0, 20.0)), f1)
  write_vtr(data.frame(x = c(3.0, 4.0)), f2)
  result <- bind_rows(tbl(f1), tbl(f2))
  expect_equal(nrow(result), 4)
  expect_true(all(is.na(result$y[3:4])))
})

test_that("bind_rows with .id", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0)), f1)
  write_vtr(data.frame(x = c(3.0, 4.0)), f2)
  result <- bind_rows(tbl(f1), tbl(f2), .id = "source")
  expect_true("source" %in% names(result))
  expect_equal(result$source, c(1, 1, 2, 2))
})

test_that("streaming bind_rows composes with filter", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0, 3.0)), f1)
  write_vtr(data.frame(x = c(4.0, 5.0, 6.0)), f2)
  result <- bind_rows(tbl(f1), tbl(f2)) |> filter(x > 3) |> collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$x, c(4, 5, 6))
})

test_that("streaming bind_rows explain shows ConcatNode", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0)), f1)
  write_vtr(data.frame(x = c(3.0, 4.0)), f2)
  output <- capture.output(bind_rows(tbl(f1), tbl(f2)) |> explain())
  expect_true(any(grepl("ConcatNode", output)))
  expect_true(any(grepl("streaming", output)))
})

# --- bind_cols ---

test_that("bind_cols combines columns", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0)), f1)
  write_vtr(data.frame(y = c(10.0, 20.0)), f2)
  result <- bind_cols(tbl(f1), tbl(f2))
  expect_equal(names(result), c("x", "y"))
  expect_equal(nrow(result), 2)
})

test_that("bind_cols errors on row count mismatch", {
  f1 <- tempfile(fileext = ".vtr")
  f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = c(1.0, 2.0)), f1)
  write_vtr(data.frame(y = c(1.0, 2.0, 3.0)), f2)
  expect_error(bind_cols(tbl(f1), tbl(f2)))
})

# --- reframe ---

test_that("reframe allows variable-length output", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(g = c("a", "a", "b", "b", "b"),
                       x = c(1.0, 2.0, 3.0, 4.0, 5.0),
                       stringsAsFactors = FALSE), f)
  result <- tbl(f) |> group_by(g) |> reframe(vals = sort(x))
  expect_equal(nrow(result), 5)
  expect_true(all(c("g", "vals") %in% names(result)))
})

# --- explain ---

test_that("explain shows plan with node types", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(1.0, 2.0, 3.0)), f)
  output <- capture.output(tbl(f) |> filter(x > 1) |> explain())
  expect_true(any(grepl("ScanNode", output)))
  expect_true(any(grepl("FilterNode", output)))
  expect_true(any(grepl("streaming", output)))
})
