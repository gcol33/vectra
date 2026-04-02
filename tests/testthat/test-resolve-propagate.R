# --- resolve() tests ---

test_that("resolve() looks up string values via integer FK", {
  df <- data.frame(
    id = c(1L, 2L, 3L, 4L),
    name = c("Alpha", "Beta", "Gamma", "Delta"),
    parent_id = c(NA, 1L, 1L, 2L),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(parent_name = resolve(parent_id, id, name)) |>
    collect()

  expect_equal(result$parent_name, c(NA, "Alpha", "Alpha", "Beta"))
})

test_that("resolve() returns NA for missing FK values", {
  df <- data.frame(
    id = c(1L, 2L, 3L),
    name = c("A", "B", "C"),
    fk = c(2L, 99L, 1L),  # 99 doesn't exist
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(resolved = resolve(fk, id, name)) |>
    collect()

  expect_equal(result$resolved, c("B", NA, "A"))
})

test_that("resolve() returns NA when FK is NA", {
  df <- data.frame(
    id = c(1L, 2L, 3L),
    value = c(10.0, 20.0, 30.0),
    fk = c(NA, 1L, 2L)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(resolved = resolve(fk, id, value)) |>
    collect()

  expect_equal(result$resolved, c(NA, 10.0, 20.0))
})

test_that("resolve() works with numeric columns", {
  df <- data.frame(
    id = c(1L, 2L, 3L),
    score = c(100.0, 200.0, 300.0),
    ref = c(3L, 1L, 2L)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(ref_score = resolve(ref, id, score)) |>
    collect()

  expect_equal(result$ref_score, c(300.0, 100.0, 200.0))
})

test_that("resolve() works with string FK and PK", {
  df <- data.frame(
    code = c("A", "B", "C"),
    label = c("Alpha", "Beta", "Gamma"),
    parent_code = c(NA, "A", "B"),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(parent_label = resolve(parent_code, code, label)) |>
    collect()

  expect_equal(result$parent_label, c(NA, "Alpha", "Beta"))
})

# --- propagate() tests ---

test_that("propagate() fills children from parent values", {
  # Tree: 1(root, rank=FAMILY) -> 2(child) -> 3(grandchild)
  df <- data.frame(
    id = c(1L, 2L, 3L),
    parent_id = c(NA, 1L, 2L),
    rank = c("FAMILY", "GENUS", "SPECIES"),
    name = c("Fagaceae", "Quercus", "Quercus robur"),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(family = propagate(
      parent_id, id,
      ifelse(rank == "FAMILY", name, NA)
    )) |>
    collect()

  expect_equal(result$family, c("Fagaceae", "Fagaceae", "Fagaceae"))
})

test_that("propagate() handles disconnected trees", {
  df <- data.frame(
    id = c(1L, 2L, 3L, 4L, 5L),
    parent_id = c(NA, 1L, 2L, NA, 4L),
    rank = c("FAMILY", "GENUS", "SPECIES", "FAMILY", "GENUS"),
    name = c("Fagaceae", "Quercus", "Q. robur", "Rosaceae", "Rosa"),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(family = propagate(
      parent_id, id,
      ifelse(rank == "FAMILY", name, NA)
    )) |>
    collect()

  expect_equal(result$family, c("Fagaceae", "Fagaceae", "Fagaceae",
                                 "Rosaceae", "Rosaceae"))
})

test_that("propagate() returns NA for orphan nodes", {
  # Node 3 has parent 99 which doesn't exist
  df <- data.frame(
    id = c(1L, 2L, 3L),
    parent_id = c(NA, 1L, 99L),
    rank = c("FAMILY", "GENUS", "SPECIES"),
    name = c("Fagaceae", "Quercus", "Unknown"),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(family = propagate(
      parent_id, id,
      ifelse(rank == "FAMILY", name, NA)
    )) |>
    collect()

  expect_equal(result$family, c("Fagaceae", "Fagaceae", NA))
})

test_that("propagate() works with numeric seed values", {
  df <- data.frame(
    id = c(1L, 2L, 3L, 4L),
    parent_id = c(NA, 1L, 2L, 1L),
    is_root = c(TRUE, FALSE, FALSE, FALSE),
    weight = c(42.0, NA, NA, NA)
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(root_weight = propagate(
      parent_id, id,
      ifelse(is_root, weight, NA)
    )) |>
    collect()

  expect_equal(result$root_weight, c(42.0, 42.0, 42.0, 42.0))
})

test_that("propagate() converges for deep trees", {
  # Chain: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9 -> 10
  n <- 10L
  df <- data.frame(
    id = seq_len(n),
    parent_id = c(NA, seq_len(n - 1L)),
    rank = c("ROOT", rep("CHILD", n - 1L)),
    name = c("TopValue", paste0("node", 2:n)),
    stringsAsFactors = FALSE
  )
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)

  result <- tbl(f) |>
    mutate(root_name = propagate(
      parent_id, id,
      ifelse(rank == "ROOT", name, NA)
    )) |>
    collect()

  expect_equal(result$root_name, rep("TopValue", n))
})
