# Computed NaN (sqrt(-1), 0/0) is a valid double but must follow R's NaN
# semantics: comparisons are NA, is.na() is TRUE, and NaN keys group together.

test_that("comparisons involving a computed NaN are NA", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  # an NA elsewhere forces the per-element path
  write_vtr(data.frame(x = c(-1, 4, NA, 9)), f)
  yr <- suppressWarnings(sqrt(c(-1, 4, NA, 9)))
  r <- collect(mutate(tbl(f), eq = sqrt(x) == 2, lt = sqrt(x) < 3))
  expect_equal(r$eq, yr == 2)
  expect_equal(r$lt, yr < 3)
  # and on the all-valid fast path
  write_vtr(data.frame(x = c(-1, 4, 9)), f)
  expect_equal(collect(mutate(tbl(f), eq = sqrt(x) == 2))$eq,
               suppressWarnings(sqrt(c(-1, 4, 9))) == 2)
})

test_that("filter drops a computed-NaN comparison (NA)", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(-1, 0, 4)), f)  # sqrt(-1)=NaN, sqrt(0)=0
  expect_equal(nrow(collect(filter(tbl(f), sqrt(x) == 0))), 1L)
})

test_that("is.na() is TRUE for a computed NaN", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(-1, 4, NA)), f)
  expect_equal(collect(mutate(tbl(f), z = is.na(sqrt(x))))$z,
               is.na(suppressWarnings(sqrt(c(-1, 4, NA)))))
  expect_equal(nrow(collect(filter(tbl(f), !is.na(sqrt(x))))), 1L)
})

test_that("NaN group keys collapse into one group", {
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(data.frame(x = c(-1, -2, 4, -3), v = c(1, 1, 1, 1)), f)
  r <- collect(summarise(group_by(mutate(tbl(f), k = sqrt(x)), k), n = sum(v)))
  # three NaN keys -> one group of 3; sqrt(4)=2 -> group of 1
  expect_equal(sort(r$n), c(1, 3))
})
