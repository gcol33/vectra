# Spill-safe holistic aggregates: median() and n_distinct() keep bounded memory
# by spilling per-group values to run files past the memory budget, then reduce
# exactly by external merge. A tiny vectra.memory forces that spill path; the
# results must match the in-RAM path and base R exactly.

make_vtr <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

test_that("median matches base R across in-RAM and spilled paths", {
  set.seed(42)
  n <- 40000
  df <- data.frame(
    g = sample(1:23, n, replace = TRUE),
    x = sample(rnorm(400), n, replace = TRUE)  # heavy ties
  )
  f <- make_vtr(df); on.exit(unlink(f))

  ref <- tapply(df$x, df$g, median)
  ref <- ref[order(as.integer(names(ref)))]

  for (mem in list("4GB", 4096)) {           # in-RAM, then forced spill
    old <- options(vectra.memory = mem)
    got <- tbl(f) |> group_by(g) |>
      summarise(m = median(x, na.rm = TRUE)) |> collect()
    got <- got[order(got$g), ]
    options(old)
    expect_equal(got$m, as.numeric(ref), tolerance = 1e-9,
                 info = paste("mem =", mem))
  }
})

test_that("n_distinct matches base R across in-RAM and spilled paths", {
  set.seed(7)
  n <- 40000
  df <- data.frame(
    g = sample(1:23, n, replace = TRUE),
    x = sample(1:500, n, replace = TRUE)
  )
  f <- make_vtr(df); on.exit(unlink(f))

  ref <- tapply(df$x, df$g, function(v) length(unique(v)))
  ref <- ref[order(as.integer(names(ref)))]

  for (mem in list("4GB", 4096)) {
    old <- options(vectra.memory = mem)
    got <- tbl(f) |> group_by(g) |>
      summarise(d = n_distinct(x)) |> collect()
    got <- got[order(got$g), ]
    options(old)
    expect_equal(got$d, as.numeric(ref), info = paste("mem =", mem))
  }
})

test_that("spilled median is exact for odd and even group sizes", {
  df <- data.frame(g = c(rep("odd", 5), rep("even", 4)),
                   x = c(1, 2, 3, 4, 5, 10, 20, 30, 40))
  f <- make_vtr(df); on.exit(unlink(f))

  old <- options(vectra.memory = 4096); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> group_by(g) |> summarise(m = median(x)) |> collect()
  expect_equal(got$m[got$g == "odd"], 3)    # middle of 1..5
  expect_equal(got$m[got$g == "even"], 25)  # mean(20, 30)
})

test_that("multiple holistic aggregates in one summarise stay correct when spilling", {
  set.seed(99)
  n <- 30000
  df <- data.frame(
    g = sample(1:11, n, replace = TRUE),
    x = sample(rnorm(300), n, replace = TRUE)
  )
  f <- make_vtr(df); on.exit(unlink(f))

  old <- options(vectra.memory = 4096); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> group_by(g) |>
    summarise(m = median(x, na.rm = TRUE), d = n_distinct(x), nn = n()) |>
    collect()
  got <- got[order(got$g), ]

  med <- tapply(df$x, df$g, median); med <- med[order(as.integer(names(med)))]
  nd  <- tapply(df$x, df$g, function(v) length(unique(v)))
  nd  <- nd[order(as.integer(names(nd)))]
  cnt <- tapply(df$x, df$g, length); cnt <- cnt[order(as.integer(names(cnt)))]

  expect_equal(got$m, as.numeric(med), tolerance = 1e-9)
  expect_equal(got$d, as.numeric(nd))
  expect_equal(got$nn, as.numeric(cnt))
})

test_that("ungrouped median and n_distinct spill correctly", {
  set.seed(3)
  x <- sample(rnorm(500), 50000, replace = TRUE)
  df <- data.frame(x = x)
  f <- make_vtr(df); on.exit(unlink(f))

  old <- options(vectra.memory = 4096); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> summarise(m = median(x), d = n_distinct(x)) |> collect()
  expect_equal(got$m, median(x), tolerance = 1e-9)
  expect_equal(got$d, length(unique(x)))
})
