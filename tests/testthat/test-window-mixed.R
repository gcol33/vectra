# Ungrouped mutate() combining window functions of different ordering classes
# (cumulative = arrival order, rank = sort by value, lead = reverse order) used
# to fall back to materializing the whole table. It now decomposes into a chain
# of single-spec streaming window nodes. Each combined column must equal the
# same window computed on its own, with the original column and row order kept.

mixed_frame <- function(n = 20000L, seed = 5) {
  set.seed(seed)
  data.frame(id = seq_len(n),
             x = sample.int(500L, n, replace = TRUE),
             y = rnorm(n),
             z = runif(n),
             w = sample.int(9L, n, replace = TRUE),
             v = rnorm(n))
}

test_that("mixed-ordering ungrouped windows equal per-window computation", {
  d <- mixed_frame()
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 512L)          # many row groups: exercises streaming

  got <- tbl(f) |>
    mutate(a = cumsum(w),          # NATURAL (arrival order)
           b = rank(x),            # BY_INPUT (sort by x)
           c = rank(desc(y)),      # BY_INPUT (sort by y, descending) -- differs from b
           e = lag(z, 1),          # NATURAL
           g = lead(v, 1)) |>      # REVERSE
    collect()

  # Column order: original columns, then the window columns in mutate() order.
  expect_equal(names(got), c(names(d), "a", "b", "c", "e", "g"))
  expect_identical(as.integer(got$id), d$id)   # original row order preserved

  # Each window column equals the same window computed on its own.
  a1 <- tbl(f) |> mutate(a = cumsum(w))       |> collect()
  b1 <- tbl(f) |> mutate(b = rank(x))         |> collect()
  c1 <- tbl(f) |> mutate(c = rank(desc(y)))   |> collect()
  e1 <- tbl(f) |> mutate(e = lag(z, 1))       |> collect()
  g1 <- tbl(f) |> mutate(g = lead(v, 1))      |> collect()
  expect_equal(got$a, a1$a)
  expect_equal(got$b, b1$b)
  expect_equal(got$c, c1$c)
  expect_equal(got$e, e1$e)
  expect_equal(got$g, g1$g)
})

test_that("two ranks on different columns in one mutate stay independent", {
  d <- mixed_frame(n = 8000L, seed = 6)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 300L)

  got <- tbl(f) |> mutate(rx = row_number(x), ry = row_number(desc(y))) |> collect()
  rx1 <- tbl(f) |> mutate(rx = row_number(x)) |> collect()
  ry1 <- tbl(f) |> mutate(ry = row_number(desc(y))) |> collect()
  expect_equal(got$rx, rx1$rx)
  expect_equal(got$ry, ry1$ry)
  expect_identical(as.integer(got$id), d$id)
})

test_that("mixed windows including a rolling aggregate decompose correctly", {
  set.seed(8)
  n <- 6000L
  d <- data.frame(id = seq_len(n),
                  t = as.POSIXct("2020-01-01", tz = "UTC") + sort(sample.int(1e6L, n)),
                  val = rnorm(n),
                  grp = sample.int(50L, n, replace = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 256L)

  got <- tbl(f) |>
    mutate(cs = cumsum(val),                       # NATURAL
           rm = roll_mean(val, t, "1 hour"),       # BY_ORDER (time)
           rk = rank(val)) |>                      # BY_INPUT (value)
    collect()
  cs1 <- tbl(f) |> mutate(cs = cumsum(val)) |> collect()
  rm1 <- tbl(f) |> mutate(rm = roll_mean(val, t, "1 hour")) |> collect()
  rk1 <- tbl(f) |> mutate(rk = rank(val)) |> collect()
  expect_equal(got$cs, cs1$cs)
  expect_equal(got$rm, rm1$rm)
  expect_equal(got$rk, rk1$rk)
  expect_identical(as.integer(got$id), d$id)
})
