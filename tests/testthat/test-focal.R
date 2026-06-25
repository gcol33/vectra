# focal() -- streamed moving-window statistics over a VECR raster, checked
# against hand-computed windows, terra::focal ground truth, and the resident
# (single-strip) path. The streamed strip pass must equal the resident result
# exactly: divergence there is a bug, not "more accurate".

write_raster <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L,
                         dtype = "f64") {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = dtype, extent = ext, tile_size = tile)
  f
}

test_that("focal recovers a hand-computed 3x3 window", {
  m <- matrix(1:25, 5, 5, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))

  s <- focal(f, w = matrix(1, 3, 3), fun = "sum", na.rm = TRUE)
  # interior cell (3,3) value 13: 3x3 block sums to 9 * 13 = 117 (linear grid)
  expect_equal(s[3, 3], sum(m[2:4, 2:4]))
  expect_equal(s[3, 3], 117)
  mn <- focal(f, w = matrix(1, 3, 3), fun = "mean", na.rm = TRUE)
  expect_equal(mn[3, 3], mean(m[2:4, 2:4]))
  # corner (1,1) under na.rm = TRUE sees only the in-raster 2x2 block
  expect_equal(s[1, 1], sum(m[1:2, 1:2]))
})

test_that("na.rm = FALSE makes edge windows NA, na.rm = TRUE fills them", {
  m <- matrix(1:25, 5, 5, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))

  keep <- focal(f, w = matrix(1, 3, 3), fun = "mean", na.rm = FALSE)
  expect_true(is.na(keep[1, 1]))                  # window runs off the edge
  expect_false(is.na(keep[3, 3]))                 # fully interior
  fill <- focal(f, w = matrix(1, 3, 3), fun = "mean", na.rm = TRUE)
  expect_false(is.na(fill[1, 1]))
  expect_equal(fill[3, 3], keep[3, 3])            # interior identical
})

test_that("weights scale sum and NA weights drop cells", {
  m <- matrix(1:25, 5, 5, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))

  w <- matrix(c(0, 1, 0, 1, 1, 1, 0, 1, 0), 3, 3, byrow = TRUE)  # plus-shape
  s <- focal(f, w = w, fun = "sum", na.rm = TRUE)
  # cell (3,3)=13: plus neighbours are 8,12,13,14,18
  expect_equal(s[3, 3], 8 + 12 + 13 + 14 + 18)

  wna <- w; wna[wna == 0] <- NA                   # NA weight == out of window
  sna <- focal(f, w = wna, fun = "sum", na.rm = TRUE)
  expect_equal(sna[3, 3], s[3, 3])

  w2 <- matrix(2, 3, 3)
  expect_equal(focal(f, w = w2, fun = "sum")[3, 3],
               2 * sum(m[2:4, 2:4]))
})

test_that("a single odd integer is shorthand for a square window", {
  m <- matrix(1:25, 5, 5, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))
  a <- focal(f, w = 3, fun = "sum")
  b <- focal(f, w = matrix(1, 3, 3), fun = "sum")
  expect_equal(a, b)
})

test_that("streamed strips equal the single-strip result", {
  set.seed(41)
  m <- matrix(rnorm(37 * 29), 37, 29)
  f_s <- write_raster(m, tile = 8L)               # many tile-rows
  f_1 <- write_raster(m, tile = 512L)             # a single strip
  on.exit(unlink(c(f_s, f_1)))

  for (fn in c("sum", "mean", "min", "max", "sd", "median")) {
    a <- focal(f_s, w = matrix(1, 3, 3), fun = fn, na.rm = TRUE)
    b <- focal(f_1, w = matrix(1, 3, 3), fun = fn, na.rm = TRUE)
    expect_equal(a, b, tolerance = 1e-12, info = fn)
  }
  # a 5x3 window crosses more than one tile-row halo
  a <- focal(f_s, w = matrix(1, 5, 3), fun = "mean")
  b <- focal(f_1, w = matrix(1, 5, 3), fun = "mean")
  expect_equal(a, b, tolerance = 1e-12)
})

test_that("streamed-to-.vec output equals the in-memory result", {
  set.seed(42)
  m <- matrix(rnorm(20 * 16), 20, 16)
  f <- write_raster(m, tile = 8L); on.exit(unlink(f))
  mem <- focal(f, w = matrix(1, 3, 3), fun = "mean", na.rm = TRUE)

  of <- tempfile(fileext = ".vec"); on.exit(unlink(of), add = TRUE)
  h <- focal(f, w = matrix(1, 3, 3), fun = "mean", na.rm = TRUE,
             path = of, dtype = "f64")
  on.exit(vec_close_raster(h), add = TRUE)
  disk <- vec_read_window(h, band = 1)
  expect_equal(max(abs(mem - disk), na.rm = TRUE), 0)
  expect_true(all(is.na(mem) == is.na(disk)))
})

test_that("focal matches terra::focal", {
  skip_if_not_installed("terra")
  set.seed(43)
  m <- matrix(rnorm(40 * 32), 40, 32)
  f <- write_raster(m, tile = 16L); on.exit(unlink(f))
  r <- terra::rast(m, extent = terra::ext(0, 32, 0, 40))

  for (fn in c("sum", "mean", "min", "max", "sd")) {
    got <- focal(f, w = matrix(1, 3, 3), fun = fn, na.rm = TRUE)
    ter <- matrix(terra::values(terra::focal(r, w = matrix(1, 3, 3),
                                             fun = fn, na.rm = TRUE)),
                  nrow = 40, byrow = TRUE)
    expect_equal(got, ter, tolerance = 1e-9, ignore_attr = TRUE, info = fn)
  }
  # na.rm = FALSE: both leave the border NA
  got <- focal(f, w = matrix(1, 3, 3), fun = "mean", na.rm = FALSE)
  ter <- matrix(terra::values(terra::focal(r, w = matrix(1, 3, 3),
                                           fun = "mean", na.rm = FALSE)),
                nrow = 40, byrow = TRUE)
  expect_equal(got, ter, tolerance = 1e-9, ignore_attr = TRUE)
  expect_true(all(is.na(got) == is.na(ter)))
})

test_that("a vectra_raster handle works as well as a path", {
  m <- matrix(1:16, 4, 4, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))
  rr <- vec_open_raster(f); on.exit(vec_close_raster(rr), add = TRUE)
  expect_equal(focal(rr, w = matrix(1, 3, 3), fun = "sum"),
               focal(f,  w = matrix(1, 3, 3), fun = "sum"))
})

test_that("focal validates inputs", {
  m <- matrix(1:16, 4, 4)
  f <- write_raster(m); on.exit(unlink(f))
  expect_error(focal(f, w = matrix(1, 2, 2)), "odd")
  expect_error(focal(f, w = 4), "odd")
  expect_error(focal(f, fun = "bogus"), "should be one of|arg")
})
