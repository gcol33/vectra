# Recovery and streaming-invariance tests for rasterize().
#
# The reference grid is an independent, vectorised binning of the points in R
# (tapply over cell keys); the streamed C accumulator must reproduce it exactly.
# A separate terra cell-for-cell check guards the geotransform convention, and a
# multi-row-group scan proves the streamed result equals the single-batch one.

# Independent reference: bin points into an nr x nc grid (row 1 = north).
ref_grid <- function(pts, ext, nr, nc, field = NULL, fun = "count",
                     bg = NA_real_) {
  xmin <- ext[1]; xmax <- ext[3]; ymax <- ext[4]
  xres <- (xmax - xmin) / nc
  yres <- (ext[4] - ext[2]) / nr
  col <- floor((pts$x - xmin) / xres)      # 0-based, west->east
  row <- floor((ymax - pts$y) / yres)      # 0-based, north->south
  inb <- col >= 0 & col < nc & row >= 0 & row < nr
  m <- matrix(bg, nr, nc)
  idx <- which(inb)
  if (!length(idx)) return(m)
  rr <- row[idx] + 1L; cc <- col[idx] + 1L
  key <- (cc - 1L) * nr + rr               # column-major matrix index
  if (is.null(field)) {
    agg <- tapply(rep(1L, length(key)), key, sum)
  } else {
    v <- pts[[field]][idx]
    keep <- !is.na(v)
    key <- key[keep]; v <- v[keep]
    fn <- switch(fun, sum = sum, mean = mean, min = min, max = max)
    agg <- tapply(v, key, fn)
  }
  m[as.integer(names(agg))] <- as.numeric(agg)
  m
}

vtr_points <- function(pts, batch_size = NULL) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(pts, f, batch_size = batch_size)
  f
}

test_that("count recovers a hand-binned grid", {
  set.seed(11)
  n <- 3000
  pts <- data.frame(x = runif(n, 0, 10), y = runif(n, 0, 10))
  f <- vtr_points(pts); on.exit(unlink(f))

  got <- tbl(f) |> rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10))
  ref <- ref_grid(pts, c(0, 0, 10, 10), 10, 10)

  expect_equal(dim(got), c(10L, 10L))
  expect_equal(got, ref, ignore_attr = TRUE)
  expect_equal(sum(got), n)               # every point lands in a cell
})

test_that("sum, mean, min, max recover the binned field", {
  set.seed(12)
  n <- 4000
  pts <- data.frame(x = runif(n, 0, 8), y = runif(n, 0, 8), z = rnorm(n))
  f <- vtr_points(pts); on.exit(unlink(f))
  ext <- c(0, 0, 8, 8)

  for (fn in c("sum", "mean", "min", "max")) {
    got <- tbl(f) |>
      rasterize(extent = ext, dims = c(8, 8), field = "z", fun = fn)
    ref <- ref_grid(pts, ext, 8, 8, field = "z", fun = fn)
    expect_equal(got, ref, ignore_attr = TRUE, info = fn)
  }
})

test_that("NA field values are skipped (na.rm)", {
  pts <- data.frame(x = c(0.5, 0.5, 0.5), y = c(0.5, 0.5, 0.5),
                    z = c(1, NA, 3))
  f <- vtr_points(pts); on.exit(unlink(f))
  # single 1x1 cell over [0,1]^2
  s <- tbl(f) |> rasterize(extent = c(0, 0, 1, 1), dims = c(1, 1),
                           field = "z", fun = "sum")
  m <- tbl(f) |> rasterize(extent = c(0, 0, 1, 1), dims = c(1, 1),
                           field = "z", fun = "mean")
  cnt <- tbl(f) |> rasterize(extent = c(0, 0, 1, 1), dims = c(1, 1))
  expect_equal(as.numeric(s), 4)          # 1 + 3
  expect_equal(as.numeric(m), 2)          # mean of 1, 3
  expect_equal(as.numeric(cnt), 3)        # count tallies all points
})

test_that("empty cells take the background value", {
  pts <- data.frame(x = 0.5, y = 0.5)     # x west, y south -> bottom-left cell
  f <- vtr_points(pts); on.exit(unlink(f))
  got <- tbl(f) |> rasterize(extent = c(0, 0, 2, 2), dims = c(2, 2),
                             background = -1)
  expect_equal(got[2, 1], 1)              # row 2 = southern row got the point
  expect_equal(sum(got == -1), 3L)        # the other three are background
})

test_that("streamed multi-batch equals single-batch", {
  set.seed(13)
  n <- 5000
  pts <- data.frame(x = runif(n, 0, 20), y = runif(n, 0, 20), z = rnorm(n))
  f1 <- vtr_points(pts)                          # default one row group
  f2 <- vtr_points(pts, batch_size = 211)        # many small row groups
  on.exit(unlink(c(f1, f2)))

  for (fn in c("count", "sum", "mean", "min", "max")) {
    fld <- if (fn == "count") NULL else "z"
    a <- tbl(f1) |> rasterize(extent = c(0, 0, 20, 20), dims = c(16, 16),
                              field = fld, fun = fn)
    b <- tbl(f2) |> rasterize(extent = c(0, 0, 20, 20), dims = c(16, 16),
                              field = fld, fun = fn)
    expect_equal(a, b, ignore_attr = TRUE, info = fn)
  }
})

test_that("res and dims describe the same grid", {
  set.seed(14)
  pts <- data.frame(x = runif(500, 0, 10), y = runif(500, 0, 10))
  f <- vtr_points(pts); on.exit(unlink(f))
  by_dims <- tbl(f) |> rasterize(extent = c(0, 0, 10, 10), dims = c(20, 20))
  by_res  <- tbl(f) |> rasterize(extent = c(0, 0, 10, 10), res = 0.5)
  expect_equal(dim(by_res), c(20L, 20L))
  expect_equal(by_dims, by_res, ignore_attr = TRUE)
})

test_that("geom (sf point) input matches coords input", {
  skip_if_not_installed("sf")
  set.seed(15)
  n <- 1500
  pts <- data.frame(x = runif(n, 0, 10), y = runif(n, 0, 10), z = rnorm(n))

  f_xy <- vtr_points(pts)
  g <- sf::st_as_binary(
    sf::st_geometry(sf::st_as_sf(pts, coords = c("x", "y"))), hex = TRUE)
  f_geom <- vtr_points(data.frame(z = pts$z, geometry = g, stringsAsFactors = FALSE))
  on.exit(unlink(c(f_xy, f_geom)))

  a <- tbl(f_xy)  |> rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10),
                               field = "z", fun = "mean")
  b <- tbl(f_geom) |> rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10),
                                geom = "geometry", field = "z", fun = "mean")
  expect_equal(a, b, ignore_attr = TRUE)
})

test_that("rasterize matches terra::rasterize cell-for-cell", {
  skip_if_not_installed("terra")
  set.seed(16)
  n <- 4000
  pts <- data.frame(x = runif(n, 0, 12), y = runif(n, 0, 12), z = rnorm(n))
  f <- vtr_points(pts); on.exit(unlink(f))

  tmpl <- terra::rast(nrows = 12, ncols = 12,
                      xmin = 0, xmax = 12, ymin = 0, ymax = 12)
  vt <- terra::vect(pts, geom = c("x", "y"))

  # count
  got_c <- tbl(f) |> rasterize(extent = c(0, 0, 12, 12), dims = c(12, 12))
  ter_c <- terra::as.matrix(terra::rasterize(vt, tmpl, fun = "length"),
                            wide = TRUE)
  # terra leaves empty cells NA; rasterize() background is NA too.
  expect_true(all((is.na(ter_c) & is.na(got_c)) |
                  (ter_c == got_c)))

  # mean of z
  got_m <- tbl(f) |> rasterize(extent = c(0, 0, 12, 12), dims = c(12, 12),
                               field = "z", fun = "mean")
  ter_m <- terra::as.matrix(terra::rasterize(vt, tmpl, field = "z",
                                             fun = "mean"), wide = TRUE)
  both <- !is.na(ter_m) & !is.na(got_m)
  expect_true(all(is.na(ter_m) == is.na(got_m)))
  expect_equal(got_m[both], ter_m[both], tolerance = 1e-9)
})

test_that("path output writes a readable .vec raster", {
  set.seed(17)
  pts <- data.frame(x = runif(2000, 0, 10), y = runif(2000, 0, 10))
  f <- vtr_points(pts); on.exit(unlink(f))
  out <- tempfile(fileext = ".vec"); on.exit(unlink(out), add = TRUE)

  r <- tbl(f) |> rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10),
                           path = out, dtype = "f64")
  expect_s3_class(r, "vectra_raster")
  expect_equal(r$width, 10L)
  expect_equal(r$height, 10L)

  mem <- ref_grid(pts, c(0, 0, 10, 10), 10, 10)
  disk <- vec_read_window(r)
  # empty cells: NA in memory, NaN->NA on read; both NA
  both <- !is.na(mem) & !is.na(disk)
  expect_equal(disk[both], mem[both], tolerance = 1e-9)
  vec_close_raster(r)
})

test_that("input validation", {
  pts <- data.frame(x = runif(10), y = runif(10))
  f <- vtr_points(pts); on.exit(unlink(f))
  expect_error(rasterize(pts), "vectra_node")
  expect_error(tbl(f) |> rasterize(extent = c(0, 0, 1, 1), dims = c(2, 2),
                                   fun = "sum"),
               "needs a `field`")
  expect_error(tbl(f) |> rasterize(extent = c(0, 0, 1, 1), dims = c(2, 2),
                                   coords = c("lon", "lat")),
               "coords column")
  expect_error(tbl(f) |> rasterize(),
               "template|extent")
})
