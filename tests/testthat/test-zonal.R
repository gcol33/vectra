# Recovery and streaming-invariance tests for zonal().
#
# The reference is an independent base-R grouped summary (tapply over zone ids)
# of the same pixels. The streamed strip fold must reproduce it exactly, equal
# across tile sizes (streaming invariance), and match terra::zonal cell-for-cell
# where terra is available.

# Independent per-zone reference over two aligned matrices (row-major pixels).
ref_zonal <- function(vals, zone, fun, na.rm = TRUE) {
  v <- as.vector(vals); z <- as.vector(zone)
  keep <- !is.na(z)
  v <- v[keep]; z <- z[keep]
  if (na.rm) {
    ok <- !is.na(v); v <- v[ok]; z <- z[ok]
  }
  labs <- sort(unique(z))
  stat <- function(f) vapply(labs, function(k) {
    vv <- v[z == k]
    if (!na.rm && any(is.na(vv)) && f != "count") return(NA_real_)
    vv <- vv[!is.na(vv)]
    switch(f,
      count = length(vv), sum = sum(vv), mean = mean(vv),
      min = min(vv), max = max(vv),
      sd = if (length(vv) < 2) NA_real_ else stats::sd(vv))
  }, numeric(1))
  out <- data.frame(zone = as.numeric(labs))
  for (f in fun) out[[f]] <- stat(f)
  out
}

write_raster <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

test_that("zonal recovers a hand-computed per-zone summary", {
  vals <- matrix(1:16, 4, 4, byrow = TRUE)
  zone <- matrix(c(1, 1, 2, 2, 1, 1, 2, 2,
                   3, 3, 4, 4, 3, 3, 4, 4), 4, 4, byrow = TRUE)
  fv <- write_raster(vals); fz <- write_raster(zone)
  on.exit(unlink(c(fv, fz)))

  got <- zonal(fv, fz, fun = c("mean", "sum", "count", "min", "max"))
  ref <- ref_zonal(vals, zone, c("mean", "sum", "count", "min", "max"))
  expect_equal(got, ref, tolerance = 1e-9)
  # zone 1 holds the four top-left values 1,2,5,6
  expect_equal(got$sum[got$zone == 1], 1 + 2 + 5 + 6)
})

test_that("sd matches the resident standard deviation", {
  set.seed(21)
  vals <- matrix(rnorm(40 * 40), 40, 40)
  zone <- matrix(sample(1:5, 40 * 40, replace = TRUE), 40, 40)
  fv <- write_raster(vals); fz <- write_raster(zone)
  on.exit(unlink(c(fv, fz)))

  got <- zonal(fv, fz, fun = "sd")
  ref <- ref_zonal(vals, zone, "sd")
  expect_equal(got, ref, tolerance = 1e-8)
})

test_that("streamed strips equal the single-strip result", {
  set.seed(22)
  vals <- matrix(rnorm(60 * 50), 60, 50)
  zone <- matrix(sample(1:7, 60 * 50, replace = TRUE), 60, 50)
  # small tiles -> many strips, one giant tile -> a single strip
  fv_s <- write_raster(vals, tile = 16L); fz_s <- write_raster(zone, tile = 16L)
  fv_1 <- write_raster(vals, tile = 512L); fz_1 <- write_raster(zone, tile = 512L)
  on.exit(unlink(c(fv_s, fz_s, fv_1, fz_1)))

  funs <- c("mean", "sum", "count", "min", "max", "sd")
  a <- zonal(fv_s, fz_s, fun = funs)
  b <- zonal(fv_1, fz_1, fun = funs)
  expect_equal(a, b, tolerance = 1e-9)
})

test_that("nodata pixels are skipped under na.rm = TRUE and taint under FALSE", {
  vals <- matrix(c(1, 2, NA, 4,
                   5, 6, 7, 8), 2, 4, byrow = TRUE)
  zone <- matrix(c(1, 1, 2, 2,
                   1, 1, 2, 2), 2, 4, byrow = TRUE)
  fv <- write_raster(vals); fz <- write_raster(zone)
  on.exit(unlink(c(fv, fz)))

  # zone 2 spans columns 3-4: values NA, 4, 7, 8
  keep <- zonal(fv, fz, fun = c("mean", "count"), na.rm = TRUE)
  expect_equal(keep$mean[keep$zone == 2], mean(c(4, 7, 8)))  # NA dropped
  expect_equal(keep$count[keep$zone == 2], 3)               # only non-NA cells

  prop <- zonal(fv, fz, fun = c("mean", "count"), na.rm = FALSE)
  expect_true(is.na(prop$mean[prop$zone == 2]))             # NA propagates
  expect_equal(prop$count[prop$zone == 2], 3)               # count still non-NA
})

test_that("zonal matches terra::zonal cell-for-cell", {
  skip_if_not_installed("terra")
  set.seed(23)
  vals <- matrix(rnorm(50 * 50), 50, 50)
  zone <- matrix(sample(1:6, 50 * 50, replace = TRUE), 50, 50)
  fv <- write_raster(vals); fz <- write_raster(zone)
  on.exit(unlink(c(fv, fz)))

  rv <- terra::rast(vals, extent = terra::ext(0, 50, 0, 50))
  rz <- terra::rast(zone, extent = terra::ext(0, 50, 0, 50))
  for (fn in c("mean", "sum", "min", "max")) {
    got <- zonal(fv, fz, fun = fn)
    ter <- terra::zonal(rv, rz, fun = fn, na.rm = TRUE)
    names(ter) <- c("zone", "val")
    ter <- ter[order(ter$zone), ]
    expect_equal(got[[fn]], ter$val, tolerance = 1e-9, info = fn)
  }
})

test_that("sf polygon zones assign each pixel by its centre", {
  skip_if_not_installed("sf")
  # 4x4 grid over [0,4]^2; split into left (x<2) and right (x>=2) halves.
  vals <- matrix(1:16, 4, 4, byrow = TRUE)
  fv <- write_raster(vals, ext = c(0, 0, 4, 4))
  on.exit(unlink(fv))

  sq <- function(x0, x1) sf::st_polygon(list(rbind(
    c(x0, 0), c(x1, 0), c(x1, 4), c(x0, 4), c(x0, 0))))
  zones <- sf::st_sf(zid = c(10, 20),
                     geometry = sf::st_sfc(sq(0, 2), sq(2, 4)))

  got <- zonal(fv, zones, fun = c("mean", "count"), zone_field = "zid")
  # left two columns -> zone 10, right two -> zone 20; 8 cells each
  expect_equal(got$zone, c(10, 20))
  expect_equal(got$count, c(8, 8))
  left  <- mean(as.vector(vals[, 1:2]))
  right <- mean(as.vector(vals[, 3:4]))
  expect_equal(got$mean, c(left, right), tolerance = 1e-9)
})

test_that("a vectra_raster handle works as well as a path", {
  vals <- matrix(1:9, 3, 3, byrow = TRUE)
  zone <- matrix(c(1, 1, 2, 1, 1, 2, 3, 3, 3), 3, 3, byrow = TRUE)
  fv <- write_raster(vals); fz <- write_raster(zone)
  on.exit(unlink(c(fv, fz)))
  rv <- vec_open_raster(fv); rz <- vec_open_raster(fz)
  on.exit({ vec_close_raster(rv); vec_close_raster(rz) }, add = TRUE)

  got <- zonal(rv, rz, fun = "sum")
  ref <- ref_zonal(vals, zone, "sum")
  expect_equal(got, ref, tolerance = 1e-9)
})

test_that("input validation", {
  vals <- matrix(1:4, 2, 2)
  fv <- write_raster(vals); on.exit(unlink(fv))
  expect_error(zonal(fv, 42), "vectra_raster.*path.*sf")
  expect_error(zonal(fv, fv, fun = "bogus"), "should be one of|arg")
  # mismatched dimensions
  big <- write_raster(matrix(1:9, 3, 3)); on.exit(unlink(big), add = TRUE)
  expect_error(zonal(fv, big), "dimensions")
})
