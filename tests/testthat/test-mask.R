# Recovery and streaming-invariance tests for mask().
#
# Ground truth is sf centre-in-polygon (the package's own pixel-centre
# convention, shared with zonal()). mask must reproduce it exactly, stay equal
# across tile sizes, and sit inside terra's any-overlap mask where terra exists.

wr <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

# Centre-in-polygon truth for an n x n grid of unit cells over [0, n]^2.
centre_cover <- function(n, poly) {
  cx <- rep(seq(0.5, n - 0.5, 1), each = n)
  cy <- rep(seq(n - 0.5, 0.5, -1), times = n)
  pts <- sf::st_as_sf(data.frame(x = cx, y = cy), coords = c("x", "y"))
  matrix(lengths(sf::st_intersects(pts, poly)) > 0, n, n, byrow = TRUE)
}

test_that("mask matches sf centre-in-polygon exactly", {
  skip_if_not_installed("sf")
  vals <- matrix(1:100, 10, 10, byrow = TRUE)
  fv <- wr(vals, ext = c(0, 0, 10, 10), tile = 4L)
  on.exit(unlink(fv))
  disc <- sf::st_buffer(sf::st_sfc(sf::st_point(c(5, 5))), 3)

  m <- mask(fv, disc)
  cov <- centre_cover(10, disc)
  expect_equal(!is.na(m), cov, ignore_attr = TRUE)
  # kept pixels keep their original value
  expect_equal(m[cov], vals[cov])
})

test_that("mask inverse keeps exactly the complement", {
  skip_if_not_installed("sf")
  vals <- matrix(1:100, 10, 10, byrow = TRUE)
  fv <- wr(vals, ext = c(0, 0, 10, 10), tile = 4L)
  on.exit(unlink(fv))
  disc <- sf::st_buffer(sf::st_sfc(sf::st_point(c(5, 5))), 3)

  m <- mask(fv, disc)
  inv <- mask(fv, disc, inverse = TRUE)
  expect_equal(is.na(inv), !is.na(m), ignore_attr = TRUE)
})

test_that("mask is invariant to tile size (streaming invariance)", {
  skip_if_not_installed("sf")
  vals <- matrix(seq_len(20 * 20), 20, 20, byrow = TRUE)
  poly <- sf::st_buffer(sf::st_sfc(sf::st_point(c(10, 12))), 5)
  f1 <- wr(vals, ext = c(0, 0, 20, 20), tile = 4L)
  f2 <- wr(vals, ext = c(0, 0, 20, 20), tile = 20L)
  on.exit(unlink(c(f1, f2)))

  expect_equal(mask(f1, poly), mask(f2, poly))
})

test_that("mask kept cells are a subset of terra's any-overlap mask", {
  skip_if_not_installed("sf")
  skip_if_not_installed("terra")
  vals <- matrix(1:100, 10, 10, byrow = TRUE)
  fv <- wr(vals, ext = c(0, 0, 10, 10), tile = 4L)
  on.exit(unlink(fv))
  disc <- sf::st_buffer(sf::st_sfc(sf::st_point(c(5, 5))), 3)

  m <- mask(fv, disc)
  tr <- terra::rast(vals, extent = terra::ext(0, 10, 0, 10))
  tmv <- matrix(terra::values(terra::mask(tr, terra::vect(disc), touches = TRUE)),
                10, 10, byrow = TRUE)
  # every cell I keep, terra (any-overlap) also keeps
  expect_true(all(!is.na(tmv)[!is.na(m)]))
})

test_that("mask preserves all bands and streams to a .vec", {
  skip_if_not_installed("sf")
  arr <- array(0, dim = c(8, 8, 2))
  arr[, , 1] <- matrix(1:64, 8, 8, byrow = TRUE)
  arr[, , 2] <- matrix(64:1, 8, 8, byrow = TRUE)
  f <- tempfile(fileext = ".vec"); out <- tempfile(fileext = ".vec")
  vec_write_raster(arr, f, dtype = "f64", extent = c(0, 0, 8, 8), tile_size = 4L)
  on.exit(unlink(c(f, out)))
  poly <- sf::st_buffer(sf::st_sfc(sf::st_point(c(4, 4))), 2.5)

  h <- mask(f, poly, path = out)
  expect_s3_class(h, "vectra_raster")
  expect_equal(h$n_bands, 2L)
  b1 <- vec_read_window(h, band = 1L); b2 <- vec_read_window(h, band = 2L)
  vec_close_raster(h)
  expect_equal(is.na(b1), is.na(b2))
})

test_that("mask rejects a non-polygon mask", {
  fv <- wr(matrix(1, 4, 4))
  on.exit(unlink(fv))
  expect_error(mask(fv, data.frame(a = 1)), "sf or sfc")
})
