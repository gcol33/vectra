# terrain() -- Horn 3x3 DEM derivatives over a streamed VECR raster, checked
# against terra::terrain / terra::shade and against the resident (single-strip)
# path. The streamed strip pass must equal the resident result exactly.

write_dem <- function(m, tile = 512L, dtype = "f64") {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = dtype,
                   extent = c(0, 0, ncol(m), nrow(m)), tile_size = tile)
  f
}

# A tilted surface with curvature so slope/aspect vary and no interior cell is
# flat (aspect undefined only on flats).
tilted <- function(nr, nc) {
  outer(seq_len(nr), seq_len(nc),
        function(r, c) 10 + 2 * c + 0.7 * r + 0.3 * r * c)
}

test_that("TPI and roughness recover hand-computed 3x3 summaries", {
  m <- matrix(c(1, 2, 3, 4,
                5, 6, 7, 8,
                9, 10, 12, 12,
                13, 14, 15, 16), 4, 4, byrow = TRUE)
  f <- write_dem(m); on.exit(unlink(f))

  tpi <- terrain(f, v = "TPI")
  # cell (2,2)=6: neighbours 1,2,3,5,7,9,10,12 -> mean 6.125 -> TPI -0.125
  nb <- c(m[1, 1:3], m[2, c(1, 3)], m[3, 1:3])
  expect_equal(tpi[2, 2], m[2, 2] - mean(nb))

  rough <- terrain(f, v = "roughness")
  blk <- m[1:3, 1:3]
  expect_equal(rough[2, 2], max(blk) - min(blk))
})

test_that("slope, aspect, TPI, roughness, TRI match terra::terrain", {
  skip_if_not_installed("terra")
  m <- tilted(12, 10)
  f <- write_dem(m, tile = 16L); on.exit(unlink(f))
  r <- terra::rast(m, extent = terra::ext(0, 10, 0, 12))

  for (v in c("slope", "aspect", "TPI", "roughness", "TRI")) {
    got <- terrain(f, v = v, unit = "degrees")
    ter <- matrix(terra::values(terra::terrain(r, v = v, unit = "degrees",
                                               neighbors = 8)),
                  nrow = 12, byrow = TRUE)
    expect_equal(got, ter, tolerance = 1e-7, ignore_attr = TRUE, info = v)
    expect_true(all(is.na(got) == is.na(ter)), info = v)
  }
})

test_that("slope honours the radians unit", {
  skip_if_not_installed("terra")
  m <- tilted(10, 9)
  f <- write_dem(m); on.exit(unlink(f))
  r <- terra::rast(m, extent = terra::ext(0, 9, 0, 10))
  got <- terrain(f, v = "slope", unit = "radians")
  ter <- matrix(terra::values(terra::terrain(r, v = "slope", unit = "radians")),
                nrow = 10, byrow = TRUE)
  expect_equal(got, ter, tolerance = 1e-7, ignore_attr = TRUE)
})

test_that("hillshade matches terra::shade", {
  skip_if_not_installed("terra")
  m <- tilted(11, 11)
  f <- write_dem(m); on.exit(unlink(f))
  r <- terra::rast(m, extent = terra::ext(0, 11, 0, 11))
  sl <- terra::terrain(r, v = "slope", unit = "radians")
  as <- terra::terrain(r, v = "aspect", unit = "radians")
  ter <- matrix(terra::values(terra::shade(sl, as, angle = 45, direction = 315)),
                nrow = 11, byrow = TRUE)
  got <- terrain(f, v = "hillshade", azimuth = 315, altitude = 45)
  expect_equal(got, ter, tolerance = 1e-7, ignore_attr = TRUE)
})

test_that("return follows input: matrix for one v, named list for several", {
  m <- tilted(6, 6)
  f <- write_dem(m); on.exit(unlink(f))
  one <- terrain(f, v = "slope")
  expect_true(is.matrix(one))
  many <- terrain(f, v = c("slope", "aspect", "TPI"))
  expect_type(many, "list")
  expect_equal(names(many), c("slope", "aspect", "TPI"))
  expect_equal(many$slope, one)
})

test_that("streamed strips equal the single-strip result", {
  m <- tilted(40, 33)
  f_s <- write_dem(m, tile = 8L)
  f_1 <- write_dem(m, tile = 512L)
  on.exit(unlink(c(f_s, f_1)))
  for (v in c("slope", "aspect", "hillshade", "TPI", "roughness", "TRI")) {
    a <- terrain(f_s, v = v)
    b <- terrain(f_1, v = v)
    expect_equal(a, b, tolerance = 1e-12, info = v)
  }
})

test_that("streamed-to-.vec output equals the in-memory result", {
  m <- tilted(22, 18)
  f <- write_dem(m, tile = 8L); on.exit(unlink(f))
  mem <- terrain(f, v = c("slope", "aspect"))

  of <- tempfile(fileext = ".vec"); on.exit(unlink(of), add = TRUE)
  h <- terrain(f, v = c("slope", "aspect"), path = of, dtype = "f64")
  on.exit(vec_close_raster(h), add = TRUE)
  expect_equal(h$n_bands, 2L)
  expect_equal(h$band_names, c("slope", "aspect"))
  s1 <- vec_read_window(h, band = 1)
  s2 <- vec_read_window(h, band = 2)
  expect_equal(max(abs(mem$slope - s1), na.rm = TRUE), 0)
  expect_equal(max(abs(mem$aspect - s2), na.rm = TRUE), 0)
})

test_that("terrain validates inputs", {
  m <- tilted(4, 4)
  f <- write_dem(m); on.exit(unlink(f))
  expect_error(terrain(f, v = "bogus"), "should be one of|arg")
  expect_error(terrain(f, unit = "grads"), "should be one of|arg")
})
