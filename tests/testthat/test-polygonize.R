# Recovery and streaming-invariance tests for polygonize().
#
# Ground truth is the hand-computed area each value covers and terra::as.polygons
# where available. The streamed strip pass must reproduce per-value area and
# stay equal across tile sizes.

wr <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

area_by_value <- function(sf_polys, vcol = "value") {
  a <- as.numeric(sf::st_area(sf_polys))
  tapply(a, sf_polys[[vcol]], sum)
}

test_that("polygonize yields one dissolved polygon per value", {
  skip_if_not_installed("sf")
  m <- matrix(c(1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3), 4, 4, byrow = TRUE)
  f <- wr(m, tile = 2L)
  on.exit(unlink(f))

  pg <- collect_sf(polygonize(f))
  expect_equal(nrow(pg), 3L)
  expect_setequal(pg$value, c(1, 2, 3))
  ab <- area_by_value(pg)
  expect_equal(as.numeric(ab[order(as.numeric(names(ab)))]), c(4, 4, 8))
})

test_that("polygonize per-value area matches terra::as.polygons", {
  skip_if_not_installed("sf")
  skip_if_not_installed("terra")
  set.seed(11)
  m <- matrix(sample(1:4, 24 * 24, replace = TRUE), 24, 24)
  f <- wr(m, tile = 6L)
  on.exit(unlink(f))

  pg <- collect_sf(polygonize(f))
  va <- area_by_value(pg)

  tr <- terra::rast(m, extent = terra::ext(0, 24, 0, 24))
  tp <- sf::st_as_sf(terra::as.polygons(tr))
  ta <- tapply(as.numeric(sf::st_area(tp)), tp[[1]], sum)

  expect_equal(as.numeric(va[order(as.numeric(names(va)))]),
               as.numeric(ta[order(as.numeric(names(ta)))]), tolerance = 1e-9)
})

test_that("polygonize per-value area is invariant to tile size", {
  skip_if_not_installed("sf")
  set.seed(12)
  m <- matrix(sample(1:3, 18 * 18, replace = TRUE), 18, 18)
  f1 <- wr(m, tile = 3L); f2 <- wr(m, tile = 18L)
  on.exit(unlink(c(f1, f2)))

  a1 <- area_by_value(collect_sf(polygonize(f1)))
  a2 <- area_by_value(collect_sf(polygonize(f2)))
  expect_equal(a1[order(as.numeric(names(a1)))],
               a2[order(as.numeric(names(a2)))], tolerance = 1e-9)
})

test_that("dissolve = FALSE emits one square per cell", {
  skip_if_not_installed("sf")
  m <- matrix(c(1, 1, 2, 2), 2, 2, byrow = TRUE)
  f <- wr(m, tile = 1L)
  on.exit(unlink(f))

  pc <- collect_sf(polygonize(f, dissolve = FALSE))
  expect_equal(nrow(pc), 4L)
  expect_equal(sum(as.numeric(sf::st_area(pc))), 4)
})

test_that("na_rm drops nodata cells", {
  skip_if_not_installed("sf")
  m <- matrix(c(1, NA, NA, 2), 2, 2, byrow = TRUE)
  f <- wr(m, tile = 2L)
  on.exit(unlink(f))

  pg <- collect_sf(polygonize(f))
  expect_setequal(pg$value, c(1, 2))
  expect_equal(sum(as.numeric(sf::st_area(pg))), 2)
})
