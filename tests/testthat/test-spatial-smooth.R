# spatial_smooth(): Chaikin corner-cutting over streamed lines and polygons.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

zig <- function() sf::st_linestring(
  rbind(c(0, 0), c(1, 1), c(2, 0), c(3, 1), c(4, 0)))

test_that("smoothing a line adds vertices and keeps the endpoints", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(zig()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_smooth(iterations = 3) |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_true(all(sf::st_geometry_type(d) == "LINESTRING"))
  co <- sf::st_coordinates(d)[, c("X", "Y")]
  expect_gt(nrow(co), 5L)                          # more vertices than the input
  expect_equal(unname(co[1, ]), c(0, 0), tolerance = 1e-9)
  expect_equal(unname(co[nrow(co), ]), c(4, 0), tolerance = 1e-9)
})

test_that("smoothing cuts the apex below the original and adds vertices", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(zig()))
  f <- vtr_from(x); on.exit(unlink(f))
  stat <- function(it) {
    d <- tbl(f) |> spatial_smooth(iterations = it) |> collect_sf()
    co <- sf::st_coordinates(d)
    list(peak = max(co[, "Y"]), n = nrow(co))
  }
  s1 <- stat(1); s3 <- stat(3)
  expect_lt(s1$peak, 1)                            # below the original apex y = 1
  expect_lt(s3$peak, 1)
  expect_gt(s3$n, s1$n)                            # more passes add more vertices
})

test_that("keep_ends = FALSE drops the original endpoints", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(zig()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_smooth(iterations = 1, keep_ends = FALSE) |> collect_sf()
  co <- sf::st_coordinates(d)[, c("X", "Y")]
  expect_false(isTRUE(all.equal(unname(co[1, ]), c(0, 0))))
})

test_that("a polygon stays a closed valid polygon after smoothing", {
  sq <- sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(4, 4), c(0, 4), c(0, 0))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(sq))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_smooth(iterations = 2) |> collect_sf()
  expect_true(all(sf::st_geometry_type(d) == "POLYGON"))
  expect_true(all(sf::st_is_valid(d)))
  # corner-cutting shrinks the ring but keeps most of the area
  a <- as.numeric(sf::st_area(d))
  expect_lt(a, 16); expect_gt(a, 12)
})

test_that("multilinestring smooths each part and attributes ride through", {
  mls <- sf::st_multilinestring(list(
    rbind(c(0, 0), c(1, 1), c(2, 0)),
    rbind(c(5, 5), c(6, 4), c(7, 5))))
  x <- sf::st_sf(id = 9L, k = "p", geometry = sf::st_sfc(mls))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_smooth(iterations = 2) |> collect_sf()
  expect_equal(d$id, 9L)
  expect_equal(d$k, "p")
  expect_true(all(sf::st_geometry_type(d) == "MULTILINESTRING"))
})

test_that("point geometry passes through unchanged", {
  pt <- sf::st_point(c(3, 4))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(pt))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_smooth() |> collect_sf()
  expect_equal(unname(sf::st_coordinates(d)[1, c("X", "Y")]), c(3, 4))
})

test_that("a CRS passed to the verb is carried", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(zig()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_smooth(crs = 3857) |> collect_sf()
  expect_equal(sf::st_crs(d), sf::st_crs(3857))
})

test_that("invalid inputs are rejected", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(zig()))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(spatial_smooth(data.frame(a = 1)), "vectra_node")
  expect_error(collect(spatial_smooth(tbl(f), iterations = 0)), "positive")
})
