# spatial_explode(): multipart -> single-part, one row per component, attributes
# replicated. Geometry round-trips through a .vtr; CRS is carried on the node.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

mp2 <- function() sf::st_multipolygon(list(
  list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))),
  list(rbind(c(2, 2), c(3, 2), c(3, 3), c(2, 3), c(2, 2)))))

test_that("a multipolygon explodes to one row per polygon, attributes copied", {
  x <- sf::st_sf(id = 7L, k = "a", geometry = sf::st_sfc(mp2()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_explode() |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_equal(d$id, c(7L, 7L))
  expect_equal(d$k, c("a", "a"))
  expect_true(all(sf::st_geometry_type(d) == "POLYGON"))
  expect_equal(sort(as.numeric(sf::st_area(d))), c(1, 1), tolerance = 1e-9)
})

test_that("part= numbers the parts within each source feature, 1-based", {
  x <- sf::st_sf(id = c(1L, 2L), geometry = sf::st_sfc(mp2(), mp2()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_explode(part = "pid") |> collect()
  expect_equal(nrow(d), 4L)
  expect_equal(d$pid, c(1L, 2L, 1L, 2L))
})

test_that("single-part geometries pass through unchanged", {
  poly <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(poly))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_explode() |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(as.numeric(sf::st_area(d)), 1, tolerance = 1e-9)
})

test_that("multilinestring and multipoint explode by part", {
  mls <- sf::st_multilinestring(list(rbind(c(0, 0), c(1, 1)),
                                     rbind(c(2, 2), c(3, 3))))
  mpt <- sf::st_multipoint(rbind(c(0, 0), c(1, 1), c(2, 2)))
  xl <- sf::st_sf(id = 1L, geometry = sf::st_sfc(mls))
  xp <- sf::st_sf(id = 1L, geometry = sf::st_sfc(mpt))
  fl <- vtr_from(xl); fp <- vtr_from(xp); on.exit(unlink(c(fl, fp)))
  dl <- tbl(fl) |> spatial_explode() |> collect_sf()
  dp <- tbl(fp) |> spatial_explode() |> collect_sf()
  expect_equal(nrow(dl), 2L)
  expect_true(all(sf::st_geometry_type(dl) == "LINESTRING"))
  expect_equal(nrow(dp), 3L)
  expect_true(all(sf::st_geometry_type(dp) == "POINT"))
})

test_that("a geometry collection explodes recursively into its members", {
  gc <- sf::st_geometrycollection(list(
    sf::st_point(c(0, 0)),
    sf::st_multipoint(rbind(c(1, 1), c(2, 2)))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(gc))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_explode() |> collect_sf()
  expect_equal(nrow(d), 3L)
  expect_true(all(sf::st_geometry_type(d) == "POINT"))
})

test_that("a CRS passed to the verb is carried onto the exploded node", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(mp2()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_explode(crs = 3857) |> collect_sf()
  expect_equal(sf::st_crs(d), sf::st_crs(3857))
})

test_that("explode then total area equals the source area", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(mp2()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_explode() |> collect_sf()
  expect_equal(sum(as.numeric(sf::st_area(d))),
               as.numeric(sf::st_area(sf::st_sfc(mp2()))), tolerance = 1e-9)
})

test_that("a clashing part column name is rejected", {
  x <- sf::st_sf(pid = 1L, geometry = sf::st_sfc(mp2()))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(collect(spatial_explode(tbl(f), part = "pid")), "already exists")
})

test_that("a non-vectra_node input is rejected", {
  expect_error(spatial_explode(data.frame(a = 1)), "vectra_node")
})
