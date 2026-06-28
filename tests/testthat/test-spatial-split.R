# spatial_split(): cut a streamed layer by a resident blade into pieces, or
# return the crossing points.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

sq    <- function() sf::st_polygon(list(
  rbind(c(0, 0), c(4, 0), c(4, 4), c(0, 4), c(0, 0))))
vblade <- function() sf::st_sfc(sf::st_linestring(rbind(c(2, -1), c(2, 5))))

test_that("a polygon is split into the faces the blade carves out", {
  x <- sf::st_sf(id = 1L, k = "a", geometry = sf::st_sfc(sq()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_split(vblade()) |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_equal(sort(as.numeric(sf::st_area(d))), c(8, 8), tolerance = 1e-9)
  expect_equal(d$id, c(1L, 1L))           # attributes copied onto each piece
  expect_equal(d$k, c("a", "a"))
  expect_equal(sum(as.numeric(sf::st_area(d))), 16, tolerance = 1e-9)
})

test_that("a line is split at the blade crossing", {
  line <- sf::st_linestring(rbind(c(0, 2), c(4, 2)))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(line))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_split(vblade()) |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_true(all(sf::st_geometry_type(d) == "LINESTRING"))
  expect_equal(sort(as.numeric(sf::st_length(d))), c(2, 2), tolerance = 1e-9)
})

test_that("a feature the blade misses passes through as one piece", {
  far <- sf::st_polygon(list(
    rbind(c(10, 10), c(12, 10), c(12, 12), c(10, 12), c(10, 10))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(far))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_split(vblade()) |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(as.numeric(sf::st_area(d)), 4, tolerance = 1e-9)
})

test_that("extract = 'points' returns the crossing points and drops misses", {
  line_hit  <- sf::st_linestring(rbind(c(0, 2), c(4, 2)))      # crosses x = 2
  line_miss <- sf::st_linestring(rbind(c(0, 9), c(1, 9)))      # never reaches blade
  x <- sf::st_sf(id = c(1L, 2L),
                 geometry = sf::st_sfc(line_hit, line_miss))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_split(vblade(), extract = "points") |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(d$id, 1L)
  xy <- sf::st_coordinates(d)
  expect_equal(unname(xy[1, c("X", "Y")]), c(2, 2), tolerance = 1e-9)
})

test_that("multiple blades split a polygon into a grid of pieces", {
  blades <- sf::st_sfc(
    sf::st_linestring(rbind(c(2, -1), c(2, 5))),
    sf::st_linestring(rbind(c(-1, 2), c(5, 2))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(sq()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_split(blades) |> collect_sf()
  expect_equal(nrow(d), 4L)
  expect_equal(sort(as.numeric(sf::st_area(d))), rep(4, 4), tolerance = 1e-9)
})

test_that("a CRS passed to the verb is carried onto the pieces", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(sq()))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_split(vblade(), crs = 3857) |> collect_sf()
  expect_equal(sf::st_crs(d), sf::st_crs(3857))
})

test_that("invalid inputs are rejected", {
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(sq()))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(spatial_split(data.frame(a = 1), vblade()), "vectra_node")
  expect_error(spatial_split(tbl(f), data.frame(a = 1)), "sf")
  expect_error(collect(spatial_split(tbl(f), vblade(), extract = "nope")),
               "should be one of")
})
