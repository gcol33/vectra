# spatial_construct(): set-wise constructions on the partition tier. Geometry
# round-trips through a .vtr; CRS is carried on the node.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

# A square cloud of points (corners + an interior point) around (ox, oy).
pt_cloud <- function(ox = 0, oy = 0) {
  xy <- rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0.4, 0.6))
  xy[, 1] <- xy[, 1] + ox; xy[, 2] <- xy[, 2] + oy
  sf::st_sfc(lapply(seq_len(nrow(xy)), function(i) sf::st_point(xy[i, ])))
}

test_that("convex hull of the whole layer is one polygon of the right area", {
  x <- sf::st_sf(geometry = pt_cloud())
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_construct("convex_hull") |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_true(all(sf::st_geometry_type(d) == "POLYGON"))
  expect_equal(as.numeric(sf::st_area(d)), 1, tolerance = 1e-9)
})

test_that("by= builds one construction per group", {
  x <- sf::st_sf(
    g = c(rep("a", 5), rep("b", 5)),
    geometry = c(pt_cloud(0, 0), pt_cloud(10, 10)))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_construct("convex_hull", by = "g") |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_setequal(d$g, c("a", "b"))
  expect_equal(sort(as.numeric(sf::st_area(d))), c(1, 1), tolerance = 1e-9)
})

test_that("envelope is the axis-aligned bounding rectangle", {
  x <- sf::st_sf(geometry = pt_cloud())
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_construct("envelope") |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(as.numeric(sf::st_area(d)), 1, tolerance = 1e-9)
})

test_that("oriented box is no larger than the envelope", {
  set.seed(1)
  xy <- cbind(runif(40), runif(40))
  x <- sf::st_sf(geometry = sf::st_sfc(
    lapply(seq_len(nrow(xy)), function(i) sf::st_point(xy[i, ]))))
  f <- vtr_from(x); on.exit(unlink(f))
  ob <- tbl(f) |> spatial_construct("oriented_box") |> collect_sf()
  env <- tbl(f) |> spatial_construct("envelope") |> collect_sf()
  expect_true(all(sf::st_geometry_type(ob) == "POLYGON"))
  expect_lte(as.numeric(sf::st_area(ob)), as.numeric(sf::st_area(env)) + 1e-9)
})

test_that("the enclosing circle covers every input point", {
  x <- sf::st_sf(geometry = pt_cloud())
  f <- vtr_from(x); on.exit(unlink(f))
  circ <- tbl(f) |> spatial_construct("enclosing_circle") |> collect_sf()
  # the bounding circle passes through the extreme points; a tiny buffer absorbs
  # the segmentation error so the boundary points count as covered
  circ_b <- sf::st_buffer(sf::st_geometry(circ), 1e-6)
  inside <- sf::st_covers(circ_b, pt_cloud())[[1]]
  expect_equal(length(inside), 5L)
})

test_that("the inscribed circle lies inside the shape", {
  poly <- sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(4, 2), c(0, 2), c(0, 0))))
  x <- sf::st_sf(geometry = sf::st_sfc(poly))
  f <- vtr_from(x); on.exit(unlink(f))
  ic <- tbl(f) |> spatial_construct("inscribed_circle") |> collect_sf()
  expect_equal(nrow(ic), 1L)
  expect_true(all(sf::st_geometry_type(ic) == "POLYGON"))
  covered <- lengths(sf::st_covered_by(sf::st_geometry(ic), sf::st_sfc(poly)))
  expect_equal(covered, 1L)
})

test_that("the pole of inaccessibility is a point inside the shape", {
  poly <- sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(4, 2), c(0, 2), c(0, 0))))
  x <- sf::st_sf(geometry = sf::st_sfc(poly))
  f <- vtr_from(x); on.exit(unlink(f))
  p <- tbl(f) |> spatial_construct("pole") |> collect_sf()
  expect_equal(nrow(p), 1L)
  expect_true(all(sf::st_geometry_type(p) == "POINT"))
  expect_equal(lengths(sf::st_within(sf::st_geometry(p), sf::st_sfc(poly))), 1L)
  # the deepest interior point of a 4x2 box sits on its centre line (y = 1)
  expect_equal(unname(sf::st_coordinates(p)[1, "Y"]), 1, tolerance = 1e-2)
})

test_that("voronoi and delaunay emit one polygon per cell", {
  x <- sf::st_sf(geometry = pt_cloud())
  f <- vtr_from(x); on.exit(unlink(f))
  v <- tbl(f) |> spatial_construct("voronoi") |> collect_sf()
  d <- tbl(f) |> spatial_construct("delaunay") |> collect_sf()
  expect_gt(nrow(v), 1L)
  expect_true(all(sf::st_geometry_type(v) == "POLYGON"))
  expect_gt(nrow(d), 1L)
  expect_true(all(sf::st_geometry_type(d) == "POLYGON"))
})

test_that("a tessellation repeats the group's by values onto every cell", {
  x <- sf::st_sf(
    g = c(rep("a", 5), rep("b", 5)),
    geometry = c(pt_cloud(0, 0), pt_cloud(10, 10)))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_construct("delaunay", by = "g") |> collect_sf()
  expect_setequal(d$g, c("a", "b"))
  expect_true(all(d$g %in% c("a", "b")))
  expect_gt(sum(d$g == "a"), 0L)
  expect_gt(sum(d$g == "b"), 0L)
})

test_that("concave hull returns a polygon", {
  set.seed(2)
  xy <- cbind(runif(60), runif(60))
  x <- sf::st_sf(geometry = sf::st_sfc(
    lapply(seq_len(nrow(xy)), function(i) sf::st_point(xy[i, ]))))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_construct("concave_hull", ratio = 0.5) |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_true(all(sf::st_geometry_type(d) %in% c("POLYGON", "MULTIPOLYGON")))
})

test_that("a CRS passed to the verb is carried onto the construction", {
  x <- sf::st_sf(geometry = pt_cloud())
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_construct("convex_hull", crs = 3857) |> collect_sf()
  expect_equal(sf::st_crs(d), sf::st_crs(3857))
})

test_that("a missing by column is rejected", {
  x <- sf::st_sf(geometry = pt_cloud())
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(collect(spatial_construct(tbl(f), "convex_hull", by = "nope")),
               "not found")
})

test_that("a non-vectra_node input is rejected", {
  expect_error(spatial_construct(data.frame(a = 1), "convex_hull"), "vectra_node")
})
