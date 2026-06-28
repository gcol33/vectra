# spatial_snap_grid() rounds coordinates to a lattice (the overlay noder's snap,
# exposed); spatial_snap() pulls vertices toward a resident reference layer.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

test_that("snap_grid rounds jittered coordinates onto the grid", {
  p <- sf::st_polygon(list(rbind(c(0.04, 0.03), c(1.02, 0.01),
                                 c(0.98, 1.03), c(0.01, 0.97), c(0.04, 0.03))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(p))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_snap_grid(0.1) |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(d$id, 1L)
  co <- sf::st_coordinates(d)[, c("X", "Y")]
  expect_equal(co, round(co / 0.1) * 0.1, tolerance = 1e-9)
})

test_that("snap_grid keeps one cleaned feature per input and its attributes", {
  ps <- sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0)))),
    sf::st_polygon(list(rbind(c(2, 2), c(3, 2), c(3, 3), c(2, 3), c(2, 2)))))
  x <- sf::st_sf(id = c(10L, 20L), k = c("a", "b"), geometry = ps)
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_snap_grid(0.5) |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_equal(d$id, c(10L, 20L))
  expect_equal(d$k, c("a", "b"))
})

test_that("snap_grid carries a CRS passed to the verb", {
  p <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(p))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_snap_grid(0.5, crs = 3857) |> collect_sf()
  expect_equal(sf::st_crs(d), sf::st_crs(3857))
})

test_that("snap_grid rejects a non-positive size", {
  p <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
  x <- sf::st_sf(geometry = sf::st_sfc(p))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(collect(spatial_snap_grid(tbl(f), 0)), "positive")
})

test_that("snap pulls near vertices onto a reference layer", {
  ref  <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(10, 0))))
  line <- sf::st_linestring(rbind(c(0, 0.2), c(5, 0.1), c(10, 0.2)))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(line))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_snap(ref, tolerance = 0.5) |> collect_sf()
  # the two endpoints sit within tolerance of the reference line, so they snap
  # onto y = 0
  ends <- sf::st_coordinates(d)
  expect_equal(min(abs(ends[, "Y"])), 0, tolerance = 1e-9)
  expect_lt(min(ends[, "Y"]), 0.2)
})

test_that("snap leaves vertices beyond the tolerance untouched", {
  ref  <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(10, 0))))
  line <- sf::st_linestring(rbind(c(0, 5), c(10, 5)))
  x <- sf::st_sf(id = 1L, geometry = sf::st_sfc(line))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_snap(ref, tolerance = 0.5) |> collect_sf()
  expect_equal(unique(sf::st_coordinates(d)[, "Y"]), 5, tolerance = 1e-9)
})

test_that("snap rejects a non-sf reference and a non-positive tolerance", {
  line <- sf::st_linestring(rbind(c(0, 0), c(1, 1)))
  x <- sf::st_sf(geometry = sf::st_sfc(line))
  f <- vtr_from(x); on.exit(unlink(f))
  ref <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(10, 0))))
  expect_error(spatial_snap(tbl(f), data.frame(a = 1), tolerance = 1), "sf")
  expect_error(collect(spatial_snap(tbl(f), ref, tolerance = -1)), "positive")
})

test_that("a non-vectra_node input is rejected by both verbs", {
  ref <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(10, 0))))
  expect_error(spatial_snap_grid(data.frame(a = 1), 1), "vectra_node")
  expect_error(spatial_snap(data.frame(a = 1), ref, tolerance = 1), "vectra_node")
})
