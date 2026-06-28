# Topology verbs: polygonize (lines -> faces), line_merge (segments -> maximal
# lines), simplify (coverage-preserving), locate (linear referencing), centerline
# (medial axis), topology (shared-edge arcs), eliminate (merge slivers). Geometry
# round-trips through a .vtr; CRS is carried on the node.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

# A 2x2 grid of unit cells drawn as six lines (three horizontal, three vertical).
grid_lines <- function() {
  sf::st_sfc(
    sf::st_linestring(rbind(c(0, 0), c(2, 0))),
    sf::st_linestring(rbind(c(0, 1), c(2, 1))),
    sf::st_linestring(rbind(c(0, 2), c(2, 2))),
    sf::st_linestring(rbind(c(0, 0), c(0, 2))),
    sf::st_linestring(rbind(c(1, 0), c(1, 2))),
    sf::st_linestring(rbind(c(2, 0), c(2, 2))))
}

# -- polygonize ---------------------------------------------------------------

test_that("polygonize forms the faces enclosed by a line network", {
  x <- sf::st_sf(geometry = grid_lines())
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_polygonize() |> collect_sf()
  expect_equal(nrow(d), 4L)
  expect_true(all(sf::st_geometry_type(d) == "POLYGON"))
  expect_equal(sort(as.numeric(sf::st_area(d))), rep(1, 4), tolerance = 1e-9)
})

test_that("polygonize builds one set of faces per by group", {
  g1 <- grid_lines()
  g2 <- grid_lines() + c(10, 10)
  x <- sf::st_sf(
    grp = c(rep("a", 6), rep("b", 6)),
    geometry = sf::st_sfc(c(g1, g2)))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_polygonize(by = "grp") |> collect_sf()
  expect_equal(nrow(d), 8L)
  expect_equal(sum(d$grp == "a"), 4L)
  expect_equal(sum(d$grp == "b"), 4L)
})

test_that("polygonize rejects a missing by column and a non-node input", {
  x <- sf::st_sf(geometry = grid_lines())
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(collect(spatial_polygonize(tbl(f), by = "nope")), "not found")
  expect_error(spatial_polygonize(data.frame(a = 1)), "vectra_node")
})

# -- line_merge ---------------------------------------------------------------

test_that("line_merge sews end-to-end segments into one maximal line", {
  seg <- sf::st_sfc(
    sf::st_linestring(rbind(c(0, 0), c(1, 0))),
    sf::st_linestring(rbind(c(1, 0), c(2, 0))),
    sf::st_linestring(rbind(c(2, 0), c(3, 0))))
  x <- sf::st_sf(geometry = seg)
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_line_merge() |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(as.numeric(sf::st_length(d)), 3, tolerance = 1e-9)
})

test_that("line_merge keeps disconnected chains separate", {
  seg <- sf::st_sfc(
    sf::st_linestring(rbind(c(0, 0), c(1, 0))),
    sf::st_linestring(rbind(c(1, 0), c(2, 0))),
    sf::st_linestring(rbind(c(5, 5), c(6, 5))))
  x <- sf::st_sf(geometry = seg)
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_line_merge() |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_equal(sort(as.numeric(sf::st_length(d))), c(1, 2), tolerance = 1e-9)
})

# -- simplify (coverage-preserving) -------------------------------------------

test_that("coverage simplify keeps neighbours edge-matched and attributes", {
  p1 <- sf::st_polygon(list(rbind(
    c(0, 0), c(1, 0), c(1.1, 0.5), c(1, 1), c(0, 1), c(0, 0))))
  p2 <- sf::st_polygon(list(rbind(
    c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1.1, 0.5), c(1, 0))))
  x <- sf::st_sf(id = c("a", "b"), geometry = sf::st_sfc(p1, p2))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_simplify(tolerance = 0.5) |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_setequal(d$id, c("a", "b"))
  in_area  <- as.numeric(sf::st_area(sf::st_union(sf::st_sfc(p1, p2))))
  out_area <- as.numeric(sf::st_area(sf::st_union(sf::st_geometry(d))))
  # a torn coverage would leave a sliver or overlap and change the union area
  expect_equal(out_area, in_area, tolerance = 1e-9)
  inter <- suppressWarnings(
    sf::st_intersection(sf::st_geometry(d)[1], sf::st_geometry(d)[2]))
  if (length(inter)) expect_lt(as.numeric(sf::st_area(inter)), 1e-9)
})

test_that("coverage simplify drops near-collinear vertices", {
  ring <- rbind(c(0, 0), c(2, 0.05), c(4, 0), c(4, 4), c(0, 4), c(0, 0))
  x <- sf::st_sf(id = "a", geometry = sf::st_sfc(sf::st_polygon(list(ring))))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_simplify(tolerance = 0.2) |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_lt(nrow(sf::st_coordinates(d)), nrow(ring))
})

# -- locate (linear referencing) ----------------------------------------------

test_that("locate returns the measure, nearest line, and offset distance", {
  line <- sf::st_sf(road = c("main", "side"), geometry = sf::st_sfc(
    sf::st_linestring(rbind(c(0, 0), c(10, 0))),
    sf::st_linestring(rbind(c(0, 5), c(0, 15)))))
  pts <- data.frame(id = 1:2, x = c(3, 1), y = c(1, 9))
  f <- tempfile(fileext = ".vtr"); write_vtr(pts, f); on.exit(unlink(f))
  d <- tbl(f) |>
    spatial_locate(line, coords = c("x", "y"), y_id = "road") |>
    collect()
  expect_equal(d$line, c("main", "side"))
  expect_equal(d$measure, c(3, 4), tolerance = 1e-6)
  expect_equal(d$distance, c(1, 1), tolerance = 1e-6)
})

test_that("locate snap moves each point onto its nearest line", {
  line <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(10, 0))))
  pts <- data.frame(id = 1L, x = 3, y = 2)
  f <- tempfile(fileext = ".vtr"); write_vtr(pts, f); on.exit(unlink(f))
  d <- tbl(f) |>
    spatial_locate(line, coords = c("x", "y"), snap = TRUE) |>
    collect_sf()
  expect_true(all(sf::st_geometry_type(d) == "POINT"))
  expect_equal(unname(sf::st_coordinates(d)[1, c("X", "Y")]), c(3, 0),
               tolerance = 1e-6)
})

test_that("locate rejects a non-node input and a non-sf line", {
  line <- sf::st_sfc(sf::st_linestring(rbind(c(0, 0), c(1, 0))))
  expect_error(spatial_locate(data.frame(a = 1), line), "vectra_node")
  pts <- data.frame(x = 0, y = 0)
  f <- tempfile(fileext = ".vtr"); write_vtr(pts, f); on.exit(unlink(f))
  expect_error(spatial_locate(tbl(f), data.frame(a = 1), coords = c("x", "y")),
               "sf or sfc")
})

# -- centerline ---------------------------------------------------------------

test_that("centerline runs down the middle of a strip", {
  road <- sf::st_polygon(list(rbind(
    c(0, 0), c(10, 0), c(10, 2), c(0, 2), c(0, 0))))
  x <- sf::st_sf(geometry = sf::st_sfc(road))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_centerline(density = 0.25, prune = 0.5) |> collect_sf()
  expect_gt(nrow(d), 0L)
  expect_true(all(sf::st_geometry_type(d) == "LINESTRING"))
  # the centerline lies inside the strip and tracks mid-height (y ~ 1)
  expect_true(all(lengths(sf::st_within(
    sf::st_geometry(d), sf::st_sfc(road))) > 0))
  ys <- sf::st_coordinates(d)[, "Y"]
  expect_lt(abs(mean(ys) - 1), 0.3)
  # it spans most of the 10-unit length
  expect_gt(sum(as.numeric(sf::st_length(d))), 8)
})

test_that("centerline passes non-polygon geometry through unchanged", {
  ln <- sf::st_linestring(rbind(c(0, 0), c(5, 0)))
  x <- sf::st_sf(geometry = sf::st_sfc(ln))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_centerline() |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(as.numeric(sf::st_length(d)), 5, tolerance = 1e-9)
})

test_that("centerline rejects bad density and prune", {
  x <- sf::st_sf(geometry = sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(4, 2), c(0, 2), c(0, 0))))))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(spatial_centerline(tbl(f), density = -1), "density")
  expect_error(spatial_centerline(tbl(f), prune = -1), "prune")
})

# -- topology -----------------------------------------------------------------

test_that("topology returns shared edges once with both neighbours", {
  p1 <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
  p2 <- sf::st_polygon(list(rbind(c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0))))
  p3 <- sf::st_polygon(list(rbind(c(0, 1), c(1, 1), c(1, 2), c(0, 2), c(0, 1))))
  x <- sf::st_sf(id = c("a", "b", "c"), geometry = sf::st_sfc(p1, p2, p3))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_topology(id = "id") |> collect_sf()
  expect_true(all(c("face1", "face2") %in% names(d)))
  expect_true(all(sf::st_geometry_type(d) == "LINESTRING"))
  nfaces <- (!is.na(d$face1)) + (!is.na(d$face2))
  # exactly two internal shared edges (a|b and a|c), the rest outer (one face)
  expect_equal(sum(nfaces == 2L), 2L)
  expect_gt(sum(nfaces == 1L), 0L)
  shared <- d[nfaces == 2L, ]
  pair <- function(r) paste(sort(c(r$face1, r$face2)), collapse = "|")
  expect_setequal(vapply(seq_len(nrow(shared)),
                         function(i) pair(shared[i, ]), character(1)),
                  c("a|b", "a|c"))
})

test_that("topology arcs rebuild the original coverage via polygonize", {
  p1 <- sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))
  p2 <- sf::st_polygon(list(rbind(c(1, 0), c(2, 0), c(2, 1), c(1, 1), c(1, 0))))
  x <- sf::st_sf(id = c("a", "b"), geometry = sf::st_sfc(p1, p2))
  f <- vtr_from(x); on.exit(unlink(f))
  arcs <- tbl(f) |> spatial_topology(id = "id") |> collect_sf()
  faces <- sf::st_collection_extract(
    sf::st_polygonize(sf::st_union(sf::st_geometry(arcs))), "POLYGON")
  expect_equal(length(faces), 2L)
  expect_equal(as.numeric(sf::st_area(sf::st_union(faces))), 2, tolerance = 1e-9)
})

test_that("topology rejects a missing id column and bad face_cols", {
  x <- sf::st_sf(geometry = sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(collect(spatial_topology(tbl(f), id = "nope")), "not found")
  expect_error(spatial_topology(tbl(f), face_cols = "only_one"), "two distinct")
})

# -- eliminate (merge slivers) ------------------------------------------------

test_that("eliminate absorbs a sliver into the square it borders", {
  big <- sf::st_polygon(list(rbind(
    c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0))))
  sliver <- sf::st_polygon(list(rbind(
    c(10, 0), c(10.3, 0), c(10.3, 10), c(10, 10), c(10, 0))))
  x <- sf::st_sf(id = c("keep", "sliver"), geometry = sf::st_sfc(big, sliver))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_eliminate(max_area = 5) |> collect_sf()
  expect_equal(nrow(d), 1L)
  # the survivor keeps the large feature's attributes
  expect_equal(d$id, "keep")
  # the union area is preserved (nothing dropped, nothing double-counted)
  expect_equal(as.numeric(sf::st_area(sf::st_geometry(d))), 103,
               tolerance = 1e-9)
})

test_that("eliminate collapses a chain of slivers to the largest member", {
  b  <- sf::st_polygon(list(rbind(
    c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0))))
  s1 <- sf::st_polygon(list(rbind(
    c(10, 0), c(10.3, 0), c(10.3, 10), c(10, 10), c(10, 0))))
  s2 <- sf::st_polygon(list(rbind(
    c(10.3, 0), c(10.6, 0), c(10.6, 10), c(10.3, 10), c(10.3, 0))))
  x <- sf::st_sf(id = c("big", "s1", "s2"), geometry = sf::st_sfc(b, s1, s2))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_eliminate(max_area = 5) |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(d$id, "big")
})

test_that("eliminate keeps a sliver with no neighbour and leaves big ones", {
  b1  <- sf::st_polygon(list(rbind(
    c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0))))
  b2  <- sf::st_polygon(list(rbind(
    c(10, 0), c(20, 0), c(20, 10), c(10, 10), c(10, 0))))
  iso <- sf::st_polygon(list(rbind(
    c(50, 50), c(50.2, 50), c(50.2, 51), c(50, 51), c(50, 50))))
  x <- sf::st_sf(id = c("b1", "b2", "iso"), geometry = sf::st_sfc(b1, b2, iso))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_eliminate(max_area = 5) |> collect_sf()
  # two untouched big squares plus the unmergeable isolated sliver
  expect_equal(nrow(d), 3L)
  expect_setequal(d$id, c("b1", "b2", "iso"))
})

test_that("eliminate by-group cleans each coverage independently", {
  mk <- function(dx) {
    big <- sf::st_polygon(list(rbind(
      c(0, 0), c(10, 0), c(10, 10), c(0, 10), c(0, 0)))) + c(dx, 0)
    sl  <- sf::st_polygon(list(rbind(
      c(10, 0), c(10.3, 0), c(10.3, 10), c(10, 10), c(10, 0)))) + c(dx, 0)
    sf::st_sfc(big, sl)
  }
  x <- sf::st_sf(
    grp = c("a", "a", "b", "b"),
    geometry = sf::st_sfc(c(mk(0), mk(100))))
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> spatial_eliminate(max_area = 5, by = "grp") |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_setequal(d$grp, c("a", "b"))
})

test_that("eliminate rejects a bad threshold and a non-node input", {
  x <- sf::st_sf(geometry = sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))))))
  f <- vtr_from(x); on.exit(unlink(f))
  expect_error(spatial_eliminate(tbl(f), max_area = -1), "positive")
  expect_error(spatial_eliminate(data.frame(a = 1), max_area = 1), "vectra_node")
})
