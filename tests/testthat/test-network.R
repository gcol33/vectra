# Network analysis: spatial_network / spatial_route / spatial_service_area.
#
# Recovery tests against hand-computed shortest paths on a known graph (and
# against igraph when available), route geometry reconstruction, service-area
# reachable sets, directed one-way handling, unreachable Inf, and the streaming
# invariance that a multi-batch origin stream equals a single-batch result.

skip_if_no_sf <- function() skip_if_not_installed("sf")

# A 3x2 lattice of unit street segments:
#   (0,1)--(1,1)--(2,1)
#     |      |      |
#   (0,0)--(1,0)--(2,0)
# Every edge has length 1, so shortest-path cost == Manhattan grid distance.
lattice_streets <- function() {
  mk <- function(x1, y1, x2, y2)
    sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
  sf::st_sfc(
    mk(0, 0, 1, 0), mk(1, 0, 2, 0),          # bottom row
    mk(0, 1, 1, 1), mk(1, 1, 2, 1),          # top row
    mk(0, 0, 0, 1), mk(1, 0, 1, 1), mk(2, 0, 2, 1)  # verticals
  )
}

write_points <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

test_that("spatial_network reports node, edge, component counts", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  expect_s3_class(net, "vectra_network")
  expect_identical(net$stats[1], 6L)        # 6 nodes
  expect_identical(net$stats[2], 14L)       # 7 lines * 2 directed edges
  expect_identical(net$stats[3], 1L)        # one connected component
})

test_that("route cost matches hand-computed grid distance", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  f <- write_points(data.frame(id = 1L, x = 0, y = 0))
  dest <- sf::st_sfc(sf::st_point(c(2, 1)))   # opposite corner

  res <- tbl(f) |>
    spatial_route(net, to = dest, coords = c("x", "y"), geometry = FALSE) |>
    collect()
  # (0,0) -> (2,1): three unit steps
  expect_equal(res$cost, 3)
  unlink(f)
})

test_that("origin-destination cost matrix is one row per pair", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  f <- write_points(data.frame(id = 1:2, x = c(0, 2), y = c(0, 0)))
  dests <- sf::st_sf(
    name = c("tl", "tr"),
    geometry = sf::st_sfc(sf::st_point(c(0, 1)), sf::st_point(c(2, 1))))

  res <- tbl(f) |>
    spatial_route(net, to = dests, to_id = "name", geometry = FALSE,
                  coords = c("x", "y")) |>
    collect()
  expect_equal(nrow(res), 4L)               # 2 origins x 2 destinations
  # origin (0,0): to tl(0,1)=1, to tr(2,1)=3; origin (2,0): tl=3, tr=1
  o1 <- res[res$id == 1L, ]
  expect_equal(o1$cost[o1$destination == "tl"], 1)
  expect_equal(o1$cost[o1$destination == "tr"], 3)
  o2 <- res[res$id == 2L, ]
  expect_equal(o2$cost[o2$destination == "tl"], 3)
  expect_equal(o2$cost[o2$destination == "tr"], 1)
  unlink(f)
})

test_that("route geometry reconstructs a path of the right length", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  f <- write_points(data.frame(id = 1L, x = 0, y = 0))
  dest <- sf::st_sfc(sf::st_point(c(2, 1)))

  sfres <- tbl(f) |>
    spatial_route(net, to = dest, coords = c("x", "y")) |>
    collect_sf()
  expect_equal(nrow(sfres), 1L)
  expect_true(as.character(sf::st_geometry_type(sfres)) == "LINESTRING")
  # the reconstructed line length equals the routing cost
  expect_equal(as.numeric(sf::st_length(sfres)), 3, tolerance = 1e-9)
  unlink(f)
})

test_that("unreachable destination returns Inf cost, not a dropped row", {
  skip_if_no_sf()
  # two disconnected components: a left segment and a far-away right segment
  mk <- function(x1, y1, x2, y2)
    sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
  streets <- sf::st_sfc(mk(0, 0, 1, 0), mk(10, 0, 11, 0))
  net <- spatial_network(streets)
  expect_identical(net$stats[3], 2L)        # two components

  f <- write_points(data.frame(id = 1L, x = 0, y = 0))
  dest <- sf::st_sfc(sf::st_point(c(11, 0)))
  res <- tbl(f) |>
    spatial_route(net, to = dest, coords = c("x", "y"), geometry = FALSE) |>
    collect()
  expect_equal(nrow(res), 1L)
  expect_true(is.infinite(res$cost))
  unlink(f)
})

test_that("directed one-way edges block the reverse direction", {
  skip_if_no_sf()
  mk <- function(x1, y1, x2, y2)
    sf::st_linestring(rbind(c(x1, y1), c(x2, y2)))
  # a -> b -> c one-way chain (digitised direction is left to right)
  streets <- sf::st_sf(
    geometry = sf::st_sfc(mk(0, 0, 1, 0), mk(1, 0, 2, 0)))
  net <- spatial_network(streets, directed = TRUE)

  f <- write_points(data.frame(id = 1:2, x = c(0, 2), y = c(0, 0)))
  # forward origin (0,0) -> (2,0) reachable; reverse (2,0) -> (0,0) is not
  res <- tbl(f) |>
    spatial_route(net, to = sf::st_sfc(sf::st_point(c(2, 0)), sf::st_point(c(0, 0))),
                  geometry = FALSE, coords = c("x", "y")) |>
    collect()
  fwd <- res[res$id == 1L & res$destination == 1L, ]   # (0,0)->(2,0)
  rev <- res[res$id == 2L & res$destination == 2L, ]   # (2,0)->(0,0)
  expect_equal(fwd$cost, 2)
  expect_true(is.infinite(rev$cost))
  unlink(f)
})

test_that("service area reachable nodes match the budget", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  f <- write_points(data.frame(id = 1L, x = 0, y = 0))

  sfres <- tbl(f) |>
    spatial_service_area(net, cost = 1, output = "nodes",
                         coords = c("x", "y")) |>
    collect_sf()
  expect_equal(nrow(sfres), 1L)
  pts <- sf::st_coordinates(sfres)[, c("X", "Y")]
  # within cost 1 of (0,0): itself, (1,0), (0,1)
  reached <- unique(pts)
  expect_equal(nrow(reached), 3L)
  unlink(f)
})

test_that("service area bands nest from small to large budget", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  f <- write_points(data.frame(id = 1L, x = 0, y = 0))

  sfres <- tbl(f) |>
    spatial_service_area(net, cost = c(1, 2), output = "nodes",
                         coords = c("x", "y")) |>
    collect_sf()
  expect_equal(nrow(sfres), 2L)             # two bands
  n1 <- nrow(unique(sf::st_coordinates(sfres[sfres$band == 1, ])[, c("X", "Y")]))
  n2 <- nrow(unique(sf::st_coordinates(sfres[sfres$band == 2, ])[, c("X", "Y")]))
  expect_equal(n1, 3L)                       # itself + 2 neighbours
  expect_gte(n2, n1)                         # larger budget reaches at least as many
  unlink(f)
})

test_that("streaming is invariant to batch size", {
  skip_if_no_sf()
  net <- spatial_network(lattice_streets())
  pts <- data.frame(id = 1:6,
                    x = c(0, 1, 2, 0, 1, 2),
                    y = c(0, 0, 0, 1, 1, 1))
  dest <- sf::st_sfc(sf::st_point(c(2, 1)))

  f <- write_points(pts)
  one <- tbl(f) |>
    spatial_route(net, to = dest, coords = c("x", "y"), geometry = FALSE) |>
    collect()
  unlink(f)

  # force several row groups by writing a small batch_size
  f2 <- tempfile(fileext = ".vtr")
  write_vtr(pts, f2, batch_size = 2L)
  many <- tbl(f2) |>
    spatial_route(net, to = dest, coords = c("x", "y"), geometry = FALSE) |>
    collect()
  unlink(f2)

  one <- one[order(one$id), ]
  many <- many[order(many$id), ]
  expect_equal(many$cost, one$cost)
})

test_that("route matches igraph shortest paths when available", {
  skip_if_no_sf()
  skip_if_not_installed("igraph")
  net <- spatial_network(lattice_streets())

  # build the same undirected graph in igraph, weight 1 per edge
  edges <- rbind(
    c(1, 2), c(2, 3), c(4, 5), c(5, 6),      # node ids depend on snap order;
    c(1, 4), c(2, 5), c(3, 6))               # mirror lattice connectivity by coord
  # map lattice coords to vectra node ids via nearest-node snapping
  corner <- function(x, y) .network_snap(net, sf::st_sfc(sf::st_point(c(x, y))))
  ids <- list(bl = corner(0, 0), bm = corner(1, 0), br = corner(2, 0),
              tl = corner(0, 1), tm = corner(1, 1), tr = corner(2, 1))
  g <- igraph::make_empty_graph(n = net$n_nodes, directed = FALSE)
  add <- function(a, b) igraph::edges(c(a, b))
  el <- c(ids$bl, ids$bm, ids$bm, ids$br, ids$tl, ids$tm, ids$tm, ids$tr,
          ids$bl, ids$tl, ids$bm, ids$tm, ids$br, ids$tr)
  g <- igraph::add_edges(g, el)
  igraph::E(g)$weight <- 1

  f <- write_points(data.frame(id = 1L, x = 0, y = 0))
  res <- tbl(f) |>
    spatial_route(net, to = sf::st_sfc(sf::st_point(c(2, 1))),
                  geometry = FALSE, coords = c("x", "y")) |>
    collect()
  ig <- igraph::distances(g, v = ids$bl, to = ids$tr)[1, 1]
  expect_equal(res$cost, ig)
  unlink(f)
})
