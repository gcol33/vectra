# spatial_overlay(): self-overlay into disjoint pieces, composed with grouped
# slice_min for earliest-wins. Planar (CRS NA / 3857) so areas are exact.

skip_if_not_installed("sf")

mk <- function(x0, x1, y0 = 0, y1 = 1) sf::st_polygon(list(rbind(
  c(x0, y0), c(x1, y0), c(x1, y1), c(x0, y1), c(x0, y0))))

test_that("two overlapping polygons split into 3 pieces, overlap duplicated", {
  polys <- sf::st_sf(year = c(1990L, 2010L), geometry = sf::st_sfc(mk(0, 2), mk(1, 3)))
  df <- spatial_overlay(polys) |> collect()

  expect_equal(length(unique(df$piece_id)), 3L)   # 3 disjoint pieces
  expect_equal(nrow(df), 4L)                       # overlap carries both years
  tab <- table(df$piece_id)
  overlap <- as.integer(names(tab)[tab == 2])
  expect_equal(sort(df$year[df$piece_id == overlap]), c(1990L, 2010L))
  expect_true("geometry" %in% names(df))
})

test_that("earliest-year-wins via grouped slice_min keeps the whole piece", {
  polys <- sf::st_sf(year = c(1990L, 2010L), geometry = sf::st_sfc(mk(0, 2), mk(1, 3)))
  first <- spatial_overlay(polys) |>
    group_by(piece_id) |>
    slice_min(year, n = 1, with_ties = FALSE) |>
    collect_sf()

  expect_s3_class(first, "sf")
  expect_equal(nrow(first), 3L)
  # tolerance covers the ~1e-7 coordinate snap of the fixed-precision overlay.
  expect_equal(sort(as.numeric(sf::st_area(first))), c(1, 1, 1), tolerance = 1e-4)
  # overlap piece resolves to the earliest year (1990).
  expect_equal(sort(first$year), c(1990L, 1990L, 2010L))
})

test_that("partition area reconstructs the union (precision-robust, no slivers)", {
  geoms <- sf::st_sfc(mk(0, 3), mk(2, 5), mk(4, 7), mk(1, 2, 0, 3), mk(6, 8))
  polys <- sf::st_sf(id = seq_along(geoms), geometry = geoms)
  pieces <- spatial_overlay(polys) |>
    group_by(piece_id) |>
    slice_min(id, n = 1, with_ties = FALSE) |>
    collect_sf()

  a_union <- as.numeric(sf::st_area(sf::st_union(sf::st_geometry(polys))))
  a_part  <- sum(as.numeric(sf::st_area(pieces)))
  # tolerance covers the fixed-precision grid the overlay snaps to.
  expect_equal(a_part, a_union, tolerance = 1e-6)
})

test_that("disjoint overlap clusters are tiled independently", {
  # two separate overlapping pairs (union area 3 each) + a lone square (area 1)
  geoms <- sf::st_sfc(mk(0, 2), mk(1, 3), mk(10, 12), mk(11, 13), mk(20, 21))
  polys <- sf::st_sf(year = 1:5, geometry = geoms)
  pieces <- spatial_overlay(polys) |>
    group_by(piece_id) |>
    slice_min(year, n = 1, with_ties = FALSE) |>
    collect_sf()

  expect_equal(nrow(pieces), 7L)                  # 3 + 3 + 1 disjoint pieces
  expect_equal(sum(as.numeric(sf::st_area(pieces))), 7, tolerance = 1e-4)
})

test_that("vars selects carried attributes; bad input errors", {
  polys <- sf::st_sf(year = c(1L, 2L), keep = c("a", "b"),
                     geometry = sf::st_sfc(mk(0, 2), mk(1, 3)))
  df <- spatial_overlay(polys, vars = "year") |> collect()
  expect_true("year" %in% names(df))
  expect_false("keep" %in% names(df))

  expect_error(spatial_overlay(42), "must be an sf object")
  expect_error(spatial_overlay(polys, piece = "year"), "already exists")
  expect_error(spatial_overlay(polys, vars = "nope"), "not found")
})

test_that("CRS is carried onto the overlay node", {
  polys <- sf::st_sf(year = c(1L, 2L),
                     geometry = sf::st_sfc(mk(0, 2), mk(1, 3), crs = 3857))
  ov <- spatial_overlay(polys)
  expect_equal(sf::st_crs(collect_sf(ov)), sf::st_crs(3857))
})

test_that("coverage invariant: piece areas sum to each input's area", {
  # nested + offset squares -> high coverage multiplicity in the centre
  geoms <- sf::st_sfc(mk(0, 6, 0, 6), mk(1, 5, 1, 5), mk(2, 4, 2, 4),
                      mk(3, 7, 3, 7), mk(-1, 3, -1, 3))
  polys <- sf::st_sf(id = seq_along(geoms), geometry = geoms)
  df <- spatial_overlay(polys, vars = "id") |> collect()

  g     <- sf::st_as_sfc(structure(df$geometry, class = "WKB"), EWKB = FALSE)
  parea <- as.numeric(sf::st_area(g))
  cov   <- tapply(parea, df$id, sum)                 # area covered per source input
  cov   <- as.numeric(cov[order(as.integer(names(cov)))])
  truth <- as.numeric(sf::st_area(sf::st_geometry(polys)))
  expect_equal(cov, truth, tolerance = 1e-6)
})

test_that("invalid input polygons are repaired before overlay", {
  # self-intersecting bowtie repaired to two triangles, overlapped by a square
  bowtie <- sf::st_polygon(list(rbind(c(0, 0), c(2, 2), c(2, 0), c(0, 2), c(0, 0))))
  polys  <- sf::st_sf(id = 1:2, geometry = sf::st_sfc(bowtie, mk(1, 3, 0, 2)))
  df <- spatial_overlay(polys, vars = "id") |> collect()
  expect_gt(nrow(df), 0L)
  g <- sf::st_as_sfc(structure(df$geometry, class = "WKB"), EWKB = FALSE)
  expect_true(all(sf::st_is_valid(g)))
})
