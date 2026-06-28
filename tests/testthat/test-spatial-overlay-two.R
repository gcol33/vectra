# spatial_overlay(x, y, how=): two-layer overlay into a planar partition carrying
# attributes from both layers. Planar (CRS NA / 3857) so areas are exact.
#
# Fixtures are two unit-height squares that overlap on [1,2]:
#   x = [0,2] x [0,1]   y = [1,3] x [0,1]
# so the partition is three unit pieces: x-only [0,1], both [1,2], y-only [2,3].

skip_if_not_installed("sf")

mk <- function(x0, x1, y0 = 0, y1 = 1) sf::st_polygon(list(rbind(
  c(x0, y0), c(x1, y0), c(x1, y1), c(x0, y1), c(x0, y0))))

xl <- sf::st_sf(xv = "A", geometry = sf::st_sfc(mk(0, 2)))
yl <- sf::st_sf(yv = "Z", geometry = sf::st_sfc(mk(1, 3)))

areas <- function(s) sort(as.numeric(sf::st_area(s)))

test_that("intersection keeps only the overlap, with both layers' attributes", {
  d <- spatial_overlay(xl, yl, how = "intersection") |> collect_sf()
  expect_equal(nrow(d), 1L)
  expect_equal(sum(as.numeric(sf::st_area(d))), 1, tolerance = 1e-4)
  expect_equal(d$xv, "A")
  expect_equal(d$yv, "Z")
})

test_that("union keeps every piece, absent side filled with NA", {
  d <- spatial_overlay(xl, yl, how = "union") |> collect_sf()
  expect_equal(nrow(d), 3L)
  expect_equal(areas(d), c(1, 1, 1), tolerance = 1e-4)
  expect_equal(sum(is.na(d$xv)), 1L)                    # the y-only piece
  expect_equal(sum(is.na(d$yv)), 1L)                    # the x-only piece
  expect_equal(sum(!is.na(d$xv) & !is.na(d$yv)), 1L)    # the overlap
})

test_that("identity keeps all of x split by y, drops y-only pieces", {
  d <- spatial_overlay(xl, yl, how = "identity") |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_equal(sum(as.numeric(sf::st_area(d))), 2, tolerance = 1e-4)
  expect_true(all(d$xv == "A"))                         # every piece has an x record
  expect_equal(sum(is.na(d$yv)), 1L)                    # the part of x outside y
})

test_that("symdiff keeps the parts in exactly one layer", {
  d <- spatial_overlay(xl, yl, how = "symdiff") |> collect_sf()
  expect_equal(nrow(d), 2L)
  expect_equal(sum(as.numeric(sf::st_area(d))), 2, tolerance = 1e-4)
  expect_true(all(xor(is.na(d$xv), is.na(d$yv))))       # exactly one side per piece
})

test_that("intersection matches sf::st_intersection on area", {
  old <- sf::sf_use_s2(FALSE); on.exit(sf::sf_use_s2(old))
  ref  <- suppressWarnings(sf::st_intersection(xl, yl))
  ours <- spatial_overlay(xl, yl, how = "intersection") |> collect_sf()
  expect_equal(sum(as.numeric(sf::st_area(ours))),
               sum(as.numeric(sf::st_area(ref))), tolerance = 1e-4)
})

test_that("shared column names are disambiguated with .x / .y", {
  x2 <- sf::st_sf(id = 1L, geometry = sf::st_sfc(mk(0, 2)))
  y2 <- sf::st_sf(id = 9L, geometry = sf::st_sfc(mk(1, 3)))
  d  <- spatial_overlay(x2, y2, how = "intersection") |> collect()
  expect_true(all(c("id.x", "id.y") %in% names(d)))
  expect_equal(d$id.x, 1L)
  expect_equal(d$id.y, 9L)
})

test_that("vars and vars_y select the carried columns per layer", {
  x3 <- sf::st_sf(a = 1L, b = 2L, geometry = sf::st_sfc(mk(0, 2)))
  y3 <- sf::st_sf(c = 3L, d = 4L, geometry = sf::st_sfc(mk(1, 3)))
  d  <- spatial_overlay(x3, y3, vars = "a", vars_y = "c", how = "intersection") |> collect()
  expect_true(all(c("a", "c") %in% names(d)))
  expect_false(any(c("b", "d") %in% names(d)))
})

test_that("a CRS mismatch is rejected", {
  xc <- sf::st_sf(g = 1L, geometry = sf::st_sfc(mk(0, 2), crs = 3857))
  yc <- sf::st_sf(h = 2L, geometry = sf::st_sfc(mk(1, 3), crs = 4326))
  expect_error(spatial_overlay(xc, yc), "share a CRS")
})

test_that("the shared CRS is carried onto the overlay node", {
  xc <- sf::st_sf(g = 1L, geometry = sf::st_sfc(mk(0, 2), crs = 3857))
  yc <- sf::st_sf(h = 2L, geometry = sf::st_sfc(mk(1, 3), crs = 3857))
  ov <- spatial_overlay(xc, yc, how = "intersection")
  expect_equal(sf::st_crs(collect_sf(ov)), sf::st_crs(3857))
})

test_that("a non-overlapping intersection is a typed empty node", {
  xa <- sf::st_sf(xv = "A", geometry = sf::st_sfc(mk(0, 1)))
  yb <- sf::st_sf(yv = "Z", geometry = sf::st_sfc(mk(5, 6)))
  d  <- spatial_overlay(xa, yb, how = "intersection") |> collect()
  expect_equal(nrow(d), 0L)
  expect_true(all(c("xv", "yv", "piece_id", "geometry") %in% names(d)))
})

# A self-overlay reached two ways: spatial_overlay(x) keeps one piece-row per
# covering source (so the overlap row appears once per year), while
# spatial_overlay(x, x, how="union") collapses each face to one geometry and
# pairs the covering x- and y-records. The partition itself -- the set of pieces
# and their areas -- is the same; only the row multiplicity differs.
test_that("union against a copy reproduces the self-union partition", {
  geoms <- sf::st_sfc(mk(0, 2), mk(1, 3))
  x <- sf::st_sf(year = c(1L, 2L), geometry = geoms)
  piece_areas <- function(o) {
    d <- collect(o)
    d <- d[!duplicated(d$piece_id), ]
    g <- sf::st_as_sfc(structure(d$geometry, class = "WKB"), EWKB = FALSE)
    sort(round(as.numeric(sf::st_area(g)), 6))
  }
  self <- piece_areas(spatial_overlay(x, vars = "year"))
  both <- piece_areas(spatial_overlay(x, x, vars = "year", vars_y = "year", how = "union"))
  expect_equal(both, self, tolerance = 1e-6)
})

test_that("reading y from a GeoPackage matches the in-memory overlay", {
  xc <- sf::st_sf(xv = "A", geometry = sf::st_sfc(mk(0, 2), crs = 3857))
  yc <- sf::st_sf(yv = "Z", geometry = sf::st_sfc(mk(1, 3), crs = 3857))
  gp <- tempfile(fileext = ".gpkg"); on.exit(unlink(gp))
  sf::st_write(yc, gp, "lyr", quiet = TRUE)
  pull <- function(o) {
    d <- as.data.frame(collect(o))
    d[order(d$piece_id), c("piece_id", "xv", "yv", "geometry")]
  }
  ref  <- pull(spatial_overlay(xc, yc, how = "union"))
  file <- pull(spatial_overlay(xc, gp, layer_y = "lyr", how = "union"))
  expect_equal(nrow(file), nrow(ref))
  expect_identical(file$geometry, ref$geometry)
  expect_identical(file$xv, ref$xv)
  expect_identical(file$yv, ref$yv)
})
