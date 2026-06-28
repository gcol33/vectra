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

test_that("reading from a GeoPackage in batches matches the in-memory overlay", {
  skip_if_not_installed("sf")
  sq <- function(x, y, s) sf::st_polygon(list(rbind(
    c(x, y), c(x+s, y), c(x+s, y+s), c(x, y+s), c(x, y))))
  g <- vector("list", 30L); yr <- integer(30L); k <- 0L
  for (i in 0:5) for (j in 0:4) { k <- k+1L; g[[k]] <- sq(i*600, j*600, 1000); yr[k] <- 1900L+k }
  x  <- sf::st_sf(year = yr, geometry = sf::st_sfc(g), crs = 3857)
  gp <- tempfile(fileext = ".gpkg"); on.exit(unlink(gp))
  sf::st_write(x, gp, "lyr", quiet = TRUE)

  pull <- function(o) {
    d <- as.data.frame(collect(o))
    d[order(d$piece_id, d$year), c("piece_id", "year", "geometry")]
  }
  ref  <- pull(spatial_overlay(x, vars = "year"))                                  # in-memory
  file <- pull(spatial_overlay(gp, layer = "lyr", vars = "year"))                  # file, one batch
  chnk <- pull(spatial_overlay(gp, layer = "lyr", vars = "year", read_chunk = 7L)) # file, many batches

  expect_equal(nrow(file), nrow(ref))
  expect_identical(file$piece_id, ref$piece_id)
  expect_identical(file$year, ref$year)
  expect_identical(file$geometry, ref$geometry)   # geometry byte-identical
  expect_identical(chnk$geometry, ref$geometry)   # batch count does not change output
})

test_that("file overlay needs layer or query, and query without grid asks for grid", {
  skip_if_not_installed("sf")
  x  <- sf::st_sf(year = 1L, geometry = sf::st_sfc(mk(0, 1)), crs = 3857)
  gp <- tempfile(fileext = ".gpkg"); on.exit(unlink(gp))
  sf::st_write(x, gp, "lyr", quiet = TRUE)
  expect_error(spatial_overlay(gp), "layer.*query")
  expect_error(spatial_overlay(gp, query = "SELECT * FROM lyr"), "grid")
})

test_that("st_write streams a resolved overlay and matches collect_sf", {
  skip_if_not_installed("sf")
  sq <- function(x, y, s) sf::st_polygon(list(rbind(
    c(x, y), c(x+s, y), c(x+s, y+s), c(x, y+s), c(x, y))))
  g <- vector("list", 30L); yr <- integer(30L); k <- 0L
  for (i in 0:5) for (j in 0:4) { k <- k+1L; g[[k]] <- sq(i*600, j*600, 1000); yr[k] <- 1900L+k }
  x <- sf::st_sf(year = yr, geometry = sf::st_sfc(g), crs = 3857)

  resolve <- function() spatial_overlay(x, vars = "year") |>
    group_by(piece_id) |> slice_min(year, n = 1, with_ties = FALSE)
  out <- tempfile(fileext = ".gpkg"); on.exit(unlink(out))
  sf::st_write(resolve(), out, crs = sf::st_crs(x), delete_dsn = TRUE, quiet = TRUE)
  B <- sf::st_read(out, quiet = TRUE)
  A <- collect_sf(resolve(), crs = sf::st_crs(x))

  key <- function(s) { o <- order(s$piece_id, s$year, round(as.numeric(sf::st_area(s)), 6)); s[o, ] }
  A <- key(A); B <- key(B)
  expect_equal(nrow(A), nrow(B))
  expect_identical(A$piece_id, B$piece_id)
  expect_identical(A$year, B$year)
  expect_equal(as.numeric(sf::st_area(A)), as.numeric(sf::st_area(B)), tolerance = 1e-9)
  expect_true(sf::st_crs(B) == sf::st_crs(x))   # same CRS (label may differ)
})
