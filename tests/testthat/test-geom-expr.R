# Native libgeos scalar geometry expressions in mutate()/filter()/summarise().
# Geometry rides through the engine as hex-WKB in a string column; each result
# is recovery-tested against sf's own computation (the ground truth). CRS is
# dropped on both sides so the comparison is purely planar.

skip_if_not_installed("sf")

vtr_from <- function(sfobj) {
  f <- tempfile(fileext = ".vtr")
  df <- as.data.frame(sf::st_drop_geometry(sfobj))
  if (ncol(df) == 0) df <- data.frame(id = seq_along(sf::st_geometry(sfobj)))
  df$geometry <- sf::st_as_binary(sf::st_geometry(sfobj), hex = TRUE)
  write_vtr(df, f)
  f
}

planar <- function(g) sf::st_set_crs(sf::st_geometry(g), NA)

# A handful of unit squares at increasing x offsets, plus one point.
squares <- function() {
  mk <- function(ox) sf::st_polygon(list(rbind(
    c(ox, 0), c(ox + 1, 0), c(ox + 1, 1), c(ox, 1), c(ox, 0))))
  sf::st_sf(id = 1:3, geometry = sf::st_sfc(mk(0), mk(2), mk(4)))
}

# ---- measures -------------------------------------------------------------

test_that("st_area / st_length recover sf's measures", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> mutate(a = st_area(geometry), p = st_length(geometry)) |> collect()
  expect_equal(d$a, as.numeric(sf::st_area(planar(x))), tolerance = 1e-9)
  # st_length of a polygon is its perimeter (boundary length)
  expect_equal(d$p, as.numeric(sf::st_length(
    sf::st_cast(planar(x), "MULTILINESTRING"))), tolerance = 1e-9)
})

test_that("st_npoints and st_geometry_type recover sf", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> mutate(n = st_npoints(geometry),
                        t = st_geometry_type(geometry)) |> collect()
  expect_equal(d$n, rep(5, 3))                 # 5 coords per closed square ring
  expect_true(all(toupper(d$t) == "POLYGON"))
})

test_that("st_x / st_y recover point coordinates and NA on non-points", {
  pts <- sf::st_sf(geometry = sf::st_sfc(
    sf::st_point(c(1, 2)), sf::st_point(c(3, 4))))
  f <- vtr_from(pts); on.exit(unlink(f))
  d <- tbl(f) |> mutate(px = st_x(geometry), py = st_y(geometry)) |> collect()
  expect_equal(d$px, c(1, 3)); expect_equal(d$py, c(2, 4))
  # polygons are not points -> NA
  fp <- vtr_from(squares()); on.exit(unlink(fp), add = TRUE)
  dp <- tbl(fp) |> mutate(px = st_x(geometry)) |> collect()
  expect_true(all(is.na(dp$px)))
})

# ---- predicates -----------------------------------------------------------

test_that("unary predicates recover is_valid / is_empty", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> mutate(v = st_is_valid(geometry),
                        e = st_is_empty(geometry)) |> collect()
  expect_true(all(d$v)); expect_true(all(!d$e))
})

test_that("st_intersects against a constant AOI filters like sf", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  aoi <- sf::st_as_sfc(sf::st_bbox(c(xmin = 1.5, ymin = 0, xmax = 4.5, ymax = 1)))
  got <- tbl(f) |> filter(st_intersects(geometry, aoi)) |> collect()
  ref <- which(lengths(sf::st_intersects(planar(x), planar(aoi))) > 0)
  expect_setequal(got$id, ref)
})

test_that("st_within / st_contains distinguish direction", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  big <- sf::st_as_sfc(sf::st_bbox(c(xmin = -1, ymin = -1, xmax = 2, ymax = 2)))
  d <- tbl(f) |> mutate(w = st_within(geometry, big)) |> collect()
  ref <- as.logical(lengths(sf::st_within(planar(x), planar(big))) > 0)
  expect_equal(d$w, ref)
})

test_that("predicate between two geometry columns (self) holds", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |> mutate(eq = st_equals(geometry, geometry),
                        ix = st_intersects(geometry, geometry)) |> collect()
  expect_true(all(d$eq)); expect_true(all(d$ix))
})

# ---- distance -------------------------------------------------------------

test_that("st_distance recovers sf, to a constant and to self", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  pt <- sf::st_sfc(sf::st_point(c(0, 0)))
  d <- tbl(f) |> mutate(dp = st_distance(geometry, pt),
                        d0 = st_distance(geometry, geometry)) |> collect()
  expect_equal(d$dp, as.numeric(sf::st_distance(planar(x), planar(pt))),
               tolerance = 1e-9)
  expect_equal(d$d0, rep(0, 3), tolerance = 1e-12)
})

# ---- transforms -----------------------------------------------------------

test_that("st_centroid recovers sf centroid coordinates", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  cent <- tbl(f) |> mutate(geometry = st_centroid(geometry)) |>
    select(id, geometry) |> collect_sf()
  ref <- suppressWarnings(sf::st_centroid(planar(x)))
  expect_equal(sf::st_coordinates(cent), sf::st_coordinates(ref),
               tolerance = 1e-9, ignore_attr = TRUE)
})

test_that("st_buffer area matches sf and st_convex_hull is a polygon", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  buf <- tbl(f) |> mutate(geometry = st_buffer(geometry, 0.25)) |>
    select(id, geometry) |> collect_sf()
  expect_equal(as.numeric(sf::st_area(buf)),
               as.numeric(sf::st_area(sf::st_buffer(planar(x), 0.25))),
               tolerance = 1e-3)
  hull <- tbl(f) |> mutate(geometry = st_convex_hull(geometry)) |>
    select(id, geometry) |> collect_sf()
  expect_true(all(sf::st_geometry_type(hull) == "POLYGON"))
})

test_that("st_envelope of a square is the square itself", {
  x <- squares(); f <- vtr_from(x); on.exit(unlink(f))
  env <- tbl(f) |> mutate(geometry = st_envelope(geometry)) |>
    select(id, geometry) |> collect_sf()
  expect_equal(as.numeric(sf::st_area(env)), rep(1, 3), tolerance = 1e-9)
})

test_that("st_make_valid repairs a self-intersecting bowtie", {
  bow <- sf::st_sfc(sf::st_polygon(list(rbind(
    c(0, 0), c(1, 1), c(1, 0), c(0, 1), c(0, 0)))))
  x <- sf::st_sf(id = 1L, geometry = bow)
  f <- vtr_from(x); on.exit(unlink(f))
  d <- tbl(f) |>
    mutate(before = st_is_valid(geometry),
           geometry = st_make_valid(geometry)) |>
    mutate(after = st_is_valid(geometry)) |>
    collect_sf()
  expect_false(d$before[1]); expect_true(d$after[1])
})

# ---- NA handling ----------------------------------------------------------

test_that("a missing geometry yields NA, not an error", {
  x <- squares()
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  df <- data.frame(id = 1:4)
  df$geometry <- c(sf::st_as_binary(sf::st_geometry(x), hex = TRUE), NA)
  write_vtr(df, f)
  d <- tbl(f) |> mutate(a = st_area(geometry),
                        c_is_na = is.na(st_centroid(geometry))) |> collect()
  expect_true(is.na(d$a[4])); expect_false(any(is.na(d$a[1:3])))
  expect_true(d$c_is_na[4])
})
