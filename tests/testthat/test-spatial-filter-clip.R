# spatial_filter (select by location) and spatial_clip (clip / erase) — streamed
# vector ops checked against sf in-memory ground truth, with planar CRS = NA so
# intersections and areas are exact Cartesian and the streamed path must equal
# the resident path.

skip_if_not_installed("sf")

make_square <- function(xmin, xmax, ymin, ymax) {
  sf::st_polygon(list(rbind(
    c(xmin, ymin), c(xmax, ymin), c(xmax, ymax),
    c(xmin, ymax), c(xmin, ymin))))
}

# ---- spatial_filter ---------------------------------------------------------

test_that("spatial_filter keeps exactly the points inside a resident region", {
  region <- sf::st_sfc(make_square(0, 1, 0, 1))
  pts <- data.frame(
    id = 1:5,
    x  = c(0.5, 0.9, 1.5, 2.0, 0.1),
    y  = c(0.5, 0.1, 0.5, 0.5, 0.9))   # ids 1, 2, 5 inside; 3, 4 outside
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f)

  res <- tbl(f) |>
    spatial_filter(region, coords = c("x", "y"), crs = NA) |>
    collect()
  expect_equal(sort(res$id), c(1L, 2L, 5L))
  # Schema preserved: no geometry column introduced for coords input.
  expect_equal(sort(names(res)), c("id", "x", "y"))
})

test_that("spatial_filter negate keeps the complement", {
  region <- sf::st_sfc(make_square(0, 1, 0, 1))
  pts <- data.frame(id = 1:5,
                    x = c(0.5, 0.9, 1.5, 2.0, 0.1),
                    y = c(0.5, 0.1, 0.5, 0.5, 0.9))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f)

  res <- tbl(f) |>
    spatial_filter(region, coords = c("x", "y"), crs = NA, negate = TRUE) |>
    collect()
  expect_equal(sort(res$id), c(3L, 4L))
})

test_that("spatial_filter matches sf across multi-batch streaming", {
  set.seed(11)
  region <- sf::st_sfc(make_square(0, 1, 0, 1))
  n <- 4000L
  pts <- data.frame(id = seq_len(n),
                    x = runif(n, -0.5, 2),
                    y = runif(n, -0.5, 2))
  sb_all <- sf::st_as_sf(pts, coords = c("x", "y"), crs = NA, remove = FALSE)
  want <- pts$id[lengths(sf::st_intersects(sb_all, region)) > 0L]

  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f, batch_size = 500L)            # several read batches
  old <- options(vectra.spatial_flush = 700)      # several spill flushes
  on.exit(options(old), add = TRUE)

  got <- tbl(f) |>
    spatial_filter(region, coords = c("x", "y"), crs = NA) |>
    collect()
  expect_equal(sort(got$id), sort(want))
})

test_that("spatial_filter works on a hex-WKB column and preserves schema", {
  polys <- sf::st_sfc(make_square(0, 1, 0, 1),
                      make_square(2, 3, 0, 1),
                      make_square(0.5, 1.5, 0, 1))
  df <- data.frame(pid = 1:3,
                   geometry = sf::st_as_binary(polys, hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  probe <- sf::st_sfc(make_square(0.9, 1.1, 0, 1))   # overlaps pid 1 and 3
  res <- tbl(f) |> spatial_filter(probe, crs = NA) |> collect()
  expect_equal(sort(res$pid), c(1L, 3L))
  expect_equal(sort(names(res)), c("geometry", "pid"))
})

# ---- spatial_clip -----------------------------------------------------------

test_that("spatial_clip clips streamed polygons to a resident mask", {
  polys <- sf::st_sf(
    pid = c("A", "B"),
    geometry = sf::st_sfc(make_square(0, 2, 0, 1), make_square(2, 4, 0, 1)))
  df <- data.frame(
    pid = polys$pid,
    geometry = sf::st_as_binary(sf::st_geometry(polys), hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  mask <- sf::st_sfc(make_square(1, 3, 0, 1))
  out <- tbl(f) |> spatial_clip(mask, crs = NA) |> collect_sf()
  out <- out[order(out$pid), ]
  # A clip = (1,2)x(0,1); B clip = (2,3)x(0,1): each area 1.
  expect_equal(sort(out$pid), c("A", "B"))
  expect_equal(as.numeric(sf::st_area(out)), c(1, 1), tolerance = 1e-9)
})

test_that("spatial_clip erase keeps the part outside the mask", {
  polys <- sf::st_sf(
    pid = c("A", "B"),
    geometry = sf::st_sfc(make_square(0, 2, 0, 1), make_square(2, 4, 0, 1)))
  df <- data.frame(
    pid = polys$pid,
    geometry = sf::st_as_binary(sf::st_geometry(polys), hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  mask <- sf::st_sfc(make_square(1, 3, 0, 1))
  out <- tbl(f) |> spatial_clip(mask, erase = TRUE, crs = NA) |> collect_sf()
  out <- out[order(out$pid), ]
  # A erase = (0,1)x(0,1); B erase = (3,4)x(0,1): each area 1.
  expect_equal(as.numeric(sf::st_area(out)), c(1, 1), tolerance = 1e-9)
})

test_that("spatial_clip matches sf across multi-batch streaming", {
  k <- 60L
  xs <- seq_len(k)
  geoms <- do.call(sf::st_sfc,
                   lapply(xs, function(i) make_square(i, i + 1, 0, 1)))
  df <- data.frame(pid = xs, geometry = sf::st_as_binary(geoms, hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f, batch_size = 7L)
  old <- options(vectra.spatial_flush = 11)
  on.exit(options(old), add = TRUE)

  mask <- sf::st_sfc(make_square(10.5, 40.5, 0, 1))
  got <- tbl(f) |> spatial_clip(mask, crs = NA) |> collect_sf()

  resident <- sf::st_sf(pid = xs, geometry = geoms)
  ref <- suppressWarnings(sf::st_intersection(resident, sf::st_union(mask)))
  expect_equal(sort(got$pid), sort(ref$pid))
  expect_equal(sum(as.numeric(sf::st_area(got))),
               sum(as.numeric(sf::st_area(ref))), tolerance = 1e-9)
})

# ---- validation -------------------------------------------------------------

test_that("spatial_filter and spatial_clip validate inputs", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = 1, y = 1), f)
  region <- sf::st_sfc(make_square(0, 1, 0, 1))
  expect_error(spatial_filter(42, region), "must be a vectra_node")
  expect_error(spatial_filter(tbl(f), data.frame(a = 1)), "must be an sf or sfc")
  expect_error(spatial_clip(42, region), "must be a vectra_node")
  expect_error(spatial_clip(tbl(f), data.frame(a = 1)), "must be an sf or sfc")
})
