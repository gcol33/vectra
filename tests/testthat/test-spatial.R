# Streamed spatial operations (spatial_map / spatial_join / collect_sf).
# Planar CRS = NA throughout so st_intersects / areas are exact Cartesian and
# the recovered values can be checked against hand-computed truth.

skip_if_not_installed("sf")

make_square <- function(xmin, xmax, ymin, ymax) {
  sf::st_polygon(list(rbind(
    c(xmin, ymin), c(xmax, ymin), c(xmax, ymax),
    c(xmin, ymax), c(xmin, ymin))))
}

# Two disjoint unit squares with a gap between them at x in (1, 2).
two_squares <- function() {
  sf::st_sf(
    pid = c("A", "B"),
    geometry = sf::st_sfc(make_square(0, 1, 0, 1),
                          make_square(2, 3, 0, 1)))
}

test_that("spatial_join recovers known point-in-polygon membership", {
  polys <- two_squares()

  # Truth: 1->A, 2->B, 3->none (gap), 4->A.
  pts <- data.frame(
    id = 1:4,
    x  = c(0.5, 2.5, 1.5, 0.1),
    y  = c(0.5, 0.5, 0.5, 0.9))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f)

  res <- tbl(f) |>
    spatial_join(polys, join = sf::st_intersects,
                 coords = c("x", "y"), crs = NA) |>
    collect()
  res <- res[order(res$id), ]

  expect_equal(nrow(res), 4L)
  expect_equal(res$pid, c("A", "B", NA, "A"))
  # Left join keeps the unmatched gap point and the coordinate columns.
  expect_equal(res$x, c(0.5, 2.5, 1.5, 0.1))
  expect_true("geometry" %in% names(res))
})

test_that("spatial_join inner (left = FALSE) drops unmatched rows", {
  polys <- two_squares()
  pts <- data.frame(id = 1:3, x = c(0.5, 1.5, 2.5), y = c(0.5, 0.5, 0.5))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f)

  res <- tbl(f) |>
    spatial_join(polys, coords = c("x", "y"), crs = NA, left = FALSE) |>
    collect()
  res <- res[order(res$id), ]

  expect_equal(res$id, c(1L, 3L))
  expect_equal(res$pid, c("A", "B"))
})

test_that("spatial_join survives multi-batch streaming and many spill flushes", {
  polys <- two_squares()

  set.seed(42)
  n <- 5000L
  # Half the points land in square A, half in square B, none in the gap.
  inA <- seq_len(n) %% 2L == 0L
  x <- ifelse(inA, runif(n, 0, 1), runif(n, 2, 3))
  y <- runif(n, 0, 1)
  pts <- data.frame(id = seq_len(n), x = x, y = y, want = ifelse(inA, "A", "B"))

  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f, batch_size = 1000L)   # 5 row groups -> 5 read batches

  old <- options(vectra.spatial_flush = 1500)  # force multiple run-file flushes
  on.exit(options(old), add = TRUE)

  res <- tbl(f) |>
    spatial_join(polys["pid"], coords = c("x", "y"), crs = NA) |>
    collect()

  expect_equal(nrow(res), n)
  res <- res[order(res$id), ]
  expect_equal(res$pid, res$want)
})

test_that("spatial_map buffers a point to a disk of the right area", {
  # A single point at the origin, geometry carried as hex WKB.
  pt <- sf::st_sfc(sf::st_point(c(0, 0)))
  df <- data.frame(id = 1L, geometry = sf::st_as_binary(pt, hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  out <- tbl(f) |> spatial_map(~ sf::st_buffer(.x, 1), crs = NA)
  sf_out <- collect_sf(out)

  expect_s3_class(sf_out, "sf")
  expect_equal(nrow(sf_out), 1L)
  # st_buffer's polygonal disk under-approximates pi; just check it is close.
  expect_equal(as.numeric(sf::st_area(sf_out)), pi, tolerance = 0.01)
})

test_that("spatial_map round-trips geometry unchanged under the identity map", {
  geoms <- sf::st_sfc(
    sf::st_point(c(1, 2)),
    sf::st_point(c(3, 4)),
    sf::st_point(c(5, 6)))
  df <- data.frame(id = 1:3, geometry = sf::st_as_binary(geoms, hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  out <- tbl(f) |> spatial_map(~ .x, crs = NA)
  sf_out <- collect_sf(out)
  sf_out <- sf_out[order(sf_out$id), ]

  coords <- sf::st_coordinates(sf_out)
  expect_equal(coords[, "X"], c(1, 3, 5))
  expect_equal(coords[, "Y"], c(2, 4, 6))
})

test_that("collect_sf rebuilds the carried CRS", {
  geoms <- sf::st_sfc(sf::st_point(c(0, 0)), crs = 4326)
  df <- data.frame(id = 1L, geometry = sf::st_as_binary(geoms, hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  out <- tbl(f) |> spatial_map(~ .x, crs = 4326)
  sf_out <- collect_sf(out)
  expect_equal(sf::st_crs(sf_out), sf::st_crs(4326))
})

test_that("spatial_join composes with offload() partitioning (both-sides-huge)", {
  polys <- two_squares()

  set.seed(7)
  n <- 2000L
  inA <- seq_len(n) %% 2L == 0L
  pts <- data.frame(
    id  = seq_len(n),
    x   = ifelse(inA, runif(n, 0, 1), runif(n, 2, 3)),
    y   = runif(n, 0, 1),
    cell = ifelse(inA, "west", "east"))   # a spatial grid key
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(pts, f)

  # Single-pass reference.
  ref <- tbl(f) |>
    spatial_join(polys["pid"], coords = c("x", "y"), crs = NA) |>
    collect()
  ref <- ref[order(ref$id), ]

  # Partition the stream by the grid key, join each shard independently,
  # then recombine. The union must reproduce the single-pass result.
  part <- offload(tbl(f), by = "cell")
  shard_res <- lapply(unclass(part), function(nd)
    nd |> spatial_join(polys["pid"], coords = c("x", "y"), crs = NA) |> collect())
  got <- do.call(rbind, shard_res)
  got <- got[order(got$id), ]

  expect_equal(nrow(got), n)
  expect_equal(got$pid, ref$pid)
})

test_that("spatial verbs validate their inputs", {
  polys <- two_squares()
  expect_error(spatial_map(42, ~ .x), "must be a vectra_node")
  expect_error(spatial_join(42, polys), "must be a vectra_node")

  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = 1, y = 1), f)
  expect_error(spatial_join(tbl(f), data.frame(a = 1)),
               "must be an sf object")
})
