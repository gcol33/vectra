# spatial_join(partition = grid()) -- the two-sided streamed path. Both inputs
# are vectra_nodes binned to a grid and joined per shard. For point left
# geometries the result must equal the resident sf::st_join, and a single huge
# cell must reproduce the resident spatial_join exactly (streaming invariance).

# A square polygon of side `s` with lower-left corner (x0, y0).
sq <- function(x0, y0, s) sf::st_polygon(list(rbind(
  c(x0, y0), c(x0 + s, y0), c(x0 + s, y0 + s), c(x0, y0 + s), c(x0, y0))))

# Four corner squares (with one overlapping the first) over [0, 20], leaving
# gaps so some points stay unmatched and the overlap yields one-to-many hits.
make_polys <- function() {
  sf::st_sf(
    poly = 1:5,
    geometry = sf::st_sfc(
      sq(0, 0, 8), sq(10, 0, 8), sq(0, 10, 8), sq(10, 10, 8), sq(4, 4, 8)))
}

write_points <- function(pts) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)
  f
}

write_polys_wkb <- function(polys) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(
    poly = polys$poly,
    geometry = sf::st_as_binary(sf::st_geometry(polys), hex = TRUE)), f)
  f
}

# Collect a join node down to a comparable (id, poly) table, row-ordered.
key_table <- function(node) {
  d <- collect(node)
  d <- d[order(d$id, d$poly, na.last = TRUE), c("id", "poly"), drop = FALSE]
  rownames(d) <- NULL
  d
}

test_that("partitioned point-in-polygon join equals the resident join", {
  skip_if_not_installed("sf")
  set.seed(20)
  pts   <- cbind(runif(400, -2, 22), runif(400, -2, 22))
  polys <- make_polys()
  fp <- write_points(pts); fg <- write_polys_wkb(polys)
  on.exit(unlink(c(fp, fg)))

  resident <- tbl(fp) |>
    spatial_join(polys["poly"], coords = c("x", "y"), join = sf::st_intersects)
  parted <- tbl(fp) |>
    spatial_join(tbl(fg), coords = c("x", "y"), join = sf::st_intersects,
                 partition = grid(5))

  expect_equal(key_table(parted), key_table(resident))
})

test_that("a single huge cell reproduces the resident join exactly", {
  skip_if_not_installed("sf")
  set.seed(21)
  pts   <- cbind(runif(250, 0, 20), runif(250, 0, 20))
  polys <- make_polys()
  fp <- write_points(pts); fg <- write_polys_wkb(polys)
  on.exit(unlink(c(fp, fg)))

  resident <- tbl(fp) |>
    spatial_join(polys["poly"], coords = c("x", "y"))
  one_cell <- tbl(fp) |>
    spatial_join(tbl(fg), coords = c("x", "y"), partition = grid(1000))

  expect_equal(key_table(one_cell), key_table(resident))
})

test_that("an inner partitioned join drops the unmatched left rows", {
  skip_if_not_installed("sf")
  set.seed(22)
  pts   <- cbind(runif(300, -2, 22), runif(300, -2, 22))
  polys <- make_polys()
  fp <- write_points(pts); fg <- write_polys_wkb(polys)
  on.exit(unlink(c(fp, fg)))

  resident <- tbl(fp) |>
    spatial_join(polys["poly"], coords = c("x", "y"), left = FALSE)
  parted <- tbl(fp) |>
    spatial_join(tbl(fg), coords = c("x", "y"), left = FALSE,
                 partition = grid(4))

  kt <- key_table(parted)
  expect_false(anyNA(kt$poly))                 # inner: every row matched
  expect_equal(kt, key_table(resident))
})

test_that("the overlap region produces one row per covering polygon", {
  skip_if_not_installed("sf")
  # A point at (6, 6) sits in both square 1 (0,0,8) and square 5 (4,4,8).
  pts   <- cbind(c(6, 15), c(6, 2))
  polys <- make_polys()
  fp <- write_points(pts); fg <- write_polys_wkb(polys)
  on.exit(unlink(c(fp, fg)))

  parted <- tbl(fp) |>
    spatial_join(tbl(fg), coords = c("x", "y"), partition = grid(3))
  kt <- key_table(parted)
  expect_equal(sort(kt$poly[kt$id == 1]), c(1, 5))   # both covering squares
  expect_equal(kt$poly[kt$id == 2], 2)               # only square 2
})

test_that("partitioned join carries the result through collect_sf", {
  skip_if_not_installed("sf")
  set.seed(23)
  pts   <- cbind(runif(80, 0, 20), runif(80, 0, 20))
  polys <- make_polys()
  fp <- write_points(pts); fg <- write_polys_wkb(polys)
  on.exit(unlink(c(fp, fg)))

  out <- tbl(fp) |>
    spatial_join(tbl(fg), coords = c("x", "y"), crs = 4326,
                 partition = grid(6))
  sf_out <- collect_sf(out)
  expect_s3_class(sf_out, "sf")
  expect_equal(as.integer(sf::st_crs(sf_out)$epsg), 4326L)
})

test_that("grid() validates its arguments", {
  expect_error(grid(-1), "positive")
  expect_error(grid(0), "positive")
  expect_error(grid(c(1, 2, 3)), "one or two")
  expect_error(grid(1, origin = 0), "length-2")
  g <- grid(c(2, 4), origin = c(1, 1))
  expect_s3_class(g, "vectra_grid")
  expect_equal(g$cellsize, c(2, 4))
})

test_that("spatial_join rejects a bad partition or non-node y", {
  skip_if_not_installed("sf")
  fp <- write_points(cbind(1, 1)); on.exit(unlink(fp))
  expect_error(
    tbl(fp) |> spatial_join(make_polys(), coords = c("x", "y"),
                            partition = grid(1)),
    "vectra_node")
  expect_error(
    tbl(fp) |> spatial_join(tbl(fp), coords = c("x", "y"),
                            partition = 5),
    "grid")
})
