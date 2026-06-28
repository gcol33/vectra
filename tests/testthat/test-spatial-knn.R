# spatial_knn(): k nearest resident-y features per streamed left row, returned
# long with rank, identifier, and distance.

skip_if_not_installed("sf")

vtr_pts <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

# four resident neighbours on the x-axis at x = 0, 1, 2, 3
nbrs <- function() sf::st_sf(
  town = c("a", "b", "c", "d"),
  geometry = sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 0)),
                        sf::st_point(c(2, 0)), sf::st_point(c(3, 0))))

test_that("k=1 returns the single nearest neighbour and its distance", {
  f <- vtr_pts(data.frame(id = c(1L, 2L), x = c(0.1, 2.9), y = c(0, 0)))
  on.exit(unlink(f))
  d <- tbl(f) |> spatial_knn(nbrs(), k = 1, coords = c("x", "y")) |> collect()
  expect_equal(nrow(d), 2L)
  expect_equal(d$rank, c(1L, 1L))
  expect_equal(d$neighbor, c(1L, 4L))         # nearest to 0.1 is x=0, to 2.9 is x=3
  expect_equal(d$distance, c(0.1, 0.1), tolerance = 1e-9)
})

test_that("k=2 returns two ranked neighbours per left feature in order", {
  f <- vtr_pts(data.frame(id = 1L, x = 0.2, y = 0))
  on.exit(unlink(f))
  d <- tbl(f) |> spatial_knn(nbrs(), k = 2, coords = c("x", "y")) |> collect()
  expect_equal(nrow(d), 2L)
  expect_equal(d$rank, c(1L, 2L))
  expect_equal(d$neighbor, c(1L, 2L))         # x=0 then x=1
  expect_equal(d$distance, c(0.2, 0.8), tolerance = 1e-9)
  expect_true(all(diff(d$distance) >= 0))     # non-decreasing with rank
})

test_that("y_id labels neighbours by a y column", {
  f <- vtr_pts(data.frame(id = 1L, x = 0.1, y = 0))
  on.exit(unlink(f))
  d <- tbl(f) |>
    spatial_knn(nbrs(), k = 1, coords = c("x", "y"), y_id = "town") |> collect()
  expect_equal(d$neighbor, "a")
})

test_that("k is capped at the number of y features", {
  f <- vtr_pts(data.frame(id = 1L, x = 0.5, y = 0))
  on.exit(unlink(f))
  d <- tbl(f) |> spatial_knn(nbrs(), k = 10, coords = c("x", "y")) |> collect()
  expect_equal(nrow(d), 4L)
  expect_equal(sort(d$neighbor), 1:4)
})

test_that("custom output column names are honoured", {
  f <- vtr_pts(data.frame(id = 1L, x = 0.1, y = 0))
  on.exit(unlink(f))
  d <- tbl(f) |>
    spatial_knn(nbrs(), k = 1, coords = c("x", "y"),
                id_col = "nb", dist_col = "dkm", rank_col = "rk") |> collect()
  expect_true(all(c("nb", "dkm", "rk") %in% names(d)))
})

test_that("left attributes ride through, replicated per neighbour", {
  f <- vtr_pts(data.frame(id = 7L, tag = "z", x = 0.2, y = 0))
  on.exit(unlink(f))
  d <- tbl(f) |> spatial_knn(nbrs(), k = 3, coords = c("x", "y")) |> collect()
  expect_equal(d$id, rep(7L, 3))
  expect_equal(d$tag, rep("z", 3))
})

test_that("hex-WKB point geometry input works and carries CRS", {
  pts <- sf::st_sfc(sf::st_point(c(0.1, 0)), sf::st_point(c(2.9, 0)), crs = 3857)
  x <- sf::st_sf(id = c(1L, 2L), geometry = pts)
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(id = x$id,
                       geometry = sf::st_as_binary(pts, hex = TRUE)), f)
  on.exit(unlink(f))
  y <- sf::st_set_crs(nbrs(), 3857)
  d <- tbl(f) |> spatial_knn(y, k = 1, crs = 3857) |> collect_sf()
  expect_equal(d$neighbor, c(1L, 4L))
  expect_equal(sf::st_crs(d), sf::st_crs(3857))
})

test_that("invalid inputs are rejected", {
  f <- vtr_pts(data.frame(id = 1L, x = 0, y = 0))
  on.exit(unlink(f))
  expect_error(spatial_knn(data.frame(a = 1), nbrs()), "vectra_node")
  expect_error(spatial_knn(tbl(f), data.frame(a = 1)), "sf")
  expect_error(collect(spatial_knn(tbl(f), nbrs(), k = 0, coords = c("x", "y"))),
               "positive")
  expect_error(
    collect(spatial_knn(tbl(f), nbrs(), coords = c("x", "y"), y_id = "nope")),
    "not found")
})
