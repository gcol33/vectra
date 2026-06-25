# Recovery test for the two-sided nearest-feature join on the grid partition.
#
# st_nearest_feature is not local to a cell, so the partition path searches the
# left point's 3x3 cell neighbourhood. With a cellsize at least the largest
# nearest distance, the streamed result must equal the resident st_join, and be
# equal across batch sizes (streaming invariance).

write_points <- function(df) {
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

write_target_wkb <- function(df) {
  g <- sf::st_as_binary(sf::st_geometry(sf::st_as_sf(df, coords = c("x", "y"))),
                        hex = TRUE)
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(tid = df$tid, geometry = g), f)
  f
}

test_that("two-sided nearest matches the resident nearest join", {
  skip_if_not_installed("sf")
  set.seed(1)
  n <- 300
  left <- data.frame(id = seq_len(n), x = runif(n, 0, 10), y = runif(n, 0, 10))
  targ <- data.frame(tid = 1:9,
                     x = rep(c(1.5, 5, 8.5), 3),
                     y = rep(c(1.5, 5, 8.5), each = 3))
  fl <- write_points(left); ft <- write_target_wkb(targ)
  on.exit(unlink(c(fl, ft)))

  got <- collect(tbl(fl) |>
    spatial_join(tbl(ft), coords = c("x", "y"),
                 join = sf::st_nearest_feature, partition = grid(5)))
  got <- got[order(got$id), ]

  lsf <- sf::st_as_sf(left, coords = c("x", "y"))
  tsf <- sf::st_as_sf(targ, coords = c("x", "y"))
  ref <- sf::st_join(lsf, tsf, join = sf::st_nearest_feature)
  ref <- ref[order(ref$id), ]

  expect_equal(got$tid, ref$tid)
})

test_that("two-sided nearest is invariant to the partition budget", {
  skip_if_not_installed("sf")
  set.seed(2)
  n <- 200
  left <- data.frame(id = seq_len(n), x = runif(n, 0, 20), y = runif(n, 0, 20))
  targ <- data.frame(tid = 1:16,
                     x = rep(seq(2.5, 17.5, 5), 4),
                     y = rep(seq(2.5, 17.5, 5), each = 4))
  fl <- write_points(left); ft <- write_target_wkb(targ)
  on.exit(unlink(c(fl, ft)))

  run <- function(budget) {
    old <- options(vectra.partition_budget = budget); on.exit(options(old))
    r <- collect(tbl(fl) |>
      spatial_join(tbl(ft), coords = c("x", "y"),
                   join = sf::st_nearest_feature, partition = grid(7)))
    r[order(r$id), "tid"]
  }
  expect_equal(run(20), run(1e6))
})
