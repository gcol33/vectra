# The native spatial_join() is a lazy SpatialJoinNode: matched and gathered in
# C, streaming into the next verb. Checked against the sf path, which is forced
# by wrapping the predicate in a closure that is not identical() to the exported
# sf function. crs = NA keeps both sides planar.

skip_if_not_installed("sf")

sq <- function(xmin, xmax, ymin, ymax)
  sf::st_polygon(list(rbind(c(xmin, ymin), c(xmax, ymin), c(xmax, ymax),
                            c(xmin, ymax), c(xmin, ymin))))

inter <- sf::st_intersects
via_sf <- function(a, b) inter(a, b)

# The sf path writes hex WKB in lower case, GEOS in upper case; the bytes agree.
sort_rows <- function(d) {
  if ("geometry" %in% names(d)) d$geometry <- toupper(d$geometry)
  d <- d[do.call(order, unname(as.list(d))), , drop = FALSE]
  rownames(d) <- NULL
  d
}

test_that("the native join is a lazy node that streams into the next verb", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(pid = 1:4, x = c(0.5, 1.5, 3.5, 9), y = 0.5), f)
  res <- sf::st_sf(rid = c("A", "B"),
                   geometry = sf::st_sfc(sq(0, 2, 0, 1), sq(1, 4, 0, 1)))
  node <- spatial_join(tbl(f), res, coords = c("x", "y"), crs = NA)
  expect_match(paste(capture.output(explain(node)), collapse = "\n"),
               "SpatialJoinNode")
  n <- spatial_join(tbl(f), res, coords = c("x", "y"), crs = NA) |>
    count(rid) |> collect_raw()
  n <- n[order(n$rid, na.last = TRUE), ]
  expect_equal(n$rid, c("A", "B", NA))
  expect_equal(n$n, c(2, 2, 1))
})

test_that("column layout, suffixes and order equal the sf path exactly", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(pid = 1:4, name = letters[1:4],
                       x = c(0.5, 1.5, 3.5, 9), y = 0.5), f)
  res <- sf::st_sf(name = c("A", "B"), val = c(10L, 20L),
                   geometry = sf::st_sfc(sq(0, 2, 0, 1), sq(1, 4, 0, 1)))
  for (lf in c(TRUE, FALSE)) for (kg in c(TRUE, FALSE)) {
    nat <- collect_raw(spatial_join(tbl(f), res, coords = c("x", "y"), crs = NA,
                                left = lf, keep_geom = kg))
    ref <- collect_raw(spatial_join(tbl(f), res, join = via_sf, coords = c("x", "y"),
                                crs = NA, left = lf, keep_geom = kg))
    info <- paste0("left=", lf, " keep_geom=", kg)
    expect_identical(names(nat), names(ref), info = info)
    expect_equal(sort_rows(nat), sort_rows(ref), info = info)
    expect_identical("geometry" %in% names(nat), kg, info = info)
  }
})

test_that("the coords point geometry is byte-identical to sf's hex WKB", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  xs <- c(0.5, -1.25, 1e-300, 123456.789, 3)
  ys <- c(0.5, 2, -7.5, -0.1, NA)
  write_vtr(data.frame(pid = seq_along(xs), x = xs, y = ys), f)
  res <- sf::st_sf(rid = "A", geometry = sf::st_sfc(sq(-1e6, 1e6, -1e6, 1e6)))
  nat <- collect_raw(spatial_join(tbl(f), res, coords = c("x", "y"), crs = NA))
  nat <- nat[order(nat$pid), ]
  ok <- !is.na(ys)
  want <- sf::st_as_binary(sf::st_as_sfc(sprintf("POINT (%.17g %.17g)", xs[ok], ys[ok])),
                           hex = TRUE)
  expect_identical(toupper(nat$geometry[ok]), toupper(unlist(want)))
  expect_identical(nat$geometry[ok], toupper(nat$geometry[ok]))
  expect_true(is.na(nat$geometry[!ok]))
  expect_true(is.na(nat$rid[!ok]))
})

test_that("geom= input: keep_geom, NA geometry and geometry-only y", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  polys <- sf::st_sfc(sq(0, 1, 0, 1), sq(2.5, 3.5, 0, 1), sq(8, 9, 8, 9))
  hex <- sf::st_as_binary(polys, hex = TRUE)
  write_vtr(data.frame(pid = 1:3, geometry = hex), f)
  res <- sf::st_sf(rid = c("A", "B"),
                   geometry = sf::st_sfc(sq(0, 3, 0, 3), sq(2, 4, 0, 2)))
  for (kg in c(TRUE, FALSE)) {
    nat <- collect_raw(spatial_join(tbl(f), res, crs = NA, keep_geom = kg))
    ref <- collect_raw(spatial_join(tbl(f), res, join = via_sf, crs = NA,
                                keep_geom = kg))
    expect_identical(names(nat), names(ref))
    expect_equal(sort_rows(nat), sort_rows(ref))
  }
  g <- tempfile(fileext = ".vtr"); on.exit(unlink(g), add = TRUE)
  write_vtr(data.frame(pid = 1:4, geometry = c(hex, NA)), g)
  na_row <- collect_raw(spatial_join(tbl(g), res, crs = NA))
  na_row <- na_row[na_row$pid == 4, ]
  expect_equal(nrow(na_row), 1)
  expect_true(is.na(na_row$rid) && is.na(na_row$geometry))
  expect_equal(nrow(collect_raw(spatial_join(tbl(g), res, crs = NA, left = FALSE) |>
                              filter(pid == 4))), 0)
  bare <- sf::st_sf(geometry = sf::st_geometry(res))
  nat <- collect_raw(spatial_join(tbl(f), bare, crs = NA, left = FALSE))
  ref <- collect_raw(spatial_join(tbl(f), bare, join = via_sf, crs = NA, left = FALSE))
  expect_identical(names(nat), names(ref))
  expect_equal(sort(nat$pid), sort(ref$pid))
})

test_that("many batches, a filtered child and fan-out past one output batch", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  set.seed(42)
  n <- 60000
  write_vtr(data.frame(pid = seq_len(n), x = runif(n, 0, 10), y = runif(n, 0, 10)),
            f, batch_size = 7001)
  # three layers that all cover the square, so every point matches three times
  # and one input batch yields more rows than a single output batch holds
  res <- sf::st_sf(rid = c("A", "B", "C"),
                   geometry = sf::st_sfc(sq(-1, 11, -1, 11), sq(-2, 12, -2, 12),
                                         sq(0, 5, -1, 11)))
  big <- tbl(f) |> filter(pid %% 2 == 0)
  out <- spatial_join(big, res, coords = c("x", "y"), crs = NA,
                      keep_geom = FALSE) |>
    count(rid) |> collect_raw()
  xs <- collect_raw(tbl(f) |> filter(pid %% 2 == 0))
  expect_equal(out$n[match(c("A", "B", "C"), out$rid)],
               c(n / 2, n / 2, sum(xs$x <= 5)))

  all_rows <- collect_raw(spatial_join(tbl(f), res, coords = c("x", "y"), crs = NA,
                                   keep_geom = FALSE))
  expect_equal(nrow(all_rows), 2 * n + sum(collect_raw(tbl(f))$x <= 5))
  # per left row, matches come out in ascending resident order
  first <- all_rows[all_rows$pid == all_rows$pid[1], "rid"]
  expect_identical(first, sort(first))
})

test_that("nearest and within-distance run on the node and equal sf", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(pid = 1:5, x = c(0.5, 2.5, 5, 9, -3),
                       y = c(0.5, 0.5, 5, 0, -3)), f)
  res <- sf::st_sf(rid = c("A", "B"),
                   geometry = sf::st_sfc(sq(0, 1, 0, 1), sq(4, 6, 4, 6)))
  pn <- sf::st_nearest_feature; wn <- function(a, b) pn(a, b)
  nat <- collect_raw(spatial_join(tbl(f), res, join = pn, coords = c("x", "y"), crs = NA))
  ref <- collect_raw(spatial_join(tbl(f), res, join = wn, coords = c("x", "y"), crs = NA))
  expect_equal(sort_rows(nat), sort_rows(ref))
  pw <- sf::st_is_within_distance; ww <- function(a, b, ...) pw(a, b, ...)
  nat <- collect_raw(spatial_join(tbl(f), res, join = pw, dist = 2,
                              coords = c("x", "y"), crs = NA))
  ref <- collect_raw(spatial_join(tbl(f), res, join = ww, dist = 2,
                              coords = c("x", "y"), crs = NA))
  expect_equal(sort_rows(nat), sort_rows(ref))
})

test_that("keep_geom is validated and bad columns error before consuming x", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(pid = 1:2, x = 1:2, y = 1:2), f)
  res <- sf::st_sf(rid = "A", geometry = sf::st_sfc(sq(0, 3, 0, 3)))
  x <- tbl(f)
  expect_error(spatial_join(x, res, coords = c("x", "z"), crs = NA), "not found")
  expect_error(spatial_join(x, res, coords = c("x", "y"), crs = NA,
                            keep_geom = NA), "keep_geom")
  expect_equal(nrow(collect_raw(spatial_join(x, res, coords = c("x", "y"), crs = NA))), 2)
})
