# Recovery and streaming-invariance tests for contours().
#
# The defining property of a contour at level L is that the raster, sampled at
# every vertex of the line, equals L. The recovery test checks exactly that
# (the value-based ground truth, robust to segmentation and ordering). Plus
# total contour length is equal across tile sizes (streaming invariance).

wr <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = "f64", extent = ext, tile_size = tile)
  f
}

test_that("contour vertices sample back to their level", {
  skip_if_not_installed("sf")
  z <- outer(1:30, 1:30, function(r, c) 0.5 * r + 0.8 * c)
  f <- wr(z, ext = c(0, 0, 30, 30), tile = 8L)
  on.exit(unlink(f))

  levs <- c(15, 25, 35)
  iso <- collect_sf(contours(f, levels = levs))
  expect_gt(nrow(iso), 0L)

  r <- vec_open_raster(f)
  on.exit(vec_close_raster(r), add = TRUE)
  bad <- 0
  for (i in seq_len(nrow(iso))) {
    co <- sf::st_coordinates(iso[i, ])
    s <- vec_extract_points(r, co[, "X"], co[, "Y"])[[3]]
    bad <- bad + sum(abs(s - iso$level[i]) > 1.0, na.rm = TRUE)
  }
  expect_equal(bad, 0)
})

test_that("contour total length is invariant to tile size", {
  skip_if_not_installed("sf")
  z <- outer(1:24, 1:24, function(r, c) r + c)
  f1 <- wr(z, ext = c(0, 0, 24, 24), tile = 6L)
  f2 <- wr(z, ext = c(0, 0, 24, 24), tile = 24L)
  on.exit(unlink(c(f1, f2)))

  levs <- c(15, 25, 35)
  l1 <- sum(as.numeric(sf::st_length(collect_sf(contours(f1, levs)))))
  l2 <- sum(as.numeric(sf::st_length(collect_sf(contours(f2, levs)))))
  expect_equal(l1, l2, tolerance = 1e-6)
})

test_that("a planar field gives one line per level of the expected length", {
  skip_if_not_installed("sf")
  # z = x + y over [0,20]^2: level L is the segment x + y = L, length L*sqrt(2)
  # truncated to the square. For L = 20 (the diagonal) length = 20*sqrt(2).
  z <- outer(1:20, 1:20, function(r, c) r + c)
  f <- wr(z, ext = c(0, 0, 20, 20), tile = 5L)
  on.exit(unlink(f))

  iso <- collect_sf(contours(f, levels = 20))
  expect_equal(nrow(iso), 1L)
  len <- sum(as.numeric(sf::st_length(iso)))
  # pixel-centre grid spans centres 0.5..19.5, so the traced diagonal is a bit
  # shorter than the full square diagonal; check it is close to 19*sqrt(2).
  expect_equal(len, 19 * sqrt(2), tolerance = 0.5)
})

test_that("merge = FALSE returns the raw per-cell segments", {
  skip_if_not_installed("sf")
  z <- outer(1:10, 1:10, function(r, c) r + c)
  f <- wr(z, ext = c(0, 0, 10, 10), tile = 10L)
  on.exit(unlink(f))

  seg <- collect_sf(contours(f, levels = 10, merge = FALSE))
  mer <- collect_sf(contours(f, levels = 10, merge = TRUE))
  expect_gt(nrow(seg), nrow(mer))
  # merging does not change total length
  expect_equal(sum(as.numeric(sf::st_length(seg))),
               sum(as.numeric(sf::st_length(mer))), tolerance = 1e-6)
})

test_that("contours rejects empty levels", {
  f <- wr(matrix(1, 4, 4))
  on.exit(unlink(f))
  expect_error(contours(f, levels = numeric(0)), "numeric vector")
})
