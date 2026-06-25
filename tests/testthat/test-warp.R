# warp() -- resample/reproject a streamed VECR raster, checked against an
# analytic linear field (exact under bilinear/cubic), terra::project ground
# truth, and the resident (single-strip) path. The streamed strip pass must
# equal the resident result exactly: divergence there is a bug.

write_raster <- function(m, ext = c(0, 0, ncol(m), nrow(m)), tile = 512L,
                         dtype = "f64", epsg = 0L) {
  f <- tempfile(fileext = ".vec")
  vec_write_raster(m, f, dtype = dtype, extent = ext, tile_size = tile,
                   epsg = epsg)
  f
}

# Pixel-centre coordinates of a warp result matrix from its geotransform.
centres <- function(got) {
  gt <- attr(got, "gt")
  nr <- nrow(got); nc <- ncol(got)
  x <- gt[1L] + (col(got) - 0.5) * gt[2L]
  y <- gt[4L] + (row(got) - 0.5) * gt[6L]
  list(x = x, y = y)
}

# A spatial linear field is reproduced exactly by bilinear and cubic sampling.
lin_field <- function(x, y) 12 + 0.7 * x - 0.4 * y

# Build a source raster whose cell (i, j) holds the linear field at its centre.
lin_raster <- function(nr, nc, tile = 512L) {
  m <- matrix(0, nr, nc)
  for (i in seq_len(nr)) for (j in seq_len(nc)) {
    xc <- j - 0.5; yc <- nr - (i - 0.5)
    m[i, j] <- lin_field(xc, yc)
  }
  write_raster(m, tile = tile)
}

test_that("warping a raster onto its own grid reproduces it", {
  set.seed(11)
  m <- matrix(rnorm(20 * 16), 20, 16)
  f  <- write_raster(m, tile = 8L)
  ft <- write_raster(m, tile = 8L)
  on.exit(unlink(c(f, ft)))
  for (meth in c("near", "bilinear", "cubic")) {
    w <- warp(f, ft, method = meth)
    expect_equal(max(abs(w - m), na.rm = TRUE), 0, info = meth)
  }
  # near has no halo, so the identity warp is fully defined.
  expect_false(anyNA(warp(f, ft, method = "near")))
})

test_that("bilinear and cubic recover an analytic linear field", {
  f <- lin_raster(24, 20, tile = 8L); on.exit(unlink(f))
  # A finer, shifted target grid inside the source so kernels stay in-bounds.
  spec <- list(extent = c(2, 2, 18, 22), res = 0.5)
  for (meth in c("bilinear", "cubic")) {
    got <- warp(f, spec, method = meth)
    cc <- centres(got)
    exp <- lin_field(cc$x, cc$y)
    int <- !is.na(got)
    expect_equal(max(abs(got[int] - exp[int])), 0, tolerance = 1e-9, info = meth)
  }
})

test_that("nearest neighbour picks the containing source cell", {
  m <- matrix(1:36, 6, 6, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))
  # 2x2 target over the same extent (3x coarsen): target centres land on the
  # source centres x = 1.5/4.5, y = 4.5/1.5 -> source rows 2/5, cols 2/5.
  got <- warp(f, list(extent = c(0, 0, 6, 6), dims = c(2, 2)), method = "near")
  expect_equal(dim(got), c(2L, 2L))
  expect_equal(got[1, 1], m[2, 2])   # NW
  expect_equal(got[1, 2], m[2, 5])   # NE
  expect_equal(got[2, 1], m[5, 2])   # SW
  expect_equal(got[2, 2], m[5, 5])   # SE
})

test_that("targets beyond the source extent come back NA", {
  m <- matrix(1:16, 4, 4, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))
  got <- warp(f, list(extent = c(-4, -4, 8, 8), res = 1), method = "near")
  expect_true(is.na(got[1, 1]))                 # NW corner is off the source
  expect_false(is.na(got[6, 6]))                # inside the source footprint
})

test_that("near, bilinear and cubic match terra::project", {
  skip_if_not_installed("terra")
  z <- outer(1:30, 1:24, function(r, c) 5 + 0.3 * c + 0.2 * r + 0.01 * r * c)
  f <- write_raster(z, tile = 8L); on.exit(unlink(f))
  rs <- terra::rast(z, extent = terra::ext(0, 24, 0, 30), crs = "EPSG:3857")
  # Read the template's actual grid back so warp uses an identical extent+dims
  # (terra may adjust resolution to fit the extent).
  tmpl <- terra::rast(extent = terra::ext(0.3, 23.1, 0.5, 29.7),
                      resolution = 0.8, crs = "EPSG:3857")
  e <- as.vector(terra::ext(tmpl))
  spec <- list(extent = c(e[1], e[3], e[2], e[4]),
               dims = c(terra::ncol(tmpl), terra::nrow(tmpl)))
  for (meth in c("near", "bilinear", "cubic")) {
    got <- warp(f, spec, method = meth)
    ter <- matrix(terra::values(terra::project(rs, tmpl, method = meth)),
                  nrow = nrow(got), byrow = TRUE)
    int <- !is.na(got) & !is.na(ter)
    # terra stores results as float32, so compare at single-precision tolerance.
    expect_equal(max(abs(got[int] - ter[int])), 0, tolerance = 1e-5, info = meth)
  }
})

test_that("reprojection across CRS matches terra::project", {
  skip_if_not_installed("terra")
  skip_if_not_installed("sf")
  z <- outer(1:40, 1:32, function(r, c) 100 + 0.5 * c + 0.3 * r)
  # Source in Web Mercator over a small patch near the equator.
  f <- write_raster(z, ext = c(0, 0, 32000, 40000), tile = 8L, epsg = 3857)
  rs <- terra::rast(z, extent = terra::ext(0, 32000, 0, 40000), crs = "EPSG:3857")
  on.exit(unlink(f))

  tmpl <- terra::project(rs, "EPSG:4326")          # terra picks the target grid
  e <- as.vector(terra::ext(tmpl))
  spec <- list(crs = 4326, extent = c(e[1], e[3], e[2], e[4]),
               dims = c(terra::ncol(tmpl), terra::nrow(tmpl)))
  got <- warp(f, spec, method = "bilinear")
  ter <- matrix(terra::values(terra::project(rs, tmpl, method = "bilinear")),
                nrow = nrow(got), byrow = TRUE)
  int <- !is.na(got) & !is.na(ter)
  expect_gt(sum(int), 0.5 * length(got))           # most cells defined on both
  expect_equal(max(abs(got[int] - ter[int])), 0, tolerance = 1e-3)
})

test_that("streamed strips equal the single-strip result", {
  set.seed(12)
  m <- matrix(rnorm(37 * 29), 37, 29)
  f_s <- write_raster(m, tile = 8L)
  f_1 <- write_raster(m, tile = 512L)
  on.exit(unlink(c(f_s, f_1)))
  spec <- list(extent = c(1, 2, 26, 33), res = 0.7)
  for (meth in c("near", "bilinear", "cubic")) {
    a <- warp(f_s, spec, method = meth)
    b <- warp(f_1, spec, method = meth)
    expect_equal(a, b, tolerance = 1e-12, info = meth)
  }
})

test_that("streamed-to-.vec output equals the in-memory result", {
  set.seed(13)
  m <- matrix(rnorm(22 * 18), 22, 18)
  f <- write_raster(m, tile = 8L); on.exit(unlink(f))
  spec <- list(extent = c(1, 1, 17, 21), res = 0.6)
  mem <- warp(f, spec, method = "bilinear")

  of <- tempfile(fileext = ".vec"); on.exit(unlink(of), add = TRUE)
  h <- warp(f, spec, method = "bilinear", path = of, dtype = "f64")
  on.exit(vec_close_raster(h), add = TRUE)
  expect_equal(h$n_bands, 1L)
  disk <- vec_read_window(h, band = 1)
  expect_equal(max(abs(mem - disk), na.rm = TRUE), 0)
  expect_true(all(is.na(mem) == is.na(disk)))
})

test_that("a vectra_raster handle works as well as a path", {
  m <- matrix(1:16, 4, 4, byrow = TRUE)
  f <- write_raster(m); on.exit(unlink(f))
  rr <- vec_open_raster(f); on.exit(vec_close_raster(rr), add = TRUE)
  spec <- list(extent = c(0, 0, 4, 4), res = 0.5)
  expect_equal(warp(rr, spec, method = "bilinear"),
               warp(f,  spec, method = "bilinear"))
})

test_that("warp validates inputs", {
  m <- matrix(1:16, 4, 4)
  f <- write_raster(m); on.exit(unlink(f))
  expect_error(warp(f, list(extent = c(0, 0, 4, 4)), method = "near"),
               "res|dims")
  expect_error(warp(f, list(crs = 3857), method = "near"), "extent|res")
  expect_error(warp(f, 42L), "template")
  expect_error(warp(f, list(extent = c(0, 0, 4, 4), res = 1), method = "spline"),
               "should be one of|arg")
})
