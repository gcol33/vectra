# Phase 1 round-trip tests for vec_write_raster / vec_open_raster /
# vec_read_window / vec_extract_points. These exercise:
#   - lossless float and integer round-trips
#   - tile boundaries (raster larger than tile_size, edge tiles)
#   - nodata handling
#   - multi-band writes
#   - geotransform-based point extraction
#   - terra round-trip when terra is installed (smoke test only — terra is
#     a Suggests dependency)

make_raster <- function(rows, cols, bands = 1L, fn = function(b, r, c) b * 1000 + r * cols + c) {
  if (bands == 1L) {
    m <- matrix(fn(1L, rep(seq_len(rows), times = cols), rep(seq_len(cols), each = rows)),
                nrow = rows, ncol = cols)
    return(m)
  }
  arr <- array(0, dim = c(rows, cols, bands))
  for (b in seq_len(bands)) {
    for (r in seq_len(rows)) {
      for (cc in seq_len(cols)) {
        arr[r, cc, b] <- fn(b, r, cc)
      }
    }
  }
  arr
}

test_that("f64 single-band round-trip preserves values exactly", {
  m <- matrix(rnorm(50 * 50), 50, 50)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_raster(m, tmp, dtype = "f64")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)
  expect_equal(r$width,  50L)
  expect_equal(r$height, 50L)
  expect_equal(r$dtype,  "f64")
  out <- vec_read_window(r)
  expect_equal(dim(out), c(50L, 50L))
  expect_equal(out, m, tolerance = 0)
})

test_that("f32 round-trip is lossless within float32 representable values", {
  m <- matrix(seq(-1, 1, length.out = 64 * 64), 64, 64)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f32")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  out <- vec_read_window(r)
  ## f32 round-trip is lossless against the f32-quantized original.
  m32 <- matrix(as.numeric(as.single(m)), 64, 64)
  expect_equal(out, m32, tolerance = 1e-7)
})

test_that("i16 round-trip is exact", {
  m <- matrix(sample(-1000:1000, 80 * 80, replace = TRUE), 80, 80)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "i16")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  out <- vec_read_window(r)
  expect_equal(out, m, tolerance = 0)
})

test_that("u8 round-trip is exact", {
  m <- matrix(sample(0:255, 200 * 200, replace = TRUE), 200, 200)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "u8")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  out <- vec_read_window(r)
  expect_equal(out, m, tolerance = 0)
})

test_that("raster larger than tile_size with edge tiles round-trips", {
  ## 600x800 > 512 tile -> 2x2 tile grid with edge tiles 88 wide / 288 tall.
  m <- matrix(runif(600 * 800), 600, 800)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f32", tile_size = 512L)
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  expect_equal(r$tile_size, 512L)
  out <- vec_read_window(r)
  expect_equal(out, matrix(as.numeric(as.single(m)), 600, 800),
               tolerance = 1e-7)
})

test_that("partial windows match the corresponding slab of the full raster", {
  m <- matrix(rnorm(300 * 400), 300, 400)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64", tile_size = 128L)
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })

  ## Window inside a single tile.
  win <- vec_read_window(r, rows = c(50, 100), cols = c(70, 120))
  expect_equal(dim(win), c(51L, 51L))
  expect_equal(win, m[50:100, 70:120], tolerance = 0)

  ## Window spanning multiple tiles.
  win2 <- vec_read_window(r, rows = c(100, 200), cols = c(200, 350))
  expect_equal(dim(win2), c(101L, 151L))
  expect_equal(win2, m[100:200, 200:350], tolerance = 0)
})

test_that("nodata pixels become NA on read", {
  m <- matrix(1:100, 10, 10)
  m[5, 5] <- -9999L
  m[8, 3] <- -9999L
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "i32", nodata = -9999)
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  expect_equal(r$nodata, -9999)
  out <- vec_read_window(r)
  expect_true(is.na(out[5, 5]))
  expect_true(is.na(out[8, 3]))
  expect_equal(out[1, 1], m[1, 1])
})

test_that("multi-band raster round-trips with band names", {
  arr <- array(seq_len(40 * 40 * 3) * 0.5, dim = c(40, 40, 3))
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(arr, tmp, dtype = "f32",
                   band_names = c("red", "green", "blue"))
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  expect_equal(r$n_bands, 3L)
  expect_equal(r$band_names, c("red", "green", "blue"))

  for (b in 1:3) {
    out <- vec_read_window(r, band = b)
    expect_equal(out, matrix(as.numeric(as.single(arr[, , b])), 40, 40),
                 tolerance = 1e-7,
                 info = sprintf("band %d", b))
  }
})

test_that("vec_extract_points returns pixel-center values via the geotransform", {
  ## 5 cols x 3 rows raster, extent (0,0)-(5,3); each pixel is 1x1.
  m <- matrix(1:15, nrow = 3, ncol = 5)   # row-major: row 1 = c(1, 4, 7, 10, 13)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64",
                   extent = c(0, 0, 5, 3))
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })

  ## Pixel centers: x = 0.5,1.5,..., y = 0.5,1.5,2.5 (with row 1 at the top).
  pts <- vec_extract_points(r,
    x = c(0.5, 4.5, 2.5),
    y = c(2.5, 0.5, 1.5))   # row 1 (top), row 3 (bottom), row 2 (middle)
  expect_equal(pts$band1[1], m[1, 1])  # top-left
  expect_equal(pts$band1[2], m[3, 5])  # bottom-right
  expect_equal(pts$band1[3], m[2, 3])  # middle
})

test_that("points outside the raster come back as NA", {
  m <- matrix(1, 4, 4)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64", extent = c(0, 0, 4, 4))
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  pts <- vec_extract_points(r, x = c(2, -1, 99), y = c(2, 2, 2))
  expect_equal(pts$band1[1], 1)
  expect_true(is.na(pts$band1[2]))
  expect_true(is.na(pts$band1[3]))
})

test_that("CRS / EPSG round-trips", {
  m <- matrix(0, 8, 8)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64", epsg = 31287L,
                   extent = c(0, 0, 8, 8))
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  expect_equal(r$epsg, 31287L)
})

test_that("compression='balanced' and 'max' produce identical decoded output", {
  set.seed(7)
  m <- matrix(rnorm(128 * 128), 128, 128)
  ref <- matrix(as.numeric(as.single(m)), 128, 128)

  outs <- list()
  sizes <- integer(3)
  for (i in seq_along(c("fast", "balanced", "max"))) {
    level <- c("fast", "balanced", "max")[i]
    tmp <- tempfile(fileext = ".vec")
    vec_write_raster(m, tmp, dtype = "f32", compression = level)
    sizes[i] <- file.size(tmp)
    r <- vec_open_raster(tmp)
    outs[[level]] <- vec_read_window(r)
    vec_close_raster(r)
    unlink(tmp)
  }

  ## All three levels must round-trip to the same f32-quantized matrix.
  expect_equal(outs$fast,     ref, tolerance = 1e-7)
  expect_equal(outs$balanced, ref, tolerance = 1e-7)
  expect_equal(outs$max,      ref, tolerance = 1e-7)

  ## max <= balanced <= fast for at least *most* inputs (not strictly true on
  ## every random tile, but on ≥ 100kB random gaussian data it should hold).
  ## Check the inequality with a small slack to avoid flake on edge cases.
  expect_lte(sizes[3], sizes[1] + 256)   # max <= fast
})

test_that("compression='max' is non-disastrous on a constant tile", {
  ## A constant raster should hit the predictor's zero-residual fast path
  ## under every codec spec — the resulting file should be tiny under any
  ## level (well under 4 KB for a 256x256 constant tile).
  m <- matrix(7.5, 256, 256)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f32", compression = "max")
  expect_lt(file.size(tmp), 4096)
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  out <- vec_read_window(r)
  expect_equal(out, matrix(as.numeric(as.single(m)), 256, 256))
})

test_that("vec_build_overviews adds N-1 reduced levels", {
  ## A 64x64 constant raster: building 4 levels gives sizes 64, 32, 16, 8.
  m <- matrix(7.0, 64, 64)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_raster(m, tmp, dtype = "f64", tile_size = 32L)

  vec_build_overviews(tmp, levels = 4L, resampling = "average")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)
  expect_equal(r$n_levels, 4L)

  ## Each level should round-trip the (constant) value.
  for (L in 0:3) {
    out <- vec_read_window(r, level = L)
    target_w <- max(1, ceiling(64 / 2^L))
    target_h <- max(1, ceiling(64 / 2^L))
    expect_equal(dim(out), c(target_h, target_w),
                 info = sprintf("level %d", L))
    expect_true(all(out == 7.0), info = sprintf("level %d", L))
  }
})

test_that("average resampling produces correct level-1 means", {
  ## Build a 4x4 raster where each 2x2 block has a distinct value;
  ## level 1 should be the per-block average (= the block's value).
  m <- matrix(0, 4, 4)
  m[1:2, 1:2] <- 1
  m[1:2, 3:4] <- 2
  m[3:4, 1:2] <- 3
  m[3:4, 3:4] <- 4
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64", tile_size = 64L)
  vec_build_overviews(tmp, levels = 2L, resampling = "average")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })

  l1 <- vec_read_window(r, level = 1L)
  expect_equal(dim(l1), c(2L, 2L))
  expect_equal(l1, matrix(c(1, 3, 2, 4), 2, 2))
})

test_that("nearest resampling takes the top-left pixel of each 2x2", {
  m <- matrix(c(1, 2, 3, 4,
                5, 6, 7, 8,
                9, 10, 11, 12,
                13, 14, 15, 16),
              nrow = 4, byrow = TRUE)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64", tile_size = 64L)
  vec_build_overviews(tmp, levels = 2L, resampling = "nearest")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })

  l1 <- vec_read_window(r, level = 1L)
  expect_equal(l1, matrix(c(1, 9, 3, 11), 2, 2))
})

test_that("mode resampling picks the most-frequent value", {
  m <- matrix(c(5, 5, 8, 8,
                5, 7, 8, 9,
                3, 3, 1, 1,
                3, 4, 2, 1),
              nrow = 4, byrow = TRUE)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "i32", tile_size = 64L)
  vec_build_overviews(tmp, levels = 2L, resampling = "mode")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  l1 <- vec_read_window(r, level = 1L)
  ## Top-left 2x2 = {5,5,5,7} -> mode 5
  ## Top-right     = {8,8,8,9} -> 8
  ## Bottom-left   = {3,3,3,4} -> 3
  ## Bottom-right  = {1,1,2,1} -> 1
  expect_equal(l1, matrix(c(5L, 3L, 8L, 1L), 2, 2))
})

test_that("vec_read_window rejects an out-of-range level", {
  m <- matrix(0, 8, 8)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })
  expect_error(vec_read_window(r, level = 5L), "level")
})

test_that("vec_build_overviews refuses to add fewer levels than already exist", {
  m <- matrix(0, 32, 32)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f64")
  vec_build_overviews(tmp, levels = 3L)
  expect_error(vec_build_overviews(tmp, levels = 2L), "already")
  unlink(tmp)
})

test_that("parallel tile decode matches serial decode (sufficient tiles)", {
  ## 800x800 with tile_size 128 -> 7x7 = 49 tiles. With OMP_NUM_THREADS>1
  ## the read path uses the parallel branch; result must match.
  set.seed(2)
  m <- matrix(rnorm(800 * 800), 800, 800)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_raster(m, tmp, dtype = "f64", tile_size = 128L)
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)
  out <- vec_read_window(r)
  expect_equal(out, m, tolerance = 0)
})

test_that("vec_to_tiff round-trips a single-band raster via terra", {
  skip_if_not_installed("terra")
  m <- matrix(seq(-1, 1, length.out = 30 * 40), 30, 40)
  vec_path <- tempfile(fileext = ".vec")
  tiff_path <- tempfile(fileext = ".tif")
  on.exit(unlink(c(vec_path, tiff_path)))

  vec_write_raster(m, vec_path, dtype = "f32",
                   extent = c(0, 0, 40, 30), epsg = 4326L)
  vec_to_tiff(vec_path, tiff_path, compression = "deflate")

  tr <- terra::rast(tiff_path)
  expect_equal(terra::nlyr(tr), 1L)
  expect_equal(terra::ncol(tr), 40L)
  expect_equal(terra::nrow(tr), 30L)
  ## Compare a sample of pixels.
  px <- terra::values(tr)
  expect_equal(as.numeric(px), as.numeric(as.single(t(m))),
               tolerance = 1e-6)
})

test_that("vec_to_tiff exports a 4-band raster terra reads correctly", {
  skip_if_not_installed("terra")
  arr <- array(0, dim = c(20, 25, 4))
  for (b in 1:4) arr[, , b] <- matrix(seq_len(20 * 25) * b, 20, 25)
  vec_path <- tempfile(fileext = ".vec")
  tiff_path <- tempfile(fileext = ".tif")
  on.exit(unlink(c(vec_path, tiff_path)))

  vec_write_raster(arr, vec_path, dtype = "f32",
                   extent = c(0, 0, 25, 20))
  vec_to_tiff(vec_path, tiff_path, compression = "deflate")

  tr <- terra::rast(tiff_path)
  expect_equal(terra::nlyr(tr), 4L)
  for (b in 1:4) {
    layer <- terra::values(tr[[b]])
    expect_equal(as.numeric(layer),
                 as.numeric(as.single(t(arr[, , b]))),
                 tolerance = 1e-6,
                 info = sprintf("band %d", b))
  }
})

test_that("vec_to_tiff propagates nodata", {
  skip_if_not_installed("terra")
  m <- matrix(1:25, 5, 5)
  m[3, 3] <- -9999L
  vec_path <- tempfile(fileext = ".vec")
  tiff_path <- tempfile(fileext = ".tif")
  on.exit(unlink(c(vec_path, tiff_path)))

  vec_write_raster(m, vec_path, dtype = "i32", nodata = -9999)
  vec_to_tiff(vec_path, tiff_path, compression = "none")

  tr <- terra::rast(tiff_path)
  vals <- terra::values(tr)[, 1]
  ## terra reads NoData as NA when GDAL_NODATA is recognised.
  expect_true(any(is.na(vals)))
})

test_that("time cube round-trips per slice", {
  ## 3 time steps x 2 bands x 8 rows x 10 cols.
  arr <- array(0, dim = c(8, 10, 2, 3))
  for (t in 1:3) {
    for (b in 1:2) {
      arr[, , b, t] <- matrix(seq_len(80) * (10 * t + b), 8, 10)
    }
  }
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(2020, 2021, 2022), path = tmp,
                      dtype = "f64")

  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)
  expect_equal(r$n_bands, 2L)

  for (t in c(2020, 2021, 2022)) {
    for (b in 1:2) {
      out <- vec_read_time_slice(r, time = t, band = b)
      expect_equal(out, arr[, , b, t - 2019],
                   info = sprintf("time=%d band=%d", t, b))
    }
  }
})

test_that("vec_read_time_slice errors on a missing time stamp", {
  arr <- array(0, dim = c(4, 4, 1, 2))
  arr[, , 1, 1] <- matrix(1:16, 4, 4)
  arr[, , 1, 2] <- matrix(101:116, 4, 4)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(1, 2), path = tmp, dtype = "i32")

  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)
  expect_error(vec_read_time_slice(r, time = 99), "no tiles match")
})

test_that("terra can ingest pixel values via point extraction (smoke test)", {
  skip_if_not_installed("terra")
  ## Build a known raster, write to .vec, sample at known points and
  ## compare to terra's interpretation of the same matrix at those points.
  set.seed(1)
  m <- matrix(rnorm(20 * 30), nrow = 20, ncol = 30)
  tmp <- tempfile(fileext = ".vec")
  vec_write_raster(m, tmp, dtype = "f32", extent = c(0, 0, 30, 20))
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) })

  tr <- terra::rast(m, extent = terra::ext(0, 30, 0, 20))
  pts <- data.frame(x = c(1.5, 15.5, 28.5),
                    y = c(19.5, 10.5, 0.5))
  ours <- vec_extract_points(r, pts$x, pts$y)$band1
  theirs <- terra::extract(tr, as.matrix(pts))[, 1]
  ## Both pipelines should report the f32-quantized matrix at pixel centers.
  expect_equal(ours, theirs, tolerance = 1e-6)
})

# ---------------------------------------------------------------------------
# Phase 6b — pixel-time-series transpose layout
# ---------------------------------------------------------------------------

# Helper: build a 4D cube where each (row, col, band, time) cell has a
# unique distinguishable value. We use the same formula in every test so
# round-trip equality checks are easy to interpret.
make_cube <- function(rows, cols, bands, n_time) {
  arr <- array(0, dim = c(rows, cols, bands, n_time))
  for (t in seq_len(n_time)) {
    for (b in seq_len(bands)) {
      arr[, , b, t] <- matrix(seq_len(rows * cols) * (10 * t + b),
                              rows, cols)
    }
  }
  arr
}

test_that("pixel-major time cube round-trips per slice", {
  arr <- make_cube(rows = 8, cols = 10, bands = 2, n_time = 3)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(2020, 2021, 2022), path = tmp,
                      dtype = "f64", layout = "pixel")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)

  expect_equal(vec_raster_layout(r), "pixel")
  expect_equal(vec_raster_times(r), c(2020, 2021, 2022))

  for (t_val in c(2020, 2021, 2022)) {
    for (b in 1:2) {
      out <- vec_read_time_slice(r, time = t_val, band = b)
      expect_equal(out, arr[, , b, t_val - 2019],
                   info = sprintf("time=%d band=%d", t_val, b))
    }
  }
})

test_that("pixel-major default layout is 'image' (no opt-in -> image)", {
  arr <- make_cube(rows = 6, cols = 6, bands = 1, n_time = 2)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(1, 2), path = tmp, dtype = "f64")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)
  expect_equal(vec_raster_layout(r), "image")
})

test_that("vec_read_pixel_series matches vec_read_time_slice (pixel layout)", {
  arr <- make_cube(rows = 7, cols = 9, bands = 2, n_time = 4)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = 1:4, path = tmp, dtype = "f64",
                      layout = "pixel")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)

  ## For every pixel, read_pixel_series should match the column of values
  ## you'd assemble by calling vec_read_time_slice four times.
  for (b in 1:2) {
    for (rr in c(1L, 4L, 7L)) {
      for (cc in c(1L, 5L, 9L)) {
        series <- vec_read_pixel_series(r, col = cc, row = rr, band = b)
        ## Direct check against the source array.
        expect_equal(series, arr[rr, cc, b, ],
                     info = sprintf("band=%d row=%d col=%d", b, rr, cc))
        ## Cross-check: assemble the same vector from time-slice reads.
        slice <- numeric(4)
        for (t in 1:4) {
          slice[t] <- vec_read_time_slice(r, time = t, band = b)[rr, cc]
        }
        expect_equal(series, slice, info = "slice consistency")
      }
    }
  }
})

test_that("vec_read_pixel_series falls back to image-major files", {
  ## For image-major, the function decodes one tile per distinct time
  ## stamp and extracts a single sample. Slow-but-correct.
  arr <- make_cube(rows = 5, cols = 5, bands = 1, n_time = 3)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(100, 200, 300), path = tmp,
                      dtype = "f64", layout = "image")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)

  expect_equal(vec_raster_layout(r), "image")
  series <- vec_read_pixel_series(r, col = 3L, row = 2L, band = 1L)
  expect_equal(series, arr[2, 3, 1, ])
  expect_equal(vec_raster_times(r), c(100, 200, 300))
})

test_that("pixel-major edge tiles round-trip with non-square remainders", {
  ## tile_size 4 against a 7x10 raster -> 2x3 tiles with edge tile widths
  ## of 4, 4, 2 and edge tile heights of 4, 3.
  arr <- make_cube(rows = 7, cols = 10, bands = 1, n_time = 2)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(1, 2), path = tmp,
                      dtype = "f64", tile_size = 4L, layout = "pixel")
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)

  ## Pixel in the bottom-right edge tile.
  series <- vec_read_pixel_series(r, col = 10L, row = 7L, band = 1L)
  expect_equal(series, arr[7, 10, 1, ])

  ## Full-raster window at t=2.
  out <- vec_read_time_slice(r, time = 2)
  expect_equal(out, arr[, , 1, 2])
})

test_that("pixel-major nodata round-trips through the time stack", {
  arr <- make_cube(rows = 5, cols = 5, bands = 1, n_time = 3)
  arr[3, 3, 1, 2] <- -9999    # nodata at single (row, col, time)
  arr[1, 1, 1, ] <- -9999     # nodata at all times for one pixel
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(1, 2, 3), path = tmp,
                      dtype = "i32", layout = "pixel", nodata = -9999)
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)

  ## The single-cell nodata becomes NA on read; the other two stamps
  ## round-trip the original cube values.
  s_centre <- vec_read_pixel_series(r, col = 3L, row = 3L, band = 1L)
  expect_equal(s_centre[1], arr[3, 3, 1, 1])
  expect_true(is.na(s_centre[2]))
  expect_equal(s_centre[3], arr[3, 3, 1, 3])

  ## All-nodata pixel returns NA at every time step.
  s_corner <- vec_read_pixel_series(r, col = 1L, row = 1L, band = 1L)
  expect_true(all(is.na(s_corner)))
})

test_that("pixel-major and image-major produce equivalent slices", {
  arr <- make_cube(rows = 6, cols = 8, bands = 2, n_time = 3)
  tmp_img <- tempfile(fileext = ".vec")
  tmp_pix <- tempfile(fileext = ".vec")
  on.exit(unlink(c(tmp_img, tmp_pix)))
  vec_write_time_cube(arr, times = c(10, 20, 30), path = tmp_img,
                      dtype = "f64", layout = "image")
  vec_write_time_cube(arr, times = c(10, 20, 30), path = tmp_pix,
                      dtype = "f64", layout = "pixel")
  ri <- vec_open_raster(tmp_img)
  rp <- vec_open_raster(tmp_pix)
  on.exit({ vec_close_raster(ri); vec_close_raster(rp); unlink(c(tmp_img, tmp_pix)) },
          add = TRUE)

  for (t_val in c(10, 20, 30)) {
    for (b in 1:2) {
      img <- vec_read_time_slice(ri, time = t_val, band = b)
      pix <- vec_read_time_slice(rp, time = t_val, band = b)
      expect_equal(img, pix, info = sprintf("t=%d b=%d", t_val, b))
    }
  }
})

test_that("vec_read_pixel_series via x/y coordinates uses the geotransform", {
  arr <- make_cube(rows = 4, cols = 5, bands = 1, n_time = 2)
  tmp <- tempfile(fileext = ".vec")
  on.exit(unlink(tmp))
  vec_write_time_cube(arr, times = c(1, 2), path = tmp,
                      dtype = "f64", layout = "pixel",
                      extent = c(0, 0, 5, 4))
  r <- vec_open_raster(tmp)
  on.exit({ vec_close_raster(r); unlink(tmp) }, add = TRUE)

  ## Pixel center (2.5, 1.5) -> col = 3, row = 3 (R indexing) since y=1.5
  ## maps to row = floor((1.5 - 4)/-1) + 1 = floor(2.5) + 1 = 3.
  ## col = floor((2.5 - 0)/1) + 1 = 3.
  series <- vec_read_pixel_series(r, x = 2.5, y = 1.5, band = 1L)
  expect_equal(series, arr[3, 3, 1, ])
})
