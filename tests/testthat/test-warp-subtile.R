# warp() bounds the source window it reads for any one output block by splitting
# the block until the window fits vectra.warp_window_cells. A tiny budget forces
# heavy splitting (down to near single pixels); the warped result must be
# identical to the unsplit warp for every resampling method, including the ones
# whose kernels reach across sub-block boundaries (bilinear, cubic).

make_src <- function() {
  z <- outer(1:60, 1:80, function(r, c) sin(r / 5) * 40 + cos(c / 7) * 30 + r + c)
  f <- tempfile(fileext = ".vec")
  vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 80, 60))
  f
}

test_that("sub-tiled warp equals the unsplit warp for every method", {
  f <- make_src(); on.exit(unlink(f))
  tgt <- list(extent = c(0, 0, 80, 60), res = 0.37)   # finer, off-grid target

  for (m in c("near", "bilinear", "cubic")) {
    old <- options(vectra.warp_window_cells = 1e12)    # never split
    ref <- warp(f, tgt, method = m)
    options(old)

    old <- options(vectra.warp_window_cells = 50)      # split down to tiny blocks
    got <- warp(f, tgt, method = m)
    options(old)

    expect_equal(dim(got), dim(ref))
    expect_identical(got, ref)                          # bit-identical assembly
  }
})

test_that("sub-tiling holds across a multi-strip output", {
  f <- make_src(); on.exit(unlink(f))
  # A coarse tile_size forces several output strips; combined with a tiny window
  # budget every strip also splits internally.
  tgt <- list(extent = c(0, 0, 80, 60), res = 0.5)

  old <- options(vectra.warp_window_cells = 1e12)
  ref <- warp(f, tgt, method = "bilinear")
  options(old)

  old <- options(vectra.warp_window_cells = 30)
  got <- warp(f, tgt, method = "bilinear")
  options(old)

  expect_identical(got, ref)
})

test_that("partly off-source targets keep NA pixels under sub-tiling", {
  f <- make_src(); on.exit(unlink(f))
  # Target extent overhangs the source, so some output pixels have no source.
  tgt <- list(extent = c(-20, -10, 90, 70), res = 0.6)

  old <- options(vectra.warp_window_cells = 1e12)
  ref <- warp(f, tgt, method = "near")
  options(old)

  old <- options(vectra.warp_window_cells = 40)
  got <- warp(f, tgt, method = "near")
  options(old)

  expect_identical(is.na(got), is.na(ref))             # NA mask preserved
  expect_identical(got, ref)
})
