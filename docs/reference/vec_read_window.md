# Read a window of pixels from a .vec raster

Decodes only the tiles overlapping the requested window. Pixels outside
the raster extent come back as `NA`.

## Usage

``` r
vec_read_window(r, band = 1L, level = 0L, cols = NULL, rows = NULL)
```

## Arguments

- r:

  A `vectra_raster` from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md).

- band:

  Band index (1-based). Default 1.

- level:

  Overview level — 0 = full resolution, 1 = half, 2 = quarter, etc. Must
  be \< `r$n_levels` (which is 1 unless
  [`vec_build_overviews()`](https://gillescolling.com/vectra/reference/vec_build_overviews.md)
  has been run on the file).

- cols:

  1-based column range `c(col_min, col_max)`. Inclusive. Coordinates are
  in the chosen level's pixel grid (so at level 1 the raster is half as
  wide). Default `c(1, level_width)`.

- rows:

  1-based row range `c(row_min, row_max)`. Inclusive. Default
  `c(1, level_height)`.

## Value

A numeric matrix with `nrow = row_max - row_min + 1` and
`ncol = col_max - col_min + 1`. Nodata pixels become `NA`.
