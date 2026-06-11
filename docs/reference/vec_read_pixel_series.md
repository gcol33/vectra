# Read the full time series at a single pixel from a .vec time cube

Returns a numeric vector of length `n_time` — one value per time step
recorded in the file, in ascending time-stamp order.

## Usage

``` r
vec_read_pixel_series(
  r,
  x = NULL,
  y = NULL,
  col = NULL,
  row = NULL,
  band = 1L,
  level = 0L
)
```

## Arguments

- r:

  A `vectra_raster` from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md).

- x, y:

  Pixel coordinates. Either both `x` and `y` (CRS units; the
  geotransform is used to map to col/row) or both `col` and `row`
  (1-based pixel indices).

- col, row:

  1-based pixel coordinates (alternative to x/y).

- band:

  Band index (1-based).

- level:

  Overview level. Default 0.

## Value

A numeric vector of length `n_time`. NA marks pixels outside the raster
or matching nodata. The corresponding time stamps can be obtained from
`vec_raster_times(r, band, level)`.

## Details

For pixel-major files (written with
`vec_write_time_cube(layout = "pixel")`) this is the optimal access
pattern: a single tile decode yields all time values for the pixel. For
image-major files the reader scans the index for distinct time stamps,
decodes one spatial tile per stamp, and extracts the pixel from each —
correct but `n_time` slower than the optimal layout.
