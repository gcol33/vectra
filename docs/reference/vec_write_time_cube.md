# Write a 4D time-cube raster to .vec

Each (band, time) combination becomes a stack of tiles tagged with the
chosen time stamp. Stamps are stored as int64 in the per-tile index
entry; a value of `0` is reserved for "untimed" so this writer remaps
any caller-supplied 0 to 1 internally.

## Usage

``` r
vec_write_time_cube(
  x,
  times,
  path,
  dtype = "f32",
  tile_size = 512L,
  layout = c("image", "pixel"),
  extent = NULL,
  gt = NULL,
  epsg = 0L,
  nodata = NA_real_,
  band_names = NULL,
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- x:

  Numeric 4D array `c(rows, cols, bands, time)`.

- times:

  Numeric/integer vector with `length(times) == dim(x)[4]`, in the unit
  of your choice (epoch ms, year, step index).

- path:

  Output `.vec` path.

- dtype:

  Storage dtype (see `vec_write_raster`).

- tile_size:

  Tile edge in pixels.

- layout:

  Tile layout — one of `"image"` (default; one tile per
  `(band, time, ty, tx)`, optimal for "give me one full image at time T"
  reads) or `"pixel"` (Phase 6b; one tile per `(band, ty, tx)` holding
  the full time stack as `[tw*th, n_time]`, optimal for "give me the
  time series at pixel `(x, y)`" reads).

- extent, gt, epsg, nodata, band_names, compression:

  Same semantics as
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md).

## Value

Invisible `NULL`.
