# Read a single time slice from a .vec time cube

Performs a linear scan of the index for tiles with `time == time` and
decodes the matching window. The lookup is O(n_tiles) per call — Phase
6's optimized hash-map lookup is a follow-up.

## Usage

``` r
vec_read_time_slice(r, time, band = 1L, level = 0L, cols = NULL, rows = NULL)
```

## Arguments

- r:

  A `vectra_raster` from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md).

- time:

  Time value to match (numeric/integer).

- band:

  Band index (1-based).

- level:

  Overview level. Default 0.

- cols, rows:

  1-based ranges, same as `vec_read_window`.

## Value

A numeric matrix.
