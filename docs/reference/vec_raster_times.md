# Distinct time stamps stored in a .vec time cube

Returns the ascending vector of time stamps recorded for the given
(band, level). Pixel-major files store one consolidated table; image-
major files derive the list from the per-tile time field.

## Usage

``` r
vec_raster_times(r, band = 1L, level = 0L)
```

## Arguments

- r:

  A `vectra_raster`.

- band:

  1-based band index.

- level:

  Overview level.

## Value

Numeric vector of stamps (length 0 when the file has no time
information).
