# Extract band values at (x, y) points from a .vec raster

Extract band values at (x, y) points from a .vec raster

## Usage

``` r
vec_extract_points(r, x, y)
```

## Arguments

- r:

  A `vectra_raster` from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md).

- x:

  Numeric vector of x coordinates in CRS units.

- y:

  Numeric vector of y coordinates, same length as `x`.

## Value

A `data.frame` with columns `x`, `y`, then one column per band (named
after `r$band_names` if recorded, otherwise `band1`, `band2`, ...). NA
marks pixels outside the raster or matching nodata.
