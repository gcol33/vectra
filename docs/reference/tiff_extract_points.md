# Extract raster values at point coordinates

Samples band values from a GeoTIFF at specific (x, y) locations using
the file's affine geotransform. Only the strips containing query points
are read, making this efficient for sparse point sets on large rasters.

## Usage

``` r
tiff_extract_points(path, x, y = NULL)
```

## Arguments

- path:

  Path to a GeoTIFF file.

- x:

  Numeric vector of x coordinates, or a data.frame / matrix with columns
  named `x` and `y`.

- y:

  Numeric vector of y coordinates (ignored if `x` is a data.frame).

## Value

A data.frame with columns `x`, `y`, `band1`, `band2`, etc. One row per
input point, in the same order as the input.

## Details

Points that fall outside the raster extent return `NA` for all bands.
Pixel assignment uses nearest-pixel rounding (i.e., the point is
assigned to the pixel whose center is closest).

## Examples

``` r
# \donttest{
f <- tempfile(fileext = ".tif")
df <- data.frame(x = as.double(rep(1:4, 3)),
                 y = as.double(rep(1:3, each = 4)),
                 band1 = as.double(1:12))
write_tiff(df, f)

# Sample at specific locations via data.frame
pts <- data.frame(x = c(2, 3), y = c(1, 2))
tiff_extract_points(f, pts)

# Or pass x and y separately
tiff_extract_points(f, x = c(2, 3), y = c(1, 2))
unlink(f)
# }
```
