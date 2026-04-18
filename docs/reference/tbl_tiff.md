# Create a lazy table reference from a GeoTIFF raster

Opens a GeoTIFF file and returns a lazy query node. Each pixel becomes a
row with columns `x`, `y`, `band1`, `band2`, etc. Coordinates are pixel
centers derived from the affine geotransform. NoData values become `NA`.

## Usage

``` r
tbl_tiff(path, batch_size = .TIFF_BATCH_SIZE)
```

## Arguments

- path:

  Path to a GeoTIFF file.

- batch_size:

  Number of raster rows per batch (default 256).

## Value

A `vectra_node` object representing a lazy scan of the raster.

## Details

Use `filter(x >= ..., y <= ...)` for extent-based cropping and
`filter(band1 > ...)` for value-based cropping. Results can be converted
back to a raster with `terra::rast(df, type = "xyz")`.

## Examples

``` r
# \donttest{
f <- tempfile(fileext = ".tif")
df <- data.frame(x = as.double(rep(1:4, 3)),
                 y = as.double(rep(1:3, each = 4)),
                 band1 = as.double(1:12))
write_tiff(df, f)
node <- tbl_tiff(f)
node |> filter(band1 > 6) |> collect()
unlink(f)
# }
```
