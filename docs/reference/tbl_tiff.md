# Create a lazy table reference from a GeoTIFF raster

Opens a GeoTIFF file and returns a lazy query node. Each pixel becomes a
row with columns `x`, `y`, `band1`, `band2`, etc. Coordinates are pixel
centers derived from the affine geotransform. NoData values become `NA`.

## Usage

``` r
tbl_tiff(path, batch_size = 256L)
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
if (FALSE) { # \dontrun{
node <- tbl_tiff("climate.tif")
node |> filter(band1 > 25) |> collect()
} # }
```
