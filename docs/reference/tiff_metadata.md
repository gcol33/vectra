# Read GDAL_METADATA from a GeoTIFF

Returns the GDAL_METADATA XML string (TIFF tag 42112) embedded in a
GeoTIFF file. Returns `NA` if the tag is not present.

## Usage

``` r
tiff_metadata(path)
```

## Arguments

- path:

  Path to a GeoTIFF file.

## Value

A single character string containing the XML, or `NA_character_`.

## Examples

``` r
# \donttest{
f <- tempfile(fileext = ".tif")
df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = as.double(1:4))
write_tiff(df, f, metadata = "<GDALMetadata></GDALMetadata>")
tiff_metadata(f)
unlink(f)
# }
```
