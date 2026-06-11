# Read CRS metadata from a GeoTIFF

Returns the spatial reference system embedded in a GeoTIFF, parsed from
the GeoKey directory (TIFF tag 34735). The projected CRS EPSG
(`PCSTypeGeoKey` 3072) is preferred over the geographic CRS EPSG
(`GeographicTypeGeoKey` 2048). Citation strings are read from
`GeoAsciiParams` (tag 34737) with priority PCS \> GeoTIFF \> geographic.

## Usage

``` r
tiff_crs(path)
```

## Arguments

- path:

  Path to a GeoTIFF file.

## Value

A list with elements `epsg` (integer or `NA_integer_`) and `citation`
(character or `NA_character_`).

## Details

Files written without a GeoKey directory return `NA` for both fields.

## Examples

``` r
# \donttest{
f <- tempfile(fileext = ".tif")
df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = as.double(1:4))
write_tiff(df, f)
tiff_crs(f)  # epsg = NA, citation = NA — vectra writer omits GeoKeys
unlink(f)
# }
```
