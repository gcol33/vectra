# Read per-band names from a GeoTIFF

Returns the band names embedded in the file's GDAL_METADATA XML (TIFF
tag 42112). GDAL writes per-band names as
`<Item name="DESCRIPTION" sample="N" role="description">...</Item>`
entries, where `sample` is the 0-based band index. Bands without a name
in the XML are reported as `NA`. Files with no GDAL_METADATA tag at all
return a length-`nbands` vector of `NA_character_`.

## Usage

``` r
tiff_band_names(path)
```

## Arguments

- path:

  Path to a GeoTIFF file.

## Value

A character vector of length `nbands`. Element `i` is the name of band
`i` (or `NA_character_` if the file does not name it).

## Details

This is a small, dependency-free scanner intended for the common case
(`terra::names(r) <- ...` and similar). For arbitrary XML, parse the raw
string from
[`tiff_metadata()`](https://gillescolling.com/vectra/reference/tiff_metadata.md)
yourself.

## Examples

``` r
# \donttest{
f <- tempfile(fileext = ".tif")
df <- data.frame(x = rep(1:2, 2), y = rep(1:2, each = 2),
                 band1 = as.double(1:4), band2 = as.double(5:8))
xml <- paste0(
  "<GDALMetadata>",
  "<Item name=\"DESCRIPTION\" sample=\"0\" role=\"description\">temperature</Item>",
  "<Item name=\"DESCRIPTION\" sample=\"1\" role=\"description\">humidity</Item>",
  "</GDALMetadata>")
write_tiff(df, f, metadata = xml)
tiff_band_names(f)
unlink(f)
# }
```
