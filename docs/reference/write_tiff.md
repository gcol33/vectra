# Write query results to a GeoTIFF file

The data must contain `x` and `y` columns (pixel center coordinates) and
one or more numeric band columns. Grid dimensions and geotransform are
inferred from the x/y coordinate arrays. Missing pixels are written as
NaN (or the type-appropriate nodata value for integer pixel types).

## Usage

``` r
write_tiff(
  x,
  path,
  compress = FALSE,
  pixel_type = "float64",
  metadata = NULL,
  ...
)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path for the output GeoTIFF file.

- compress:

  Logical; use DEFLATE compression? Default `FALSE`.

- pixel_type:

  Character string specifying the output pixel type. One of `"float64"`
  (default), `"float32"`, `"int16"`, `"int32"`, `"uint8"`, or
  `"uint16"`.

- metadata:

  Optional character string of GDAL_METADATA XML to embed in the file
  (tag 42112). Use
  [`tiff_metadata()`](https://gillescolling.com/vectra/reference/tiff_metadata.md)
  to read it back.

- ...:

  Reserved for future use.

## Value

Invisible `NULL`.

## Examples

``` r
# \donttest{
# Write as int16 with DEFLATE compression
df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = c(100, 200, 300, 400))
f <- tempfile(fileext = ".tif")
write_tiff(df, f, compress = TRUE, pixel_type = "int16")
unlink(f)
# }
```
