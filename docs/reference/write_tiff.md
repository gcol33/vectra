# Write query results to a GeoTIFF file

The data must contain `x` and `y` columns (pixel center coordinates) and
one or more numeric band columns. Grid dimensions and geotransform are
inferred from the x/y coordinate arrays. Missing pixels are written as
NaN.

## Usage

``` r
write_tiff(x, path, compress = FALSE, ...)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path for the output GeoTIFF file.

- compress:

  Logical; use DEFLATE compression? Default `FALSE`.

- ...:

  Reserved for future use.

## Value

Invisible `NULL`.

## Examples

``` r
if (FALSE) { # \dontrun{
tbl_tiff("climate.tif") |>
  filter(band1 > 25) |>
  write_tiff("filtered.tif")
} # }
```
