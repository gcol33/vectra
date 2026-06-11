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
  crs = NULL,
  tiled = FALSE,
  tile_size = 256L,
  bigtiff = "auto",
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

- crs:

  Optional CRS to embed as a GeoKey directory (TIFF tag 34735). Accepts
  an integer EPSG code, an `"EPSG:xxxx"` string, or a list with named
  fields `epsg`, `geographic` (`TRUE`/`FALSE`), and optionally
  `citation`. Codes that are not auto-classified as projected/geographic
  default to projected; pass `geographic = TRUE` to override. Use
  [`tiff_crs()`](https://gillescolling.com/vectra/reference/tiff_crs.md)
  to read it back.

- tiled:

  Logical; write a tiled GeoTIFF (TIFF tags 322/323/324/325) instead of
  strips. Default `FALSE`. Tiled layout enables random-access block
  reads and is required for Cloud-Optimized GeoTIFF (COG).

- tile_size:

  Integer; tile edge length in pixels. Must be a positive multiple of 16
  (TIFF spec). Either a single value (square tiles) or a length-2 vector
  `c(width, height)`. Default `256`. Edge tiles at the right and bottom
  of the image are padded to full tile size with the NoData / NaN value.

- bigtiff:

  Controls BigTIFF dispatch. `"auto"` (default) emits BigTIFF when the
  expected raw payload would exceed the classic-TIFF 4 GB ceiling,
  otherwise emits classic TIFF. `TRUE` forces BigTIFF (magic `0x002B`,
  64-bit offsets), useful for round-trip tests on small data. `FALSE`
  forces classic TIFF — beware that classic TIFF will silently corrupt
  outputs larger than 4 GB. Tiled BigTIFF is not yet supported.

- ...:

  Reserved for future use.

## Value

Invisible `NULL`.

## Examples

``` r
# \donttest{
# Write as int16 with DEFLATE compression and an EPSG:4326 GeoKey
df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = c(100, 200, 300, 400))
f <- tempfile(fileext = ".tif")
write_tiff(df, f, compress = TRUE, pixel_type = "int16", crs = 4326L)
tiff_crs(f)
unlink(f)
# }
```
