# Export a .vec raster to GeoTIFF

Writes the level-0 pixels of a `.vec` raster to a GeoTIFF file. The TIFF
inherits dtype, geotransform, EPSG, and nodata from the source. Strip
layout; the writer supports `"none"`, `"deflate"`, and `"lzw"`
compression. LZW also applies horizontal differencing (Predictor 2) for
integer pixel types, which dramatically improves compression on smooth
raster data and matches the layout most production GIS tools produce by
default. Tiled and BigTIFF output land in a follow-up.

## Usage

``` r
vec_to_tiff(r, path, compression = c("deflate", "lzw", "none"))
```

## Arguments

- r:

  Either a path to a `.vec` raster or a `vectra_raster` returned by
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md).
  If a handle is passed it is left open.

- path:

  Output `.tif` path.

- compression:

  One of `"deflate"` (default), `"lzw"`, or `"none"`.

## Value

Invisible `NULL`.
