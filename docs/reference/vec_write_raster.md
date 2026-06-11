# Write a raster matrix or 3D array to a .vec raster file

Writes a row-major raster (one band) or a band-major 3D array
(multi-band) to the VECR raster format. Each tile is encoded as a
self-describing tdc block (PRED_2D + BYTE_SHUFFLE + LZ).

## Usage

``` r
vec_write_raster(
  x,
  path,
  dtype = "f32",
  tile_size = 512L,
  extent = NULL,
  gt = NULL,
  epsg = 0L,
  nodata = NA_real_,
  band_names = NULL,
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- x:

  A numeric matrix `c(rows, cols)` for a single band, or a numeric 3D
  array `c(rows, cols, bands)` for multi-band.

- path:

  Output file path.

- dtype:

  Storage dtype, one of `"f64"`, `"f32"`, `"i8"`, `"u8"`, `"i16"`,
  `"u16"`, `"i32"`, `"u32"`, `"i64"`, `"u64"`. Defaults to `"f32"` for
  floating-point input — `"f64"` doubles file size with no information
  gain for typical climate rasters.

- tile_size:

  Square tile edge in pixels. Default 512.

- extent:

  Numeric vector `c(xmin, ymin, xmax, ymax)`. Used together with the
  raster dimensions to derive the geotransform. Either `extent` or `gt`
  must be supplied for georeferenced output.

- gt:

  Numeric(6) GDAL-style geotransform. Overrides `extent` if both are
  given.

- epsg:

  EPSG code (integer) or 0L for none.

- nodata:

  Nodata value, or `NA_real_` to skip recording one.

- band_names:

  Optional character vector of length equal to the number of bands.

- compression:

  Compression effort, one of `"fast"` (single spec, fast encode),
  `"balanced"` (probe two entropy coders, ~2x encode time), or `"max"`
  (probe six candidate specs per tile, slowest encode but smallest
  file). Decode cost is unchanged across levels because each tile
  records its own codec spec. Default `"fast"`.

## Value

Invisible `NULL`.
