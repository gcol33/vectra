# Open a .vec raster

Lazy open: parses the header and tile index but does not decode any
tiles. Returns a list with metadata and an external pointer handle. The
pointer is auto-finalized when garbage collected; call
[`vec_close_raster()`](https://gillescolling.com/vectra/reference/vec_close_raster.md)
to release earlier.

## Usage

``` r
vec_open_raster(path)
```

## Arguments

- path:

  Path to a `.vec` raster file.

## Value

A `vectra_raster` list with elements: `ptr`, `width`, `height`,
`n_bands`, `tile_size`, `dtype`, `gt`, `epsg`, `nodata`, `band_names`.
