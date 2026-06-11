# Tile layout of an open .vec raster

Returns `"image"` (default Phase 6 layout — one tile per
`(band, time, ty, tx)`) or `"pixel"` (Phase 6b transpose layout — one
tile per `(band, ty, tx)` holding the full time stack).

## Usage

``` r
vec_raster_layout(r)
```

## Arguments

- r:

  A `vectra_raster`.

## Value

Character(1) `"image"` or `"pixel"`.
