# Close a .vec raster handle

Idempotent. The handle is also auto-released by R's garbage collector.

## Usage

``` r
vec_close_raster(r)
```

## Arguments

- r:

  A `vectra_raster` returned by
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md).

## Value

Invisible `NULL`.
