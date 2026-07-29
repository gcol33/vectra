# Predictor-space nearest-neighbour distance surface over a raster

Computes, aligned to the grid of a projection raster `x`, a surface of
the mean distance in predictor space from each cell to the nearest cells
of a reference raster `reference`. Both are multi-band environmental
rasters with one band per predictor. This is the raster, out-of-core
counterpart of
[`feature_knn()`](https://gillescolling.com/vectra/reference/feature_knn.md):
the reference raster is read once into memory and indexed, while `x` is
walked one tile-row strip at a time and streamed to the sink, so the
projection side is larger-than-RAM. `x` and `reference` need not share a
grid – the output follows `x`.

## Usage

``` r
rast_feature_distance(
  x,
  reference,
  vars = NULL,
  k = NULL,
  percentage = NULL,
  metric = c("euclidean", "mahalanobis"),
  path = NULL,
  dtype = "f32",
  nthreads = NULL,
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- x, reference:

  `vectra_raster` handles or paths to `.vec` rasters: the raster to
  score and the reference raster, with matching bands (predictors) in
  the same order.

- vars:

  Predictors to use: band names, band indices, or `NULL` (default, all
  bands). Names resolve against `x`'s band names.

- k:

  Number of nearest reference cells to average over. Supply exactly one
  of `k` or `percentage`.

- percentage:

  Percent of the reference cells to average over (the nearest
  `ceil(percentage/100 * N)`). Supply exactly one of `k` or
  `percentage`.

- metric:

  `"euclidean"` (default) or `"mahalanobis"` (whitened by the reference
  covariance).

- path:

  Optional output `.vec` path. When given the surface is streamed to
  disk and the opened
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
  handle is returned invisibly; when `NULL` the surface is returned as
  an in-memory matrix.

- dtype:

  Storage dtype for `.vec` output. Default `"f32"`.

- nthreads:

  Threads for the distance scan. `NULL` (default) uses all available
  (capped to two under `R CMD check`).

- compression:

  Compression effort for `.vec` output. Default `"fast"`.

## Value

With `path = NULL`, a numeric matrix on `x`'s grid (row 1 northmost)
carrying `gt`, `extent`, and `crs` attributes. With `path` given, the
written single-band `vectra_raster` handle (invisibly). Cells with any
`NA` predictor come back `NA`.

## Details

The distance is the mean distance to the nearest `k`, or nearest
`percentage`% of the reference cells (`ceil(percentage/100 * N)`), under
a Euclidean or Mahalanobis metric. Supply exactly one of `k` or
`percentage`.

This is the streaming distance surface behind an environmental-novelty
or transferability diagnostic such as MOP (Owens et al. 2013). The
strict non-analogous-conditions layers (per-predictor out-of-range
counts) are a separate, already-native computation – a per-band range
reduce plus
[`rast_calc()`](https://gillescolling.com/vectra/reference/rast_calc.md)
– and are shown alongside this surface in the
[`vignette("sdm")`](https://gillescolling.com/vectra/articles/sdm.md);
this function is the continuous-distance piece only.

`x` and `reference` must be `.vec` rasters (or paths to them); bring
GeoTIFFs onto the `.vec` grid with
[`warp()`](https://gillescolling.com/vectra/reference/warp.md) first.

## References

Owens, H.L. et al. (2013) Constraints on interpretation of ecological
niche models by limited environmental ranges on calibration areas.
*Ecological Modelling* 263:10-18.

## See also

[`feature_knn()`](https://gillescolling.com/vectra/reference/feature_knn.md)
for the table-level primitive,
[`warp()`](https://gillescolling.com/vectra/reference/warp.md) to bring
rasters onto a shared grid,
[`rast_calc()`](https://gillescolling.com/vectra/reference/rast_calc.md)
for cellwise raster algebra.

## Examples

``` r
# Two-band reference and projection rasters.
set.seed(1)
r1 <- matrix(rnorm(400, 10, 2), 20, 20)
r2 <- matrix(rnorm(400, 800, 40), 20, 20)
x1 <- r1 + 3               # projection shifted warmer/drier -> more novel
x2 <- r2 - 60
fr <- tempfile(fileext = ".vec"); fx <- tempfile(fileext = ".vec")
vec_write_raster(array(c(r1, r2), c(20, 20, 2)), fr, dtype = "f64",
                 extent = c(0, 0, 20, 20), band_names = c("bio1", "bio12"))
vec_write_raster(array(c(x1, x2), c(20, 20, 2)), fx, dtype = "f64",
                 extent = c(0, 0, 20, 20), band_names = c("bio1", "bio12"))

d <- rast_feature_distance(fx, fr, percentage = 10)
round(mean(d), 2)
unlink(c(fr, fx))
```
