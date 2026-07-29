# Nearest neighbours of a streamed layer in predictor space

Streams a query side `x` through the engine and, for each row, returns
the mean distance to its nearest neighbours in a resident reference
cloud `y` – where distance is measured in *predictor* (attribute) space,
not on coordinates. This is the continuous half of the MOP
transferability metric (Owens et al. 2013): the mean environmental
distance from a projection cell to the nearest part of a calibration
cloud. Where
[`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md)
measures geographic proximity with GEOS, `feature_knn()` measures
environmental novelty with an L2 or Mahalanobis distance in n-variable
space.

## Usage

``` r
feature_knn(
  x,
  y,
  vars = NULL,
  k = NULL,
  percentage = NULL,
  metric = c("euclidean", "mahalanobis"),
  dist_col = "knn_distance",
  nthreads = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
  ...): the streamed query side. Its predictor columns are read one
  batch at a time.

- y:

  A data.frame or numeric matrix: the resident reference cloud, one
  column per predictor. Rows with any `NA` are dropped.

- vars:

  Character vector of predictor column names present in both `x` and
  `y`. Defaults to the column names of `y`.

- k:

  Number of nearest neighbours to average over. Supply exactly one of
  `k` or `percentage`.

- percentage:

  Percent of the reference cloud to average over (the nearest
  `ceil(percentage/100 * nrow(y))` neighbours). Supply exactly one of
  `k` or `percentage`.

- metric:

  `"euclidean"` (default) for straight L2 distance in predictor units,
  or `"mahalanobis"` for distance whitened by the reference cloud's
  covariance.

- dist_col:

  Name of the appended distance column. Default `"knn_distance"`.

- nthreads:

  Threads for the per-batch distance scan. `NULL` (default) uses all
  available (capped to two under `R CMD check`).

- flush_rows:

  Rows to buffer before spilling a run file. `NULL` (default) spills by
  the streaming memory budget instead.

## Value

A `vectra_node` of `x`'s columns plus `dist_col`, backed by temporary
`.vtr` spills removed when the node is garbage-collected.

## Details

The reference cloud is materialized once, whitened for the chosen
metric, and held resident; the query side streams one batch at a time,
so the projection side can exceed memory while peak memory stays at one
batch plus the resident cloud. The neighbour count is either an absolute
`k` or the nearest `percentage`% of the cloud
(`ceil(percentage/100 * nrow(y))`), matching MOP's
proportion-of-reference formulation.

## References

Owens, H.L. et al. (2013) Constraints on interpretation of ecological
niche models by limited environmental ranges on calibration areas.
*Ecological Modelling* 263:10-18.

## See also

[`rast_feature_distance()`](https://gillescolling.com/vectra/reference/rast_feature_distance.md)
for the same distance computed out-of-core over a projection raster,
[`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md)
for geographic nearest neighbours.

## Examples

``` r
set.seed(1)
ref <- data.frame(bio1 = rnorm(500, 10, 2), bio12 = rnorm(500, 800, 50))
qy  <- data.frame(bio1 = c(10, 20), bio12 = c(800, 400))
f <- tempfile(fileext = ".vtr")
write_vtr(qy, f)

# Row 1 sits in the cloud (small distance); row 2 is far outside it.
tbl(f) |>
  feature_knn(ref, percentage = 5) |>
  collect()
unlink(f)
```
