# Species Distribution Models

## What vectra does for a distribution model

A species distribution model relates occurrence records to environmental
predictors. The predictors usually arrive as raster layers (climate,
terrain, land cover), and the records as point coordinates. The first
step of almost every workflow is the same: sample each raster at the
occurrence coordinates to build a table of predictor values, then fit a
model to that table.

vectra covers two parts of this pipeline directly. It reads `GeoTIFF`
rasters (including tiled and `BigTIFF` layouts) and samples them at
point coordinates with
[`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md),
reading only the strips that contain query points, so a sparse point set
over a continental raster touches a small fraction of the file. And it
consumes a query one batch at a time, so a model matrix that exceeds
memory can still be fitted:
[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
streams the data through
[`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) one chunk
per iteration.

Two operations sit outside vectra’s scope, and the rest of this article
assumes they are handled before extraction:

- **Vector geometry.** vectra has no polygons, lines, or spatial joins.
  Buffer a study region, intersect with a range map, or rasterize a
  shapefile using `sf` or `terra` first.
- **Reprojection.** vectra reads the coordinate reference system of a
  `GeoTIFF` with
  [`tiff_crs()`](https://gillescolling.com/vectra/reference/tiff_crs.md)
  but does not transform coordinates. Occurrence points must already be
  in the raster’s CRS when they reach
  [`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md).

## Align coordinates before extracting

[`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
maps each coordinate to a pixel through the raster’s affine
geotransform. It does this in the raster’s own CRS, so points recorded
in a different CRS must be reprojected first. Read the raster CRS with
[`tiff_crs()`](https://gillescolling.com/vectra/reference/tiff_crs.md),
then transform the points to match:

``` r

# Occurrences recorded in lon/lat (EPSG:4326), raster in a projected CRS.
target <- tiff_crs("predictors.tif")$epsg          # e.g. 3035 (LAEA Europe)

pts <- sf::st_as_sf(occ, coords = c("lon", "lat"), crs = 4326)
pts <- sf::st_transform(pts, target)
xy  <- sf::st_coordinates(pts)

env <- tiff_extract_points("predictors.tif", x = xy[, 1], y = xy[, 2])
```

When the points and the raster already share a CRS this step is
unnecessary. The examples below work in lon/lat throughout.

## Build a predictor table

We synthesize a small two-layer climate raster over Central Europe and
write it to a `GeoTIFF` with an EPSG:4326 GeoKey. Layer `band1` stands
in for mean annual temperature and `band2` for annual precipitation.

``` r

nx <- 80; ny <- 50
xmin <- 5; xmax <- 17; ymin <- 45; ymax <- 55
xres <- (xmax - xmin) / nx
yres <- (ymax - ymin) / ny

g <- expand.grid(col = 0:(nx - 1), row = 0:(ny - 1))
g$x <- xmin + (g$col + 0.5) * xres          # pixel centres
g$y <- ymax - (g$row + 0.5) * yres          # row 1 is the northern edge
g$band1 <- 14 - 0.6 * (g$y - 50) + 0.2 * (g$x - 11) + rnorm(nrow(g), 0, 0.3)
g$band2 <- 750 + 25 * (g$y - 50) - 8 * (g$x - 11) + rnorm(nrow(g), 0, 10)

tif <- tempfile(fileext = ".tif")
write_tiff(g[, c("x", "y", "band1", "band2")], tif, crs = 4326L)

tiff_crs(tif)
#> $epsg
#> [1] 4326
#> 
#> $citation
#> [1] NA
```

Now we sample the raster at a set of survey sites.
[`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
returns one row per point with the input coordinates and one column per
band. Points outside the raster come back as `NA`.

``` r

N <- 1500
sites <- data.frame(
  x = runif(N, xmin + 0.3, xmax - 0.3),
  y = runif(N, ymin + 0.3, ymax - 0.3)
)

env <- tiff_extract_points(tif, sites)
head(env)
#>           x        y    band1    band2
#> 1 12.185552 51.38240 13.48465 767.0959
#> 2 16.404559 46.80544 16.52226 624.3277
#> 3  7.619038 51.65444 12.43874 817.2647
#> 4  5.534852 53.85685 10.44774 896.6918
#> 5  6.119896 47.56621 14.34668 730.9303
#> 6 11.790835 52.69071 12.26763 796.8652
```

We standardize the two predictors once and store the prepared columns,
then draw a presence/absence outcome from them. Standardizing before the
data reaches the model lets every streaming batch share the same
transform, which the out-of-core fit below relies on. With the
generating coefficients known, each fit can be checked against them.

``` r

occ <- data.frame(
  bio1  = as.numeric(scale(env$band1)),   # standardized once, then stored
  bio12 = as.numeric(scale(env$band2))
)
eta <- -0.2 + 1.3 * occ$bio1 - 0.9 * occ$bio12
occ$presence <- rbinom(N, 1, plogis(eta))
table(occ$presence)
#> 
#>   0   1 
#> 790 710
```

## Fit in memory when the table fits

For most studies the extracted table has thousands to a few million rows
and a handful of predictors, which fits in memory comfortably even when
the source rasters are enormous. The ordinary path is to extract, then
fit with [`glm()`](https://rdrr.io/r/stats/glm.html):

``` r

fit <- glm(presence ~ bio1 + bio12, data = occ, family = binomial())
round(coef(fit), 3)
#> (Intercept)        bio1       bio12 
#>      -0.182       1.156      -1.099
```

The recovered coefficients sit close to the values used to generate the
data (intercept -0.2, `bio1` 1.3, `bio12` -0.9).

## Stream when the table is larger than memory

Dense background sampling, many predictors, or a model fitted across a
whole continent can push the predictor table past available RAM. Here
vectra streams the table through the model in bounded pieces. We write
the modelling frame to a `.vtr` file to stand in for a table too large
to hold in memory.

``` r

vtr <- tempfile(fileext = ".vtr")
write_vtr(occ, vtr)
```

### A single pass with collect_chunked()

[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
folds a function over the query one batch at a time. It is the right
tool for any summary that accumulates in bounded space across the data.
Here we compute the class balance and per-class predictor means in a
single streaming pass, never holding more than one batch:

``` r

acc <- collect_chunked(
  tbl(vtr),
  function(acc, chunk) {
    p <- chunk$presence == 1
    acc$n      <- acc$n      + nrow(chunk)
    acc$n_pres <- acc$n_pres + sum(p)
    acc$sum1   <- acc$sum1   + tapply(chunk$bio1, p, sum)[c("FALSE", "TRUE")]
    acc
  },
  .init = list(n = 0L, n_pres = 0L, sum1 = c("FALSE" = 0, "TRUE" = 0))
)
acc$n_pres / acc$n                      # prevalence
#> [1] 0.4733333
```

The same pattern accumulates the cross-products `X'X` and `X'y` behind a
linear model, giving an exact least-squares fit in one pass. A
generalized linear model needs more: each iteratively reweighted step
reweights the rows by the current coefficient estimate, so the data must
be read once per iteration.

### An out-of-core GLM with chunk_feeder()

[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
exposes the query as a resettable generator that
[`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) drives
itself, re-reading the stream on every iteration. Because a vectra node
is consumed as it streams, the feeder takes a factory: a function that
returns a fresh node each time the stream is rewound.

``` r

src <- function() tbl(vtr) |> select(presence, bio1, bio12)

fit_stream <- biglm::bigglm(
  presence ~ bio1 + bio12,
  data = chunk_feeder(src),
  family = binomial()
)
round(coef(fit_stream), 3)
#> (Intercept)        bio1       bio12 
#>      -0.182       1.156      -1.099
```

The streamed fit matches the in-memory
[`glm()`](https://rdrr.io/r/stats/glm.html) to within its convergence
tolerance, while the engine never holds more than one batch of the
predictor table at a time. The same call scales to a `.vtr` or CSV far
larger than RAM: only the factory and the formula change.

`bigglm()` rebuilds the model matrix from each batch as it arrives. A
formula term whose definition is estimated from the data, such as
[`scale()`](https://rdrr.io/r/base/scale.html),
[`poly()`](https://rdrr.io/r/stats/poly.html), `ns()`, or `bs()`, is
recomputed on every batch and takes a different value on each one.
Prepare these terms before writing the table: standardize predictors as
we did above, set explicit `Boundary.knots` on splines, and make sure
every factor level appears in the stream. The formula then names
prepared columns and each batch yields the same transform.

## Choosing a path

Extract with
[`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
in every case. After that:

- If the predictor table fits in memory,
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  it and fit with [`glm()`](https://rdrr.io/r/stats/glm.html),
  [`mgcv::gam()`](https://rdrr.io/pkg/mgcv/man/gam.html), or any
  modelling function. This is the common case.
- If a single-pass summary is all you need (class balance, sufficient
  statistics, an online mean), use
  [`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md).
- If the table itself exceeds memory and you need a full GLM, feed it to
  [`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) through
  [`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md).
  \`\`\`
