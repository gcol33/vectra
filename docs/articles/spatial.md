# Out-of-core GIS with vectra

## The streaming envelope

Every desktop GIS runs its toolbox one in-memory layer at a time. Open a
layer, run the tool, write the result. The working layer has to fit in
RAM, and a country-scale point set or a national DEM often does not.

vectra runs the same toolbox one batch at a time. A query is pulled
through the C engine in fixed-size row groups, each spatial step works
on the batch in front of it, and the transformed batch spills back to
disk as a fresh lazy node. Peak memory is one batch and whatever small
resident layer the step needs, so a billion-row layer flows past a fixed
memory budget.

Topology stays with [sf](https://r-spatial.github.io/sf/) and
[terra](https://rspatial.github.io/terra/): vectra adds no GEOS or GDAL
link. Geometry rides through the engine as hex-encoded WKB in an
ordinary string column, and the coordinate reference system is carried
on the node. What vectra contributes is the streaming envelope around
the operations a desktop GIS keeps resident.

``` r

library(vectra)
```

## From a raster to one row per cell

A raster is a dense grid of values. To feed it into a tabular pipeline,
read it as one row per cell, each cell a record of its coordinate and
value. That is the bridge between the raster cube and every verb in the
engine.

[`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
opens a `.vec` raster cube, and
[`vec_extract_points()`](https://gillescolling.com/vectra/reference/vec_extract_points.md)
samples it at coordinates. A whole national climate grid stays on disk
while the occurrence points it is queried at stay in memory.

``` r

clim <- vec_open_raster("worldclim_bio.vec")

occ <- read.csv("occurrences.csv")          # species, x, y
occ <- cbind(occ, vec_extract_points(clim, occ$x, occ$y))
head(occ)
```

For a GeoTIFF the same sampling is
[`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md).
From here the points are an ordinary vectra table, ready for any verb.

## Select by location

The most-used vector tool keeps the features that stand in a spatial
relation to another layer: points inside a study region, parcels
touching a road, patches within a buffer.
[`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md)
streams the large layer and tests each batch against a small resident
locator with an sf predicate, keeping or dropping whole features.

``` r

region <- sf::st_read("study_area.gpkg", quiet = TRUE)

inside <- tbl("occurrences.vtr") |>
  spatial_filter(region, coords = c("x", "y"), crs = 4326)
```

The result carries the same schema as the input, with the outside
features removed. `negate = TRUE` keeps the outside features instead. To
cut geometry along the region rather than keep whole features,
[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md)
intersects each batch with the mask, and `erase = TRUE` keeps the part
outside it.

``` r

clipped <- tbl("rivers.vtr") |> spatial_clip(region, crs = 4326)
```

To tag each feature with the polygon it falls in rather than filter,
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
appends the polygon attributes. When both layers are larger than RAM,
`partition = grid(cellsize)` bins them to a grid and joins one shard at
a time, with the right layer itself a streamed node.

``` r

tagged <- tbl("gbif_points.vtr") |>
  spatial_join(tbl("countries.vtr"), coords = c("x", "y"),
               crs = 4326, partition = grid(1))
```

## Rasterize a streamed point set

Folding a point set into a grid is the headline larger-than-RAM
operation: the grid fits in memory, the points do not.
[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
maps each point to its cell through the geotransform and accumulates a
per-cell value in C, one batch at a time, no spill. This is the
monoid-fold tier: bounded memory, a single pass.

``` r

# Point density on a continental grid, streamed from a billion-row file.
density <- tbl("gbif_points.vtr") |>
  rasterize(extent = c(-180, -90, 180, 90), res = 0.1,
            fun = "count", path = "density.vec")
```

`fun` is the per-cell reduction: `"count"`, `"sum"`, `"mean"`, `"min"`,
`"max"`. With a `field` it aggregates that column rather than tallies
points. The output is a `.vec` cube or an in-memory matrix.

Going the other way,
[`zonal()`](https://gillescolling.com/vectra/reference/zonal.md)
summarises a raster within zones, streaming the grid one tile-row strip
at a time and folding per-zone moments in memory.

``` r

admin <- sf::st_read("regions.gpkg", quiet = TRUE)
zonal(clim, admin, fun = c("mean", "sd"), zone_field = "region_id")
```

## Terrain on a streamed DEM

Moving-window operations read each output tile expanded by the kernel
radius (a halo read), so a national DEM never has to be resident.
[`focal()`](https://gillescolling.com/vectra/reference/focal.md) applies
an arbitrary weight window, and
[`terrain()`](https://gillescolling.com/vectra/reference/terrain.md)
derives slope, aspect, hillshade, TPI, roughness, and TRI with Horn’s
method, matching
[`terra::terrain()`](https://rspatial.github.io/terra/reference/terrain.html).
When `path` is given the result streams straight back to a new `.vec`,
so neither the input nor the output band is ever fully in memory.

``` r

terrain("dem.vec", v = c("slope", "aspect", "hillshade"),
        path = "dem_derivatives.vec")
```

To resample or reproject,
[`warp()`](https://gillescolling.com/vectra/reference/warp.md) walks the
output one tile-row strip at a time, projects each strip’s pixel centres
into the source CRS (via PROJ when the two CRSs differ), and samples the
bounded source window with nearest, bilinear, or cubic interpolation,
matching
[`terra::project()`](https://rspatial.github.io/terra/reference/project.html).

``` r

warp("dem.vec", list(crs = 3035, res = 25),
     method = "bilinear", path = "dem_laea.vec")
```

## Back from raster to vector

[`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
carries points onto a grid;
[`polygonize()`](https://gillescolling.com/vectra/reference/polygonize.md)
carries a grid back to vector. It reads one tile-row strip at a time and
dissolves equal-valued cells into one polygon per value, so a classified
land-cover raster becomes a polygon layer out of core.

``` r

habitat <- polygonize("landcover.vec")       # one polygon per class
```

[`contours()`](https://gillescolling.com/vectra/reference/contours.md)
traces iso-lines from a continuous surface with marching squares over
the same haloed strip pass, then joins each level’s segments into
continuous lines.

``` r

isolines <- contours("dem.vec", levels = seq(0, 3000, by = 200))
```

## Masking and raster math

[`mask()`](https://gillescolling.com/vectra/reference/mask.md) clips a
raster to a polygon layer one strip at a time, keeping the pixels whose
centre falls inside it, the raster counterpart of
[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md).

``` r

study <- mask("dem.vec", region, path = "dem_study.vec")
```

[`rast_calc()`](https://gillescolling.com/vectra/reference/rast_calc.md)
evaluates a cellwise expression across aligned rasters, reading each
input strip by strip. A vegetation index, a reclassification, or
arithmetic across layers is one expression over the named rasters.

``` r

ndvi <- rast_calc(list(nir = "nir.vec", red = "red.vec"),
                  (nir - red) / (nir + red), path = "ndvi.vec")
```

[`mosaic()`](https://gillescolling.com/vectra/reference/mosaic.md)
merges rasters that share a resolution and cell grid onto their union,
resolving overlap with `mean`, `first`, `last`, `min`, `max`, or `sum`,
one output strip at a time.

``` r

tile_merge <- mosaic(list("n50.vec", "n51.vec", "n52.vec"), fun = "mean")
```

## Distance to the nearest feature

[`proximity()`](https://gillescolling.com/vectra/reference/proximity.md)
gives every cell its Euclidean distance to the nearest feature cell, in
CRS units. Features are the non-NA cells, or the cells whose value is in
`target`. The transform is separable: a pass along the rows, a pass
along the columns, each linear in the line length, with an out-of-core
transpose between them so the whole grid is never resident. Squared
distances scale by the x and y resolution, so the answer is exact on
non-square cells.

``` r

dist_to_road <- proximity("roads.vec", path = "road_distance.vec")
sea_distance <- proximity("landcover.vec", target = 0)   # 0 = water
```

Cost-distance, which accumulates a per-cell friction along the path, is
a global shortest-path problem and stays resident:
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) the
raster and run a resident solver for it.

## The cost model

Each operation states its memory tier, so the toolbox reads as a cost
model rather than a grab-bag. The tier tells you what a run holds
resident.

- **Monoid fold.** One batch at a time, bounded memory, no spill:
  [`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md),
  [`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md),
  [`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md),
  [`zonal()`](https://gillescolling.com/vectra/reference/zonal.md), and
  every
  [`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
  recipe.
- **Sort / partition.** Spatial locality first:
  [`focal()`](https://gillescolling.com/vectra/reference/focal.md),
  [`terrain()`](https://gillescolling.com/vectra/reference/terrain.md),
  [`warp()`](https://gillescolling.com/vectra/reference/warp.md) over
  tile-and-halo reads,
  [`proximity()`](https://gillescolling.com/vectra/reference/proximity.md)
  over its transpose passes,
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md),
  and the partitioned
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md).
- **All-to-all.** Inherently resident:
  [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  and operations such as Voronoi, Delaunay, convex hull, and global
  neighbour graphs. For these, collect the result and run sf or terra on
  it; vectra does not pretend to stream what is global by nature.

That boundary is the honest part. The streaming envelope covers the
operations whose memory can be bounded, and names the ones it cannot.

## Where to go next

- [Streaming spatial
  operations](https://gillescolling.com/vectra/articles/streaming-spatial.md)
  for the hands-on companion: every vector verb run end to end on a real
  layer, with runnable code and figures.

- [Larger-than-RAM
  strategy](https://gillescolling.com/vectra/articles/large-data.md) for
  the spill and partition tiers in depth.

- [Offloading](https://gillescolling.com/vectra/articles/offload.md) for
  the cost grades the spatial tiers reuse.

- [Species distribution
  modelling](https://gillescolling.com/vectra/articles/sdm.md) for an
  end-to-end ecological workflow.

- The function reference for every spatial verb and its arguments.
