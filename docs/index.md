# vectra

*querying data that won’t fit in memory*

[![CRAN
status](https://www.r-pkg.org/badges/version/vectra)](https://CRAN.R-project.org/package=vectra)
[![CRAN
downloads](https://cranlogs.r-pkg.org/badges/grand-total/vectra)](https://cran.r-project.org/package=vectra)
[![Monthly
downloads](https://cranlogs.r-pkg.org/badges/vectra)](https://cran.r-project.org/package=vectra)
[![R-CMD-check](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml)
[![ASAN/UBSAN](https://github.com/gcol33/vectra/actions/workflows/sanitizers.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/sanitizers.yml)
[![Codecov test
coverage](https://codecov.io/gh/gcol33/vectra/graph/badge.svg)](https://app.codecov.io/gh/gcol33/vectra)
[![License:
MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**dplyr-style queries on larger-than-RAM data, backed by a pull-based
columnar engine written in C11.**

Point vectra at a file too big to load and query it with the verbs you
already use. Data flows through the engine one row group at a time, so
peak memory stays bounded no matter how large the file gets. Arrow needs
compiled binaries that match your platform, DuckDB links a bundled
library, Spark wants a JVM. vectra is a standard R extension with no
external dependencies: it compiles where R compiles.

``` r

library(vectra)

# Lazy scan over a multi-GB CSV; nothing runs until collect()
tbl_csv("measurements.csv") |>
  filter(temperature > 30, year >= 2020) |>
  group_by(station) |>
  summarise(avg_temp = mean(temperature), n = n()) |>
  collect()
```

## One engine, several file formats

`.vtr` (vectra’s own columnar format), CSV, SQLite, and GeoTIFF all open
into the same lazy query nodes, so the same pipeline runs against any of
them:

``` r

# GeoTIFF climate raster as tidy data
tbl_tiff("worldclim_bio1.tif") |>
  filter(band1 > 0) |>
  mutate(temp_c = band1 / 10) |>
  collect()

# SQLite without DBI
tbl_sqlite("survey.db", "responses") |>
  filter(year == 2025) |>
  left_join(tbl_sqlite("survey.db", "sites"), by = "site_id") |>
  collect()
```

Convert to `.vtr` once for repeated queries, then read it back fast:

``` r

write_vtr(big_df, "data.vtr", batch_size = 100000)

tbl("data.vtr") |>
  filter(x > 0, region == "EU") |>
  group_by(region) |>
  summarise(total = sum(value), n = n()) |>
  collect()
```

## How the engine stays bounded

The optimizer rewrites the plan before any data moves, and execution
streams the result rather than materializing it:

- **Streaming execution**: one row group flows through at a time, never
  the whole file
- **Column pruning**: columns you never reference are skipped at the
  scan
- **Predicate pushdown**: per-rowgroup min/max statistics skip row
  groups that cannot match a filter
- **Hash joins**: build the right side, stream the left; join a fact
  table against a lookup without holding both in memory
- **External sort**: a memory budget with automatic spill to disk

[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
shows the optimized plan, including which columns and row groups were
pruned:

``` r

tbl("data.vtr") |>
  filter(x > 0) |>
  select(id, x) |>
  explain()
#> vectra execution plan
#>
#> ProjectNode [streaming]
#>   FilterNode [streaming]
#>     ScanNode [streaming, 2/5 cols (pruned), predicate pushdown, v3 stats]
```

## Fuzzy matching in the engine

String distance runs inside the C engine, with no round-trip to R per
row:

``` r

tbl("taxa.vtr") |>
  filter(levenshtein(species, "Quercus robur") <= 2) |>
  mutate(similarity = jaro_winkler(species, "Quercus robur")) |>
  arrange(desc(similarity)) |>
  collect()
```

`levenshtein()`, `dl_dist()` (Damerau-Levenshtein), and `jaro_winkler()`
are available in
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
alongside [`nchar()`](https://rdrr.io/r/base/nchar.html),
[`substr()`](https://rdrr.io/r/base/substr.html),
[`grepl()`](https://rdrr.io/r/base/grep.html),
[`gsub()`](https://rdrr.io/r/base/grep.html) and the rest of the string
toolkit.

## Spatial data, out of core

Same idea, applied to geometry: read and write layers larger than
memory, running the GIS operations you would otherwise do in sf.
Geometry rides through as WKB in an ordinary column, so the spatial
verbs stream one batch at a time like the rest. Tag a billion-point
stream with the polygon each point falls in, holding the polygons plus
one batch in memory:

``` r

library(sf)

tbl("gps_pings.vtr") |>
  spatial_join(zones_sf, join = st_intersects) |>
  count(zone_id) |>
  collect()
```

`st_*` geometry functions evaluate inside
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
computed in C on the GEOS library straight off the geometry column:

``` r

tbl("parcels.vtr") |>
  filter(st_area(geometry) > 1e6) |>
  mutate(centroid = st_centroid(geometry)) |>
  collect_sf()
```

Raster operations stream strip by strip over the tiled `.vec` format, so
a grid larger than memory passes through
[`zonal()`](https://gillescolling.com/vectra/reference/zonal.md),
[`focal()`](https://gillescolling.com/vectra/reference/focal.md),
[`terrain()`](https://gillescolling.com/vectra/reference/terrain.md),
[`warp()`](https://gillescolling.com/vectra/reference/warp.md), and map
algebra one tile-row at a time:

``` r

r <- vec_open_raster("dem.vec")
terrain(r, c("slope", "aspect", "hillshade"))     # Horn's method over haloed strips
zonal(r, watersheds_sf, fun = c("mean", "sd"))    # per-zone, one strip resident
```

The toolbox reaches further: vector-raster bridges
([`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md),
[`polygonize()`](https://gillescolling.com/vectra/reference/polygonize.md),
[`contours()`](https://gillescolling.com/vectra/reference/contours.md)),
planar overlay and coverage cleaning
([`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md),
[`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md),
[`spatial_eliminate()`](https://gillescolling.com/vectra/reference/spatial_eliminate.md)),
and routing on a line network
([`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md),
[`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md),
[`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)).
Geometry stays with GEOS through libgeos; `sf` is needed only for vector
format I/O and reprojection.

## Star schemas instead of flat-table column creep

Register dimension tables once, then pull columns from any of them;
joins are built for you, and unmatched keys are reported:

``` r

s <- vtr_schema(
  fact    = tbl("observations.vtr"),
  species = link("sp_id", tbl("species.vtr")),
  site    = link("site_id", tbl("sites.vtr"))
)

lookup(s, count, species$name, site$habitat) |> collect()
#> species: all 500 keys matched
#> site: 3/500 unmatched keys (X1, X2, X3)
```

## Incremental updates

Append new rows as a new row group without rewriting the file, or take a
key-based diff between two snapshots:

``` r

append_vtr(new_rows_df, "data.vtr")

d <- diff_vtr("snapshot_old.vtr", "snapshot_new.vtr", key_col = "id")
collect(d$added)   # rows present in new but not old
d$deleted          # key values present in old but not new
```

## Streaming results into a model fit

[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
brings the whole result into memory.
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
folds a function over a query one batch at a time, holding a single
batch plus the accumulator, so a result larger than memory reduces to a
small summary in one pass: a running count, per-group sufficient
statistics, or the cross-products behind a linear fit.

``` r

# Accumulate X'X and X'y for an exact OLS fit, one streaming batch at a time
acc <- tbl("survey.vtr") |>
  select(mpg, wt, hp) |>
  collect_chunked(
    function(acc, chunk) {
      X <- cbind(1, chunk$wt, chunk$hp)
      list(XtX = acc$XtX + crossprod(X),
           Xty = acc$Xty + crossprod(X, chunk$mpg))
    },
    .init = list(XtX = matrix(0, 3, 3), Xty = matrix(0, 3, 1))
  )
solve(acc$XtX, acc$Xty)
```

For models that re-read the data on every iteration,
[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
exposes a query as a resettable generator that
[`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) drives
directly, fitting a GLM on data too large to hold in memory:

``` r

src <- function() tbl("occurrences.vtr") |> select(presence, bio1, bio12)
biglm::bigglm(presence ~ bio1 + bio12, data = chunk_feeder(src),
              family = binomial())
```

[`offload()`](https://gillescolling.com/vectra/reference/offload.md)
spills a prepared query to disk once and streams it back, so each
reweighted pass reads the prepared columns from a file rather than
rebuilding the pipeline. With a `by` key it splits the query into
per-key shards on disk, each small enough to pull into memory on its
own.
[`group_map()`](https://gillescolling.com/vectra/reference/group_map.md)
then runs your function on each shard, passing the shard as an ordinary
data.frame along with its group key, and returns the results keyed by
shard. That function is arbitrary R: a model fit, a clustering call, a
custom summary, or any computation that needs the whole group in memory
at once.
[`group_modify()`](https://gillescolling.com/vectra/reference/group_map.md)
recombines per-shard data.frames into one table.

``` r

# Prepare once, then let bigglm re-read the spill on every pass
s <- offload(tbl("occurrences.vtr") |> select(presence, bio1, bio12))
biglm::bigglm(presence ~ bio1 + bio12, data = chunk_feeder(s),
              family = binomial())

# Per-region: group_map runs any function on each in-memory shard (here a GLM)
p <- offload(tbl("occurrences.vtr"), by = "region")
fits <- group_map(p, function(d, region)
  glm(presence ~ bio1 + bio12, data = d, family = binomial()))
```

## Verbs

vectra covers the dplyr surface most analysis pipelines use:
[`filter()`](https://gillescolling.com/vectra/reference/filter.md),
[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md) /
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
with the common aggregations, all the joins
([`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
through
[`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md),
plus
[`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)),
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md) and
the `slice_*()` family, and window functions (`row_number()`,
[`rank()`](https://rdrr.io/r/base/rank.html),
[`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
[`cumsum()`](https://rdrr.io/r/base/cumsum.html), `ntile()`, and more).
Date/time and string functions evaluate in the engine.
[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`rename()`](https://gillescolling.com/vectra/reference/rename.md),
[`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
and [`across()`](https://gillescolling.com/vectra/reference/across.md)
accept the full tidyselect helper set (`starts_with()`, `where()`,
`all_of()`, and the rest).

The [Function Reference](https://gillescolling.com/vectra/reference/)
lists every verb and expression with examples.

## Installation

``` r

install.packages("vectra")            # CRAN

install.packages("pak")               # development version
pak::pak("gcol33/vectra")
```

## Documentation

Engine:

- [Getting
  Started](https://gillescolling.com/vectra/articles/quickstart.html)
- [Engine
  Reference](https://gillescolling.com/vectra/articles/engine.html)
- [Format
  Backends](https://gillescolling.com/vectra/articles/formats.html)
- [Joins](https://gillescolling.com/vectra/articles/joins.html)
- [Star Schemas](https://gillescolling.com/vectra/articles/schema.html)
- [String
  Operations](https://gillescolling.com/vectra/articles/string-ops.html)
- [Indexing and
  Optimization](https://gillescolling.com/vectra/articles/indexing.html)
- [Working with Large
  Data](https://gillescolling.com/vectra/articles/large-data.html)
- [Offloading and Out-of-core
  Fits](https://gillescolling.com/vectra/articles/offload.html)

Spatial:

- [Out-of-core
  GIS](https://gillescolling.com/vectra/articles/spatial.html)
- [Streaming Spatial
  Operations](https://gillescolling.com/vectra/articles/streaming-spatial.html)
- [Geometry Functions in
  Expressions](https://gillescolling.com/vectra/articles/geometry-expressions.html)
- [Coverage and
  Topology](https://gillescolling.com/vectra/articles/coverage-topology.html)
- [Network
  Analysis](https://gillescolling.com/vectra/articles/networks.html)
- [Species Distribution
  Models](https://gillescolling.com/vectra/articles/sdm.html)

## Support

> “Software is like sex: it’s better when it’s free.” — Linus Torvalds

I’m a PhD student who builds R packages in my free time because I
believe good tools should be free and open. I started these projects for
my own work and figured others might find them useful too.

If this package saved you some time, buying me a coffee is a nice way to
say thanks. It helps with my coffee addiction.

[![Buy Me A
Coffee](https://img.shields.io/badge/-Buy%20me%20a%20coffee-FFDD00?logo=buymeacoffee&logoColor=black)](https://buymeacoffee.com/gcol33)

## License

MIT (see the LICENSE.md file)

## Citation

``` bibtex
@software{vectra,
  author = {Colling, Gilles},
  title  = {vectra: Columnar Query Engine for Larger-Than-RAM Data},
  year   = {2026},
  url    = {https://github.com/gcol33/vectra}
}
```
