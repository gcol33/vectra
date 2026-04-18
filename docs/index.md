# vectra

[![CRAN
status](https://www.r-pkg.org/badges/version/vectra)](https://CRAN.R-project.org/package=vectra)
[![R-CMD-check](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml)
[![ASAN/UBSAN](https://github.com/gcol33/vectra/actions/workflows/sanitizers.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/sanitizers.yml)
[![Codecov test
coverage](https://codecov.io/gh/gcol33/vectra/graph/badge.svg)](https://app.codecov.io/gh/gcol33/vectra)
[![License:
MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

vectra is an R-native columnar query engine for datasets larger than
RAM.

Write dplyr-style pipelines against multi-GB files on a laptop. Data
streams through a C11 pull-based engine one row group at a time, so peak
memory stays bounded regardless of file size.

## Quick Start

Point vectra at any file and query it with dplyr verbs. Nothing runs
until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).

``` r

library(vectra)

# CSV — lazy scan with type inference
tbl_csv("measurements.csv") |>
  filter(temperature > 30, year >= 2020) |>
  group_by(station) |>
  summarise(avg_temp = mean(temperature), n = n()) |>
  collect()

# GeoTIFF — climate rasters as tidy data
tbl_tiff("worldclim_bio1.tif") |>
  filter(band1 > 0) |>
  mutate(temp_c = band1 / 10) |>
  collect()

# Point extraction — sample raster values at coordinates, no terra needed
tiff_extract_points("worldclim_bio1.tif",
                    x = c(10.5, 11.2), y = c(47.1, 47.3))

# SQLite — zero-dependency, no DBI required
tbl_sqlite("survey.db", "responses") |>
  filter(year == 2025) |>
  left_join(tbl_sqlite("survey.db", "sites"), by = "site_id") |>
  collect()
```

For repeated queries, convert to vectra’s native `.vtr` format for
faster reads:

``` r

write_vtr(big_df, "data.vtr", batch_size = 100000)

tbl("data.vtr") |>
  filter(x > 0, region == "EU") |>
  group_by(region) |>
  summarise(total = sum(value), n = n()) |>
  collect()
```

Append new data without rewriting the file, or do a key-based diff
between two snapshots:

``` r

# Append new rows as a new row group — existing data untouched
append_vtr(new_rows_df, "data.vtr")

# Logical diff: what was added or deleted between two snapshots?
d <- diff_vtr("snapshot_old.vtr", "snapshot_new.vtr", key_col = "id")
collect(d$added)   # rows present in new but not old
d$deleted          # key values present in old but not new
```

Fuzzy string matching runs inside the C engine, no round-trip to R:

``` r

tbl("taxa.vtr") |>
  filter(levenshtein(species, "Quercus robur") <= 2) |>
  mutate(similarity = jaro_winkler(species, "Quercus robur")) |>
  arrange(desc(similarity)) |>
  collect()
```

Register a star schema to avoid flat-table column creep. Define the
links once, then pull only what you need:

``` r

s <- vtr_schema(
  fact    = tbl("observations.vtr"),
  species = link("sp_id", tbl("species.vtr")),
  site    = link("site_id", tbl("sites.vtr"))
)

# Pull columns from any dimension — joins are built automatically
lookup(s, count, species$name, site$habitat) |> collect()
#> species: all 500 keys matched
#> site: 3/500 unmatched keys (X1, X2, X3)
```

Use [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
to inspect the optimized plan:

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
#>
#> Output columns (2):
#>   id <int64>
#>   x <double>
```

## Why vectra

Querying large datasets in R usually means Arrow (requires compiled
binaries matching your platform), DuckDB (links a 30 MB bundled
library), or Spark (requires a JVM and cluster configuration).

vectra is a self-contained C11 engine compiled as a standard R
extension. No external libraries, no JVM, no runtime configuration. It
provides:

- **Streaming execution**: data flows one row group at a time, never
  fully in memory
- **Zero-copy filtering**: selection vectors avoid row duplication
- **Query optimizer**: column pruning skips unneeded columns at scan;
  predicate pushdown uses per-rowgroup min/max statistics to skip entire
  row groups
- **Hash joins**: build right, stream left — join a 50 GB fact table
  against a lookup without materializing both
- **External sort**: 1 GB memory budget with automatic spill-to-disk
- **Window functions**: `row_number()`,
  [`rank()`](https://rdrr.io/r/base/rank.html), `dense_rank()`,
  [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
  [`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
  [`cummin()`](https://rdrr.io/r/base/cumsum.html),
  [`cummax()`](https://rdrr.io/r/base/cumsum.html)
- **String expressions**:
  [`nchar()`](https://rdrr.io/r/base/nchar.html),
  [`substr()`](https://rdrr.io/r/base/substr.html),
  [`grepl()`](https://rdrr.io/r/base/grep.html) evaluated in the engine
  without round-tripping to R
- **Multiple data sources**: `.vtr`, CSV, SQLite, GeoTIFF — all produce
  the same lazy query nodes
- **Integer TIFF output**: write rasters as
  `int16`/`int32`/`uint8`/`uint16`/`float32` with embedded GDAL metadata
  for 5-10x smaller files

## Features

| Category | Verbs |
|:---|:---|
| **Transform** | [`filter()`](https://gillescolling.com/vectra/reference/filter.md), [`select()`](https://gillescolling.com/vectra/reference/select.md), [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md), [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md), [`rename()`](https://gillescolling.com/vectra/reference/rename.md), [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md) |
| **Aggregate** | [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md), [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md) (`n`, `sum`, `mean`, `min`, `max`, `sd`, `var`, `first`, `last`, `any`, `all`, `median`, `n_distinct`), [`count()`](https://gillescolling.com/vectra/reference/count.md), [`tally()`](https://gillescolling.com/vectra/reference/count.md), [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md) |
| **Join** | [`left_join()`](https://gillescolling.com/vectra/reference/left_join.md), [`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md), [`right_join()`](https://gillescolling.com/vectra/reference/left_join.md), [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md), [`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md), [`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md), [`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md), [`lookup()`](https://gillescolling.com/vectra/reference/lookup.md) |
| **Order** | [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md), [`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md), [`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md), [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md), [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md), [`slice()`](https://gillescolling.com/vectra/reference/slice.md) |
| **Window** | `row_number()`, [`rank()`](https://rdrr.io/r/base/rank.html), `dense_rank()`, [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`, [`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`, [`cummin()`](https://rdrr.io/r/base/cumsum.html), [`cummax()`](https://rdrr.io/r/base/cumsum.html), `ntile()`, `percent_rank()`, `cume_dist()` |
| **Date/Time** | `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`, [`as.Date()`](https://rdrr.io/r/base/as.Date.html) (in [`filter()`](https://gillescolling.com/vectra/reference/filter.md)/[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)) |
| **String** | [`nchar()`](https://rdrr.io/r/base/nchar.html), [`substr()`](https://rdrr.io/r/base/substr.html), [`grepl()`](https://rdrr.io/r/base/grep.html), [`tolower()`](https://rdrr.io/r/base/chartr.html), [`toupper()`](https://rdrr.io/r/base/chartr.html), [`trimws()`](https://rdrr.io/r/base/trimws.html), [`paste0()`](https://rdrr.io/r/base/paste.html), [`gsub()`](https://rdrr.io/r/base/grep.html), [`sub()`](https://rdrr.io/r/base/grep.html), [`startsWith()`](https://rdrr.io/r/base/startsWith.html), [`endsWith()`](https://rdrr.io/r/base/startsWith.html) (in [`filter()`](https://gillescolling.com/vectra/reference/filter.md)/[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)) |
| **String similarity** | `levenshtein()`, `levenshtein_norm()`, `dl_dist()`, `dl_dist_norm()`, `jaro_winkler()` — fuzzy matching in [`filter()`](https://gillescolling.com/vectra/reference/filter.md)/[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md), with optional `max_dist` early termination |
| **Expression** | [`abs()`](https://rdrr.io/r/base/MathFun.html), [`sqrt()`](https://rdrr.io/r/base/MathFun.html), [`log()`](https://rdrr.io/r/base/Log.html), [`exp()`](https://rdrr.io/r/base/Log.html), [`floor()`](https://rdrr.io/r/base/Round.html), [`ceiling()`](https://rdrr.io/r/base/Round.html), [`round()`](https://rdrr.io/r/base/Round.html), [`log2()`](https://rdrr.io/r/base/Log.html), [`log10()`](https://rdrr.io/r/base/Log.html), [`sign()`](https://rdrr.io/r/base/sign.html), [`trunc()`](https://rdrr.io/r/base/Round.html), `if_else()`, `between()`, `%in%`, [`as.numeric()`](https://rdrr.io/r/base/numeric.html), [`pmin()`](https://rdrr.io/r/base/Extremes.html), [`pmax()`](https://rdrr.io/r/base/Extremes.html), `resolve()`, `propagate()` (in [`filter()`](https://gillescolling.com/vectra/reference/filter.md)/[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)) |
| **Combine** | [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md), [`bind_cols()`](https://gillescolling.com/vectra/reference/bind_rows.md), [`across()`](https://gillescolling.com/vectra/reference/across.md) |
| **Schema** | [`vtr_schema()`](https://gillescolling.com/vectra/reference/vtr_schema.md), [`link()`](https://gillescolling.com/vectra/reference/link.md), [`lookup()`](https://gillescolling.com/vectra/reference/lookup.md) — star schema definition and dimension lookup with match reporting |
| **I/O** | [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md), [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md), [`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md), [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md), [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md), [`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md), [`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md), [`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md), [`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md), [`tiff_metadata()`](https://gillescolling.com/vectra/reference/tiff_metadata.md), [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md), [`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md), [`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md) |
| **Inspect** | [`explain()`](https://gillescolling.com/vectra/reference/explain.md), [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md), [`print()`](https://rdrr.io/r/base/print.html), [`pull()`](https://gillescolling.com/vectra/reference/pull.md) |

Full tidyselect support in
[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`rename()`](https://gillescolling.com/vectra/reference/rename.md),
[`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
and [`across()`](https://gillescolling.com/vectra/reference/across.md):
`starts_with()`, `ends_with()`, `contains()`, `matches()`, `where()`,
`everything()`, `all_of()`, `any_of()`.

## Installation

``` r

# CRAN
install.packages("vectra")

# Development version
pak::pak("gcol33/vectra")
```

## Documentation

- [Getting
  Started](https://gillescolling.com/vectra/articles/quickstart.html) —
  Full walkthrough with runnable examples
- [Format
  Backends](https://gillescolling.com/vectra/articles/formats.html) —
  CSV, SQLite, Excel, GeoTIFF, and streaming conversion pipelines
- [Joins](https://gillescolling.com/vectra/articles/joins.html) — All
  join types, fuzzy joins, key coercion, and memory model
- [Star Schemas](https://gillescolling.com/vectra/articles/schema.html)
  — Dimension lookups, match reporting, and avoiding flat-table column
  creep
- [String
  Operations](https://gillescolling.com/vectra/articles/string-ops.html)
  — Pattern matching, fuzzy matching, and block lookups
- [Indexing and
  Optimization](https://gillescolling.com/vectra/articles/indexing.html)
  — Hash indexes, zone-map pruning, column pruning, and reading
  [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
  output
- [Working with Large
  Data](https://gillescolling.com/vectra/articles/large-data.html) —
  Streaming pipelines, append/delete/diff, external sort, and memory
  budgeting
- [Engine
  Reference](https://gillescolling.com/vectra/articles/engine.html) —
  Execution model, types, coercion, .vtr format, and limitations
- [Function Reference](https://gillescolling.com/vectra/reference/)

## Support

> “Software is like sex: it’s better when it’s free.” – Linus Torvalds

If this package saved you some time, buying me a coffee is a nice way to
say thanks.

[![Buy Me A
Coffee](https://img.shields.io/badge/-Buy%20me%20a%20coffee-FFDD00?logo=buymeacoffee&logoColor=black)](https://buymeacoffee.com/gcol33)

## License

MIT (see the LICENSE.md file)

## Citation

``` bibtex
@software{vectra,
  author = {Colling, Gilles},
  title = {vectra: Columnar Query Engine for Larger-Than-RAM Data},
  year = {2026},
  url = {https://github.com/gcol33/vectra}
}
```
