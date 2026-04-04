# vectra

[![CRAN status](https://www.r-pkg.org/badges/version/vectra)](https://CRAN.R-project.org/package=vectra)
[![R-CMD-check](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml)
[![ASAN/UBSAN](https://github.com/gcol33/vectra/actions/workflows/sanitizers.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/sanitizers.yml)
[![Codecov test coverage](https://codecov.io/gh/gcol33/vectra/graph/badge.svg)](https://app.codecov.io/gh/gcol33/vectra)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

vectra is an R-native columnar query engine for datasets larger than RAM.

Write dplyr-style pipelines against multi-GB files on a laptop. Data streams through a C11 pull-based engine one row group at a time, so peak memory stays bounded regardless of file size.

## Quick Start

Point vectra at any file and query it with dplyr verbs. Nothing runs until `collect()`.

```r
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

# SQLite — zero-dependency, no DBI required
tbl_sqlite("survey.db", "responses") |>
  filter(year == 2025) |>
  left_join(tbl_sqlite("survey.db", "sites"), by = "site_id") |>
  collect()
```

For repeated queries, convert to vectra's native `.vtr` format for faster reads:

```r
write_vtr(big_df, "data.vtr", batch_size = 100000)

tbl("data.vtr") |>
  filter(x > 0, region == "EU") |>
  group_by(region) |>
  summarise(total = sum(value), n = n()) |>
  collect()
```

Append new data without rewriting the file, or do a key-based diff between two snapshots:

```r
# Append new rows as a new row group — existing data untouched
append_vtr(new_rows_df, "data.vtr")

# Logical diff: what was added or deleted between two snapshots?
d <- diff_vtr("snapshot_old.vtr", "snapshot_new.vtr", key_col = "id")
collect(d$added)   # rows present in new but not old
d$deleted          # key values present in old but not new
```

Fuzzy string matching runs inside the C engine, no round-trip to R:

```r
tbl("taxa.vtr") |>
  filter(levenshtein(species, "Quercus robur") <= 2) |>
  mutate(similarity = jaro_winkler(species, "Quercus robur")) |>
  arrange(desc(similarity)) |>
  collect()
```

Use `explain()` to inspect the optimized plan:

```r
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

Querying large datasets in R usually means Arrow (requires compiled binaries matching your platform), DuckDB (links a 30 MB bundled library), or Spark (requires a JVM and cluster configuration).

vectra is a self-contained C11 engine compiled as a standard R extension. No external libraries beyond zlib, no JVM, no runtime configuration. It provides:

- **Streaming execution**: data flows one row group at a time, never fully in memory
- **Zero-copy filtering**: selection vectors avoid row duplication
- **Query optimizer**: column pruning skips unneeded columns at scan; predicate pushdown uses per-rowgroup min/max statistics to skip entire row groups
- **Hash joins**: build right, stream left --- join a 50 GB fact table against a lookup without materializing both
- **External sort**: 1 GB memory budget with automatic spill-to-disk
- **Window functions**: `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`, `cumsum()`, `cummean()`, `cummin()`, `cummax()`
- **String expressions**: `nchar()`, `substr()`, `grepl()` evaluated in the engine without round-tripping to R
- **Multiple data sources**: `.vtr`, CSV, SQLite, GeoTIFF --- all produce the same lazy query nodes

## Features

| Category | Verbs |
|:---------|:------|
| **Transform** | `filter()`, `select()`, `mutate()`, `transmute()`, `rename()`, `relocate()` |
| **Aggregate** | `group_by()`, `summarise()` (`n`, `sum`, `mean`, `min`, `max`, `sd`, `var`, `first`, `last`, `any`, `all`, `median`, `n_distinct`), `count()`, `tally()`, `distinct()` |
| **Join** | `left_join()`, `inner_join()`, `right_join()`, `full_join()`, `semi_join()`, `anti_join()`, `cross_join()` |
| **Order** | `arrange()`, `slice_head()`, `slice_tail()`, `slice_min()`, `slice_max()`, `slice()` |
| **Window** | `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`, `cumsum()`, `cummean()`, `cummin()`, `cummax()`, `ntile()`, `percent_rank()`, `cume_dist()` |
| **Date/Time** | `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`, `as.Date()` (in `filter()`/`mutate()`) |
| **String** | `nchar()`, `substr()`, `grepl()`, `tolower()`, `toupper()`, `trimws()`, `paste0()`, `gsub()`, `sub()`, `startsWith()`, `endsWith()` (in `filter()`/`mutate()`) |
| **String similarity** | `levenshtein()`, `levenshtein_norm()`, `dl_dist()`, `dl_dist_norm()`, `jaro_winkler()` — fuzzy matching in `filter()`/`mutate()`, with optional `max_dist` early termination |
| **Expression** | `abs()`, `sqrt()`, `log()`, `exp()`, `floor()`, `ceiling()`, `round()`, `log2()`, `log10()`, `sign()`, `trunc()`, `if_else()`, `between()`, `%in%`, `as.numeric()`, `pmin()`, `pmax()`, `resolve()`, `propagate()` (in `filter()`/`mutate()`) |
| **Combine** | `bind_rows()`, `bind_cols()`, `across()` |
| **I/O** | `tbl()`, `tbl_csv()`, `tbl_sqlite()`, `tbl_tiff()`, `write_vtr()`, `write_csv()`, `write_sqlite()`, `write_tiff()`, `append_vtr()`, `delete_vtr()`, `diff_vtr()` |
| **Inspect** | `explain()`, `glimpse()`, `print()`, `pull()` |

Full tidyselect support in `select()`, `rename()`, `relocate()`, and `across()`: `starts_with()`, `ends_with()`, `contains()`, `matches()`, `where()`, `everything()`, `all_of()`, `any_of()`.

## Installation

```r
# CRAN
install.packages("vectra")

# Development version
pak::pak("gcol33/vectra")
```

## Documentation

- [Getting Started](https://gillescolling.com/vectra/articles/quickstart.html) — Full walkthrough with runnable examples
- [Format Backends](https://gillescolling.com/vectra/articles/formats.html) — CSV, SQLite, Excel, GeoTIFF, and streaming conversion pipelines
- [Joins](https://gillescolling.com/vectra/articles/joins.html) — All join types, fuzzy joins, key coercion, and memory model
- [String Operations](https://gillescolling.com/vectra/articles/string-ops.html) — Pattern matching, fuzzy matching, and block lookups
- [Indexing and Optimization](https://gillescolling.com/vectra/articles/indexing.html) — Hash indexes, zone-map pruning, column pruning, and reading `explain()` output
- [Working with Large Data](https://gillescolling.com/vectra/articles/large-data.html) — Streaming pipelines, append/delete/diff, external sort, and memory budgeting
- [Engine Reference](https://gillescolling.com/vectra/articles/engine.html) — Execution model, types, coercion, .vtr format, and limitations
- [Function Reference](https://gillescolling.com/vectra/reference/)

## Support

> "Software is like sex: it's better when it's free." -- Linus Torvalds

If this package saved you some time, buying me a coffee is a nice way to say thanks.

[![Buy Me A Coffee](https://img.shields.io/badge/-Buy%20me%20a%20coffee-FFDD00?logo=buymeacoffee&logoColor=black)](https://buymeacoffee.com/gcol33)

## License

MIT (see the LICENSE.md file)

## Citation

```bibtex
@software{vectra,
  author = {Colling, Gilles},
  title = {vectra: Columnar Query Engine for Larger-Than-RAM Data},
  year = {2026},
  url = {https://github.com/gcol33/vectra}
}
```
