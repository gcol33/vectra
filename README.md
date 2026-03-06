# vectra

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

Querying large datasets in R usually means Arrow, DuckDB, or Spark. These work, but the dependency chains are heavy and installation is fragile.

vectra is a self-contained C11 engine compiled as a standard R extension. No external libraries, no JVM, no runtime configuration. It provides:

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
| **Aggregate** | `group_by()`, `summarise()`, `count()`, `tally()`, `distinct()` |
| **Join** | `left_join()`, `inner_join()`, `right_join()`, `full_join()`, `semi_join()`, `anti_join()` |
| **Order** | `arrange()`, `slice_head()`, `slice_tail()`, `slice_min()`, `slice_max()` |
| **Window** | `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`, `cumsum()`, `cummean()`, `cummin()`, `cummax()` |
| **String** | `nchar()`, `substr()`, `grepl()` (in `filter()`/`mutate()`) |
| **Combine** | `bind_rows()`, `bind_cols()`, `across()` |
| **I/O** | `tbl()`, `tbl_csv()`, `tbl_sqlite()`, `tbl_tiff()`, `write_vtr()`, `write_csv()`, `write_sqlite()`, `write_tiff()` |
| **Inspect** | `explain()`, `print()`, `pull()` |

Full tidyselect support in `select()`, `rename()`, `relocate()`, and `across()`: `starts_with()`, `ends_with()`, `contains()`, `matches()`, `where()`, `everything()`, `all_of()`, `any_of()`.

## Installation

```r
# install.packages("pak")
pak::pak("gcol33/vectra")
```

## Documentation

- `vignette("engine")` --- execution model, supported types, coercion rules, memory guarantees, and current limitations

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
