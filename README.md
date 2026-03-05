# vectra

[![R-CMD-check](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml/badge.svg)](https://github.com/gcol33/vectra/actions/workflows/R-CMD-check.yml)
[![Codecov test coverage](https://codecov.io/gh/gcol33/vectra/graph/badge.svg)](https://app.codecov.io/gh/gcol33/vectra)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Columnar Query Engine for Larger-Than-RAM Data in R**

Query datasets that don't fit in memory using familiar dplyr verbs, backed by a pure C11 execution engine and a custom on-disk columnar format. No Java, no Spark, no Arrow dependency: just R and C.

## Quick Start

```r
library(vectra)

# Write any data.frame to disk
write_vtr(mtcars, "cars.vtr")

# Build a lazy query, nothing runs until collect()
tbl("cars.vtr") |>
  filter(cyl > 4) |>
  group_by(cyl) |>
  summarise(avg_mpg = mean(mpg), n = n()) |>
  collect()
```

## Statement of Need

Working with datasets larger than available RAM in R typically requires either loading a JVM (JDBC/Spark), linking against a system library (Arrow/DuckDB), or switching languages entirely. These tools are powerful but come with heavy dependency chains that complicate installation and deployment.

vectra takes a different approach: a self-contained C11 engine compiled as a standard R extension with zero external dependencies. Data lives in a custom columnar format (`.vtr`) and flows through a pull-based pipeline one row group at a time, so peak memory stays bounded regardless of file size.

## Features

### I/O & Storage

- **`write_vtr()`**: Serialize any data.frame to the `.vtr` columnar format
  - Supported types: integer, double, logical, character, `bit64::integer64`
  - Multi-row-group writes via `batch_size` for streaming reads
  - NA support with per-column validity bitmaps

- **`tbl()`**: Open a `.vtr` file as a lazy query
  - No data is read until `collect()` is called
  - Column pruning: only columns referenced by the query are loaded

### Transformation Verbs

- **`filter()`**: Row filtering with arbitrary boolean expressions
  - Arithmetic, comparisons, `&`, `|`, `!`, `is.na()`
  - Zero-copy via selection vectors (no row duplication)

- **`mutate()` / `transmute()`**: Column expressions
  - Arithmetic (`+`, `-`, `*`, `/`, `%%`), comparisons, boolean logic
  - `across()` for multi-column operations
  - Window functions in grouped context (see below)

- **`select()` / `rename()` / `relocate()`**: Column selection and reordering
  - Full `tidyselect` support (`starts_with()`, `where()`, etc.)

### Aggregation

- **`group_by()` + `summarise()`**: Hash-based grouped aggregation
  - Aggregation functions: `n()`, `sum()`, `mean()`, `min()`, `max()`
  - `na.rm` support on all aggregation functions
  - `.groups` parameter for controlling residual grouping

- **`count()` / `tally()`**: Shorthand counting with optional `wt` column
- **`distinct()`**: Unique row selection via hash grouping
- **`reframe()`**: Multi-row grouped summaries

### Joins

- **`left_join()` / `inner_join()` / `right_join()` / `full_join()`**: Hash joins
  - Natural join (common columns), named keys (`c("a" = "b")`), or explicit `by`
  - Suffix handling for overlapping non-key columns

- **`semi_join()` / `anti_join()`**: Filtering joins
- **`bind_rows()` / `bind_cols()`**: Concatenation across queries

### Window Functions

- **Ranking**: `row_number()`, `rank()`, `dense_rank()`
- **Offset**: `lag()`, `lead()` with configurable offset and default
- **Cumulative**: `cumsum()`, `cummean()`, `cummin()`, `cummax()`
- Used inside grouped `mutate()` for per-group computation

### Ordering & Slicing

- **`arrange()`**: Multi-column sorting with `desc()` for descending order
- **`slice_head()` / `slice_tail()`**: First or last n rows
- **`slice_min()` / `slice_max()`**: Rows with extreme values in a column

### Inspection

- **`explain()`**: Print the full execution plan tree with node types and schemas
- **`print()`**: Column names and types without reading data
- **`pull()`**: Extract a single column as a vector

### Zero External Dependencies

The C11 engine compiles with R's standard toolchain. Runtime dependencies are limited to `rlang` and `tidyselect` for NSE handling. No system libraries, no JVM, no compilation flags beyond what `R CMD INSTALL` provides.

## Installation

Development version:

```r
# install.packages("pak")
pak::pak("gcol33/vectra")
```

## Usage Examples

### Basic Query (`filter` + `select`)

```r
library(vectra)

# Write a data.frame to disk
write_vtr(nycflights13::flights, "flights.vtr")

# Lazy query: nothing runs until collect()
tbl("flights.vtr") |>
  filter(dep_delay > 60, carrier == "UA") |>
  select(year, month, day, dep_delay, arr_delay) |>
  collect()
```

### Grouped Aggregation (`group_by` + `summarise`)

```r
tbl("flights.vtr") |>
  group_by(carrier) |>
  summarise(
    avg_delay = mean(dep_delay, na.rm = TRUE),
    n_flights = n()
  ) |>
  arrange(desc(avg_delay)) |>
  collect()
```

### Joins Across Files

```r
write_vtr(nycflights13::airlines, "airlines.vtr")

tbl("flights.vtr") |>
  left_join(tbl("airlines.vtr"), by = "carrier") |>
  group_by(name) |>
  summarise(avg_delay = mean(dep_delay, na.rm = TRUE)) |>
  collect()
```

### Window Functions

```r
# Per-carrier cumulative delay and ranking
tbl("flights.vtr") |>
  filter(month == 1) |>
  group_by(carrier) |>
  mutate(rn = row_number(), cum_delay = cumsum(dep_delay)) |>
  select(carrier, dep_delay, rn, cum_delay) |>
  collect()
```

### Multi-Column Operations (`across`)

```r
tbl("flights.vtr") |>
  group_by(carrier) |>
  summarise(across(c(dep_delay, arr_delay), mean, na.rm = TRUE)) |>
  collect()
```

### Streaming Large Files

```r
# Write in 100k-row chunks for bounded memory reads
write_vtr(big_df, "big.vtr", batch_size = 100000)

# Query still works the same way
tbl("big.vtr") |>
  filter(x > 0) |>
  group_by(g) |>
  summarise(total = sum(x)) |>
  collect()
```

### Inspect the Execution Plan

```r
tbl("flights.vtr") |>
  filter(month == 1) |>
  select(carrier, dep_delay) |>
  group_by(carrier) |>
  summarise(n = n()) |>
  explain()
#> vectra execution plan
#>
#>   GroupAgg [carrier] -> [n]
#>     Project [carrier, dep_delay]
#>       Filter
#>         Scan flights.vtr
```

## How It Works

1. `write_vtr()` serializes a data.frame into a columnar `.vtr` file split into row groups
2. `tbl()` opens the file and returns a lazy `vectra_node`
3. Each verb (`filter`, `select`, `mutate`, ...) appends a plan node without reading data
4. `collect()` pulls batches through the node tree one row group at a time
5. The C engine evaluates expressions, applies filters via selection vectors (zero-copy), hashes groups, and joins using hash tables

## Support

> "Software is like sex: it's better when it's free." -- Linus Torvalds

I'm a PhD student who builds R packages in my free time because I believe good tools should be free and open. I started these projects for my own work and figured others might find them useful too.

If this package saved you some time, buying me a coffee is a nice way to say thanks. It helps with my coffee addiction.

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
