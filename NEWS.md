# vectra 0.2.2

## Query optimizer

* Column pruning: scan nodes only read columns needed by the query plan.
* Predicate pushdown: filter predicates are attached to scan nodes and use
  `.vtr` v3 per-rowgroup min/max statistics to skip entire row groups.

## Engine

* `.vtr` format version 3 with per-column per-rowgroup statistics (min/max).
* O(n log n) `rank()` and `dense_rank()` (replaces O(n²) comparison-based).
* Nested expressions in `summarise()`: `summarise(m = mean(x + y))` auto-inserts
  a hidden mutate.

## Expressions

* `nchar()`: returns string length as integer.
* `substr(x, start, stop)`: substring extraction (1-based, like R).
* `grepl(pattern, x)`: fixed string matching (no regex).
* `paste0(a, b)`: two-argument string concatenation.
* `gsub(pattern, replacement, x)` / `sub()`: fixed-string replacement.
* `startsWith()` / `endsWith()`: string prefix/suffix matching.
* `pmin()` / `pmax()`: element-wise minimum/maximum.
* `log2()`, `log10()`, `sign()`, `trunc()`: additional math functions.

## Aggregation

* `sd()` and `var()`: sample standard deviation and variance via Welford's
  online algorithm. Returns NA for groups with fewer than 2 values (R semantics).
* `first()` and `last()`: first and last non-NA value per group. Both support
  `na.rm = TRUE`.

## Verbs

* `slice_min()` and `slice_max()` gain a working `with_ties` parameter
  (default `TRUE`). Ties at the boundary are now included by default; use
  `with_ties = FALSE` for exactly `n` rows.
* `count()` and `tally()` gain a working `sort` parameter. `sort = TRUE`
  returns results in descending order of the count column.
* `transmute()` and `reframe()` now support `across()`.
* `distinct(.keep_all = TRUE)` with a column subset now emits a message when
  falling back to R.

## Utilities

* `glimpse()`: preview column names, types, and first few values without
  collecting the full result.
* `collect()` now works on data.frames (no-op), so `slice_min(...) |> collect()`
  works regardless of the `with_ties` path.

## Documentation

* New quickstart vignette: `vignette("quickstart")`.
* `@details` sections added to `filter()`, `mutate()`, `summarise()`,
  `arrange()`, `distinct()`, `count()`, and join functions.

# vectra 0.2.1

## Engine

* External merge sort with 1 GB memory budget and automatic spill-to-disk.
* Sort-based `group_by() |> summarise()` path for spill-safe aggregation.
* Chunked FULL join finalize (65,536 rows per batch).
* Automatic type coercion (`int64 <-> double`) in join keys and `bind_rows()`.
* `rank()` and `dense_rank()` window functions.

## Type system

* `.vtr` format version 2 with per-column annotations.
* Date, POSIXct, and factor columns roundtrip through `write_vtr()` / `collect()`.
* `where()` predicates work in `select()`, `rename()`, `relocate()`, and `across()`.

## Infrastructure

* Engine reference vignette (`vignette("engine")`).
* 17-scenario benchmark suite with baseline snapshots and regression thresholds.
* ASAN/UBSAN CI job on Linux.
* Benchmark smoke job on PRs.

# vectra 0.1.0

* Initial release.
* Custom columnar on-disk format (`.vtr`) with multi-row-group support.
* dplyr-compatible verbs: `filter()`, `select()`, `mutate()`, `transmute()`,
  `rename()`, `relocate()`, `group_by()`, `summarise()`, `count()`, `tally()`,
  `distinct()`, `reframe()`, `arrange()`, `slice_head()`, `slice_tail()`,
  `slice_min()`, `slice_max()`, `pull()`.
* Hash joins: `left_join()`, `inner_join()`, `right_join()`, `full_join()`,
  `semi_join()`, `anti_join()`.
* `bind_rows()` and `bind_cols()` for combining queries.
* Window functions: `row_number()`, `lag()`, `lead()`, `cumsum()`, `cummean()`,
  `cummin()`, `cummax()`.
* `across()` support in `mutate()` and `summarise()`.
* `explain()` for inspecting the execution plan.
* `tidyselect` integration for column selection helpers.
* Data sources: `.vtr`, CSV, SQLite, GeoTIFF.
* Data sinks: `write_csv()`, `write_sqlite()`, `write_tiff()`.
