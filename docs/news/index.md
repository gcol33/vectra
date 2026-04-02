# Changelog

## vectra 0.2.2

### Query optimizer

- Column pruning: scan nodes only read columns needed by the query plan.
- Predicate pushdown: filter predicates are attached to scan nodes and
  use `.vtr` v3 per-rowgroup min/max statistics to skip entire row
  groups.

### Engine

- `.vtr` format version 3 with per-column per-rowgroup statistics
  (min/max).
- O(n log n) [`rank()`](https://rdrr.io/r/base/rank.html) and
  `dense_rank()` (replaces O(n²) comparison-based).
- Nested expressions in
  [`summarise()`](https://gcol33.github.io/vectra/reference/summarise.md):
  `summarise(m = mean(x + y))` auto-inserts a hidden mutate.

### Expressions

- [`nchar()`](https://rdrr.io/r/base/nchar.html): returns string length
  as integer.
- `substr(x, start, stop)`: substring extraction (1-based, like R).
- `grepl(pattern, x)`: fixed string matching (no regex).

## vectra 0.2.1

### Engine

- External merge sort with 1 GB memory budget and automatic
  spill-to-disk.
- Sort-based `group_by() |> summarise()` path for spill-safe
  aggregation.
- Chunked FULL join finalize (65,536 rows per batch).
- Automatic type coercion (`int64 <-> double`) in join keys and
  [`bind_rows()`](https://gcol33.github.io/vectra/reference/bind_rows.md).
- [`rank()`](https://rdrr.io/r/base/rank.html) and `dense_rank()` window
  functions.

### Type system

- `.vtr` format version 2 with per-column annotations.
- Date, POSIXct, and factor columns roundtrip through
  [`write_vtr()`](https://gcol33.github.io/vectra/reference/write_vtr.md)
  / [`collect()`](https://gcol33.github.io/vectra/reference/collect.md).
- `where()` predicates work in
  [`select()`](https://gcol33.github.io/vectra/reference/select.md),
  [`rename()`](https://gcol33.github.io/vectra/reference/rename.md),
  [`relocate()`](https://gcol33.github.io/vectra/reference/relocate.md),
  and [`across()`](https://gcol33.github.io/vectra/reference/across.md).

### Infrastructure

- Engine reference vignette
  ([`vignette("engine")`](https://gcol33.github.io/vectra/articles/engine.md)).
- 17-scenario benchmark suite with baseline snapshots and regression
  thresholds.
- ASAN/UBSAN CI job on Linux.
- Benchmark smoke job on PRs.

## vectra 0.1.0

- Initial release.
- Custom columnar on-disk format (`.vtr`) with multi-row-group support.
- dplyr-compatible verbs:
  [`filter()`](https://gcol33.github.io/vectra/reference/filter.md),
  [`select()`](https://gcol33.github.io/vectra/reference/select.md),
  [`mutate()`](https://gcol33.github.io/vectra/reference/mutate.md),
  [`transmute()`](https://gcol33.github.io/vectra/reference/transmute.md),
  [`rename()`](https://gcol33.github.io/vectra/reference/rename.md),
  [`relocate()`](https://gcol33.github.io/vectra/reference/relocate.md),
  [`group_by()`](https://gcol33.github.io/vectra/reference/group_by.md),
  [`summarise()`](https://gcol33.github.io/vectra/reference/summarise.md),
  [`count()`](https://gcol33.github.io/vectra/reference/count.md),
  [`tally()`](https://gcol33.github.io/vectra/reference/count.md),
  [`distinct()`](https://gcol33.github.io/vectra/reference/distinct.md),
  [`reframe()`](https://gcol33.github.io/vectra/reference/reframe.md),
  [`arrange()`](https://gcol33.github.io/vectra/reference/arrange.md),
  [`slice_head()`](https://gcol33.github.io/vectra/reference/slice_head.md),
  [`slice_tail()`](https://gcol33.github.io/vectra/reference/slice_head.md),
  [`slice_min()`](https://gcol33.github.io/vectra/reference/slice_head.md),
  [`slice_max()`](https://gcol33.github.io/vectra/reference/slice_head.md),
  [`pull()`](https://gcol33.github.io/vectra/reference/pull.md).
- Hash joins:
  [`left_join()`](https://gcol33.github.io/vectra/reference/left_join.md),
  [`inner_join()`](https://gcol33.github.io/vectra/reference/left_join.md),
  [`right_join()`](https://gcol33.github.io/vectra/reference/left_join.md),
  [`full_join()`](https://gcol33.github.io/vectra/reference/left_join.md),
  [`semi_join()`](https://gcol33.github.io/vectra/reference/left_join.md),
  [`anti_join()`](https://gcol33.github.io/vectra/reference/left_join.md).
- [`bind_rows()`](https://gcol33.github.io/vectra/reference/bind_rows.md)
  and
  [`bind_cols()`](https://gcol33.github.io/vectra/reference/bind_rows.md)
  for combining queries.
- Window functions: `row_number()`,
  [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
  [`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
  [`cummin()`](https://rdrr.io/r/base/cumsum.html),
  [`cummax()`](https://rdrr.io/r/base/cumsum.html).
- [`across()`](https://gcol33.github.io/vectra/reference/across.md)
  support in
  [`mutate()`](https://gcol33.github.io/vectra/reference/mutate.md) and
  [`summarise()`](https://gcol33.github.io/vectra/reference/summarise.md).
- [`explain()`](https://gcol33.github.io/vectra/reference/explain.md)
  for inspecting the execution plan.
- `tidyselect` integration for column selection helpers.
- Data sources: `.vtr`, CSV, SQLite, GeoTIFF.
- Data sinks:
  [`write_csv()`](https://gcol33.github.io/vectra/reference/write_csv.md),
  [`write_sqlite()`](https://gcol33.github.io/vectra/reference/write_sqlite.md),
  [`write_tiff()`](https://gcol33.github.io/vectra/reference/write_tiff.md).
