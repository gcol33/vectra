# Changelog

## vectra 0.3.2

- Fix misaligned `int64_t` memory access in `vtr_codec.c` (UBSAN).
  Dictionary encoding wrote and read 8-byte offsets through an unaligned
  pointer; delta decoding had the same issue. All fixed with `memcpy`.

## vectra 0.3.1

- CRAN submission fixes: title case, quoted technical terms in
  DESCRIPTION, corrected documentation URLs.

## vectra 0.3.0

### File operations

- `append_vtr(df, path)`: append a data.frame as a new row group to an
  existing `.vtr` file. Existing row groups are never rewritten.
- `delete_vtr(path, row_ids)`: logically delete rows by 0-based physical
  index. Writes a tombstone side file (`<path>.del`); the `.vtr` file is
  never modified. Deletions are cumulative and excluded automatically on
  the next [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md)
  call.
- `diff_vtr(old_path, new_path, key_col)`: key-based logical diff
  between two `.vtr` files. Returns a list with `added` (a lazy
  `vectra_node`) and `deleted` (a vector of key values). Implemented as
  a single-pass C streaming engine with O(n_unique_keys) memory.

### Expressions

- [`tolower()`](https://rdrr.io/r/base/chartr.html),
  [`toupper()`](https://rdrr.io/r/base/chartr.html),
  [`trimws()`](https://rdrr.io/r/base/trimws.html): case conversion and
  whitespace trimming for string columns in
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md).
- `levenshtein(x, y)` / `levenshtein_norm(x, y)`: Levenshtein edit
  distance and normalised variant (0–1). Supports column-vs-column and
  column-vs-literal comparisons. Optional `max_dist` argument for early
  termination.
- `dl_dist(x, y)` / `dl_dist_norm(x, y)`: Damerau-Levenshtein distance
  (counts transpositions as cost 1) and normalised variant.
- `jaro_winkler(x, y)`: Jaro-Winkler similarity (0–1, higher = more
  similar). All string-similarity functions propagate `NA` and work in
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md).
- `resolve(fk, pk, value)`: scalar self-join — looks up `value` where
  `pk == fk` within the same batch. Useful for denormalising
  parent-child tables without a join.
- `propagate(parent_id, id, seed)`: tree-traversal aggregation —
  propagates non-NA `seed` values down a parent-child hierarchy until
  all reachable nodes are filled. Converges in O(depth) passes.

### Format

- `.vtr` format version 4 with a two-layer codec (no external
  dependencies):
  - Encoding: `PLAIN` (default), `DICTIONARY` (string columns with \<
    50% unique values), `DELTA` (monotonically increasing `int64`
    columns).
  - Compression: custom LZ77 byte compressor (`LZ_VTR`, ~120 lines of
    C). Applied after encoding; skipped for buffers \< 64 bytes or when
    compression does not reduce size. Files written with v4 are
    typically 30–60% smaller than v3.
    [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) reads
    v1–v4 files;
    [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
    always writes v4.

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
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md):
  `summarise(m = mean(x + y))` auto-inserts a hidden mutate.

### Expressions

- `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`:
  date/time component extraction for Date and POSIXct columns.
- [`as.Date()`](https://rdrr.io/r/base/as.Date.html) and
  [`as.POSIXct()`](https://rdrr.io/r/base/as.POSIXlt.html) literals in
  filter expressions (e.g. `filter(date > as.Date("2020-01-01"))`).
- `as.Date(string_col)`: convert ISO-format date strings to Date values.
- [`nchar()`](https://rdrr.io/r/base/nchar.html): returns string length
  as integer.
- `substr(x, start, stop)`: substring extraction (1-based, like R).
- `grepl(pattern, x)`: fixed string matching (no regex).
- `paste0(a, b)`: two-argument string concatenation.
- `gsub(pattern, replacement, x)` /
  [`sub()`](https://rdrr.io/r/base/grep.html): fixed-string replacement.
- [`startsWith()`](https://rdrr.io/r/base/startsWith.html) /
  [`endsWith()`](https://rdrr.io/r/base/startsWith.html): string
  prefix/suffix matching.
- [`pmin()`](https://rdrr.io/r/base/Extremes.html) /
  [`pmax()`](https://rdrr.io/r/base/Extremes.html): element-wise
  minimum/maximum.
- [`log2()`](https://rdrr.io/r/base/Log.html),
  [`log10()`](https://rdrr.io/r/base/Log.html),
  [`sign()`](https://rdrr.io/r/base/sign.html),
  [`trunc()`](https://rdrr.io/r/base/Round.html): additional math
  functions.

### Aggregation

- [`sd()`](https://rdrr.io/r/stats/sd.html) and
  [`var()`](https://rdrr.io/r/stats/cor.html): sample standard deviation
  and variance via Welford’s online algorithm. Returns NA for groups
  with fewer than 2 values (R semantics).
- `first()` and `last()`: first and last non-NA value per group. Both
  support `na.rm = TRUE`.

### Verbs

- [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  and
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  gain a working `with_ties` parameter (default `TRUE`). Ties at the
  boundary are now included by default; use `with_ties = FALSE` for
  exactly `n` rows.
- [`count()`](https://gillescolling.com/vectra/reference/count.md) and
  [`tally()`](https://gillescolling.com/vectra/reference/count.md) gain
  a working `sort` parameter. `sort = TRUE` returns results in
  descending order of the count column.
- [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
  and
  [`reframe()`](https://gillescolling.com/vectra/reference/reframe.md)
  now support
  [`across()`](https://gillescolling.com/vectra/reference/across.md).
- `distinct(.keep_all = TRUE)` with a column subset now emits a message
  when falling back to R.

### Utilities

- [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md):
  preview column names, types, and first few values without collecting
  the full result.
- [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  now works on data.frames (no-op), so `slice_min(...) |> collect()`
  works regardless of the `with_ties` path.

### Documentation

- New quickstart vignette:
  [`vignette("quickstart")`](https://gillescolling.com/vectra/articles/quickstart.md).
- `@details` sections added to
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
  [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
  [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md),
  [`count()`](https://gillescolling.com/vectra/reference/count.md), and
  join functions.

## vectra 0.2.1

### Engine

- External merge sort with 1 GB memory budget and automatic
  spill-to-disk.
- Sort-based `group_by() |> summarise()` path for spill-safe
  aggregation.
- Chunked FULL join finalize (65,536 rows per batch).
- Automatic type coercion (`int64 <-> double`) in join keys and
  [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md).
- [`rank()`](https://rdrr.io/r/base/rank.html) and `dense_rank()` window
  functions.

### Type system

- `.vtr` format version 2 with per-column annotations.
- Date, POSIXct, and factor columns roundtrip through
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
  /
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md).
- `where()` predicates work in
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md),
  [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
  and
  [`across()`](https://gillescolling.com/vectra/reference/across.md).

### Infrastructure

- Engine reference vignette
  ([`vignette("engine")`](https://gillescolling.com/vectra/articles/engine.md)).
- 17-scenario benchmark suite with baseline snapshots and regression
  thresholds.
- ASAN/UBSAN CI job on Linux.
- Benchmark smoke job on PRs.

## vectra 0.1.0

- Initial release.
- Custom columnar on-disk format (`.vtr`) with multi-row-group support.
- dplyr-compatible verbs:
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md),
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md),
  [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md),
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
  [`count()`](https://gillescolling.com/vectra/reference/count.md),
  [`tally()`](https://gillescolling.com/vectra/reference/count.md),
  [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md),
  [`reframe()`](https://gillescolling.com/vectra/reference/reframe.md),
  [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
  [`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`pull()`](https://gillescolling.com/vectra/reference/pull.md).
- Hash joins:
  [`left_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`right_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md).
- [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  and
  [`bind_cols()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  for combining queries.
- Window functions: `row_number()`,
  [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
  [`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
  [`cummin()`](https://rdrr.io/r/base/cumsum.html),
  [`cummax()`](https://rdrr.io/r/base/cumsum.html).
- [`across()`](https://gillescolling.com/vectra/reference/across.md)
  support in
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md).
- [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
  for inspecting the execution plan.
- `tidyselect` integration for column selection helpers.
- Data sources: `.vtr`, CSV, SQLite, GeoTIFF.
- Data sinks:
  [`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md),
  [`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md),
  [`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md).
