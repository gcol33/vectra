# vectra Engine Reference

This document defines vectra’s public contract: which operations are
supported, how types and coercion work, what streams and what
materializes, and where guarantees stop. It is the reference for what
vectra promises.

See [`?tbl`](https://gillescolling.com/vectra/reference/tbl.md),
[`?filter`](https://gillescolling.com/vectra/reference/filter.md),
[`?left_join`](https://gillescolling.com/vectra/reference/left_join.md),
and [`?explain`](https://gillescolling.com/vectra/reference/explain.md)
for function-level documentation.

## What vectra is

vectra is an R-native columnar query engine for datasets that don’t fit
in memory. It provides dplyr-style verbs backed by a pure C11 pull-based
execution engine and a custom on-disk format (`.vtr`). All operations
are lazy until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
materializes results as an R data.frame.

vectra is not a dplyr backend plugin. It defines its own S3 generics.
The verbs share names and semantics with dplyr, but do not depend on it.

## Execution model

### Pull-based pipeline

Every verb (`filter`, `select`, `mutate`, …) builds a plan node. Nodes
form a tree. No data moves until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
calls the root node’s `next_batch()` function, which pulls data through
the tree one **row group** at a time.

A row group (internally: `VecBatch`) is a set of columnar arrays,
typically thousands to millions of rows. Each column is a typed array
with a validity bitmap for NA support.

``` r

# Nothing executes here --- just builds a plan tree
plan <- tbl("data.vtr") |>
  filter(x > 0) |>
  select(id, x, y) |>
  mutate(z = x + y)

# Data flows when you call collect()
result <- collect(plan)

# Or inspect the plan without executing it
explain(plan)
```

### Selection vectors (zero-copy filtering)

[`filter()`](https://gillescolling.com/vectra/reference/filter.md) does
not copy rows. It attaches a **selection vector** to the batch: an
integer array indexing which physical rows pass the predicate.
Downstream nodes read only the selected rows. This avoids memory
allocation and copying for selective filters.

### Columnar storage

Data is stored and processed column-by-column, not row-by-row. This
means operations that touch few columns (e.g. `select(id, x)` on a
100-column table) only read the columns they need from disk.

## Data sources

| Function                  | Format                 | Streaming                |
|:--------------------------|:-----------------------|:-------------------------|
| `tbl(path)`               | `.vtr` (vectra native) | yes, row-group-at-a-time |
| `tbl_csv(path)`           | CSV                    | yes, batch-at-a-time     |
| `tbl_sqlite(path, table)` | SQLite                 | yes, batch-at-a-time     |
| `tbl_tiff(path)`          | GeoTIFF                | yes, row-strip-at-a-time |

All sources produce the same `vectra_node` object. The query engine does
not know or care which source is upstream.

### Output sinks

| Function | Format | Streaming |
|:---|:---|:---|
| [`collect()`](https://gillescolling.com/vectra/reference/collect.md) | R data.frame | materializes full result in R memory |
| `write_vtr(df, path)` | `.vtr` | writes from data.frame |
| `write_csv(x, path)` | CSV | streams batch-by-batch |
| `write_sqlite(x, path, table)` | SQLite | streams batch-by-batch |
| `write_tiff(x, path)` | GeoTIFF | streams batch-by-batch |

## Supported verbs

### Transformation verbs

| Verb | Streams | Notes |
|:---|:---|:---|
| `filter(...)` | yes | Zero-copy via selection vector |
| `select(...)` | yes | Full tidyselect: `starts_with()`, `where()`, `-col`, etc. |
| `mutate(...)` | yes | Arithmetic, comparison, boolean, [`is.na()`](https://rdrr.io/r/base/NA.html), [`nchar()`](https://rdrr.io/r/base/nchar.html), [`substr()`](https://rdrr.io/r/base/substr.html), [`grepl()`](https://rdrr.io/r/base/grep.html), math (`abs`, `sqrt`, `log`, `exp`, `floor`, `ceiling`, `round`, `log2`, `log10`, `sign`, `trunc`), `if_else()`, `between()`, `%in%`, type casting (`as.numeric`), [`tolower()`](https://rdrr.io/r/base/chartr.html), [`toupper()`](https://rdrr.io/r/base/chartr.html), [`trimws()`](https://rdrr.io/r/base/trimws.html), [`paste0()`](https://rdrr.io/r/base/paste.html), [`gsub()`](https://rdrr.io/r/base/grep.html), [`sub()`](https://rdrr.io/r/base/grep.html), [`startsWith()`](https://rdrr.io/r/base/startsWith.html), [`endsWith()`](https://rdrr.io/r/base/startsWith.html), [`pmin()`](https://rdrr.io/r/base/Extremes.html), [`pmax()`](https://rdrr.io/r/base/Extremes.html), `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`, [`as.Date()`](https://rdrr.io/r/base/as.Date.html) |
| `transmute(...)` | yes | Like [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) but drops unmentioned columns |
| `rename(...)` | yes | Full tidyselect rename support |
| `relocate(...)` | yes | Reorder columns with `.before` / `.after` |

### Aggregation verbs

| Verb | Streams | Notes |
|:---|:---|:---|
| `group_by(...)` | metadata only | Attaches grouping info; no data moves |
| `summarise(...)` | **materializes** | Hash-based or sort-based aggregation |
| [`ungroup()`](https://gillescolling.com/vectra/reference/ungroup.md) | metadata only | Removes grouping |
| `count(...)` | **materializes** | Sugar for `group_by() |> summarise(n = n())` |
| `tally(...)` | **materializes** | Like [`count()`](https://gillescolling.com/vectra/reference/count.md) on existing groups |

Supported aggregation functions: `n()`,
[`sum()`](https://rdrr.io/r/base/sum.html),
[`mean()`](https://rdrr.io/r/base/mean.html),
[`min()`](https://rdrr.io/r/base/Extremes.html),
[`max()`](https://rdrr.io/r/base/Extremes.html),
[`sd()`](https://rdrr.io/r/stats/sd.html),
[`var()`](https://rdrr.io/r/stats/cor.html), `first()`, `last()`,
[`any()`](https://rdrr.io/r/base/any.html),
[`all()`](https://rdrr.io/r/base/all.html),
[`median()`](https://rdrr.io/r/stats/median.html), `n_distinct()`. All
accept `na.rm = TRUE`.

### Ordering verbs

| Verb | Streams | Notes |
|:---|:---|:---|
| `arrange(...)` | **materializes** | External merge sort with 1 GB spill budget |
| `slice_head(n)` | yes | Limit node, stops after n rows |
| `slice_tail(n)` | **materializes** | Must see all rows to take last n |
| `slice_min(order_by, n)` | partial | Heap-based top-N; `with_ties = TRUE` (default) includes ties |
| `slice_max(order_by, n)` | partial | Heap-based top-N; `with_ties = TRUE` (default) includes ties |
| `head(n)` | yes | Alias for `slice_head() |> collect()` |
| `slice(...)` | **materializes** | Select or exclude rows by position (positive or negative indices) |
| `distinct(...)` | **materializes** | Uses hash-based grouping |

### Join verbs

| Verb | Streams | Notes |
|:---|:---|:---|
| `inner_join(x, y)` | **build materializes right** | Hash join; left streams |
| `left_join(x, y)` | **build materializes right** | Hash join; left streams |
| `right_join(x, y)` | **build materializes left** | Implemented as swapped left join |
| `full_join(x, y)` | **build materializes right** | Hash join + finalize pass |
| `semi_join(x, y)` | **build materializes right** | Hash join; returns left rows only |
| `anti_join(x, y)` | **build materializes right** | Hash join; returns non-matching left rows |
| `cross_join(x, y)` | **materializes** | Cartesian product; no key columns required |

All joins support: `by = "col"`, `by = c("a" = "b")`, `by = NULL`
(natural join), and `suffix = c(".x", ".y")`.

### Window functions

Available inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md):

| Function | Description |
|:---|:---|
| `row_number()` | Sequential row number (respects groups) |
| `rank(col)` | Min rank with gaps for ties (like [`dplyr::min_rank()`](https://dplyr.tidyverse.org/reference/row_number.html)) |
| `dense_rank(col)` | Consecutive rank without gaps |
| `lag(col, n, default)` | Previous value |
| `lead(col, n, default)` | Next value |
| `cumsum(col)` | Cumulative sum |
| `cummean(col)` | Cumulative mean |
| `cummin(col)` | Cumulative minimum |
| `cummax(col)` | Cumulative maximum |
| `ntile(n)` | Divide rows into n roughly equal buckets |
| `percent_rank(col)` | Relative rank scaled to \[0, 1\] |
| `cume_dist(col)` | Cumulative distribution (proportion of values \<= current) |

Window functions respect
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
partitions. They **materialize** all data within each partition.

### Other verbs

| Verb | Streams | Notes |
|:---|:---|:---|
| `pull(var)` | **materializes** | Collects one column as a vector |
| `bind_rows(...)` | yes | Streaming concat if schemas are compatible |
| `bind_cols(...)` | **materializes** | Collects all inputs, then [`cbind()`](https://rdrr.io/r/base/cbind.html) |
| `across(...)` | n/a | Column expansion helper for [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)/[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md) |
| [`explain()`](https://gillescolling.com/vectra/reference/explain.md) | n/a | Prints the plan tree |
| [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md) | **materializes** (preview only) | Shows column types and first few values |

### tidyselect support

[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`rename()`](https://gillescolling.com/vectra/reference/rename.md),
[`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
[`distinct()`](https://gillescolling.com/vectra/reference/distinct.md),
and `across(.cols)` support the full tidyselect vocabulary:

- `starts_with()`, `ends_with()`, `contains()`, `matches()`

- `everything()`, `last_col()`

- `all_of()`, `any_of()`

- `where()` predicates (e.g. `where(is.numeric)`)

- `-column` negation

`where()` works because vectra builds a 0-row typed proxy data.frame
from the schema, giving tidyselect enough type information to evaluate
predicates.

## Supported types

### Base types

| C type   | R input   | R output (default) | R output (bit64 mode) |
|:---------|:----------|:-------------------|:----------------------|
| `int64`  | integer   | double             | integer64             |
| `double` | double    | double             | double                |
| `bool`   | logical   | logical            | logical               |
| `string` | character | character          | character             |

R’s 32-bit `integer` is widened to 64-bit `int64` on write. On read,
`int64` is returned as `double` by default (R has no native 64-bit
integer). Set `options(vectra.int64 = "bit64")` to get
[`bit64::integer64`](https://rdrr.io/pkg/bit64/man/bit64-package.html)
output instead.

### Annotated types

The `.vtr` format version 2 stores per-column annotations that preserve
R type metadata through the write/read cycle:

| R class | Annotation | Storage | Roundtrip |
|:---|:---|:---|:---|
| Date | `"Date"` | double (days since epoch) | exact |
| POSIXct | `"POSIXct\|tz"` | double (seconds since epoch) | exact (tz preserved) |
| factor | `"factor\|lev1\|lev2\|..."` | string | exact (levels + order preserved) |

Annotations are metadata. The underlying C engine operates on the base
types only. Type restoration happens at
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
time.

Date and POSIXct columns support component extraction via `year()`,
`month()`, `day()`, `hour()`, `minute()`, `second()` in
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
[`filter()`](https://gillescolling.com/vectra/reference/filter.md)
expressions. Use `as.Date("2020-01-01")` as a literal in filter
comparisons. Date arithmetic (adding/subtracting days) works via
standard `+` and `-`.

## Coercion rules

### Arithmetic and comparison expressions

The coercion hierarchy for numeric operations is:

    bool < int64 < double

When an expression combines two different numeric types, the narrower
type is promoted to the wider type before evaluation. String columns
cannot participate in arithmetic or comparison with numeric columns —
this is an error.

### Join key coercion

Join keys follow the same hierarchy. If the left key is `int64` and the
right key is `double`, the `int64` side is coerced to `double` for
hashing and comparison. The coercion happens internally; the output
column retains the original left-side type.

Joining a `string` key against a numeric key is an error.

### bind_rows coercion

When
[`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
combines tables with different column types, it computes the common type
using the same `bool < int64 < double` hierarchy. Per-batch coercion
happens at the C level during streaming — no R fallback needed.

If column names differ across inputs, the R fallback path is used: all
inputs are collected, aligned by column name, and combined with
[`rbind()`](https://rdrr.io/r/base/cbind.html).

## NA semantics

### Storage

NAs are tracked by a per-column validity bitmap. Every column of every
type supports NA values. The bitmap is bit-packed (1 bit per row, 1 =
valid).

### Propagation

- **Arithmetic**: `NA + x = NA`, `NA * x = NA`

- **Comparison**: `NA > x = NA`, `x == NA = NA`

- **Boolean**: `NA & FALSE = FALSE`, `NA & TRUE = NA`,
  `NA | TRUE = TRUE`, `NA | FALSE = NA`

- **Aggregation**: NAs are included by default; use `na.rm = TRUE` to
  exclude

- **Joins**: NA keys never match (same as SQL NULL semantics)

- **Window functions**: [`cumsum()`](https://rdrr.io/r/base/cumsum.html)
  and friends propagate NA forward

### is.na()

`is.na(col)` is supported in
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
expressions. It returns a boolean column based on the validity bitmap.

## Ordering guarantees

- [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md),
  [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md):
  preserve input order

- [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md):
  produces a total order (stable sort)

- `group_by() |> summarise()`: output order is **not guaranteed**
  (hash-based path) or sorted by key (sort-based path); do not depend on
  either

- [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md):
  output order is not guaranteed

- Joins: probe-side order is preserved within each batch; build-side
  order is not guaranteed

- [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md):
  child order is preserved (first child’s rows, then second’s, etc.)

## Streaming vs materializing

### Streaming nodes (constant memory per batch)

- Scan (`.vtr`, CSV, SQLite, TIFF)

- Filter

- Project (select / mutate / rename / relocate / transmute)

- Limit (slice_head, head)

- Concat (bind_rows)

### Materializing nodes

These nodes buffer data in memory:

| Node | What it buffers | Bounded by |
|:---|:---|:---|
| Sort (arrange) | All input rows | 1 GB memory budget, then spills to disk |
| GroupAgg (summarise) | Hash table of groups + accumulators | Number of distinct groups |
| TopN (slice_min/max) | Heap of n rows | Requested n |
| Window | All rows per partition | Partition size |
| Join (build phase) | Right-side table in hash table | Right-side row count |

### External sort (spill-to-disk)

[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
uses an external merge sort when accumulated data exceeds 1 GB. Sorted
runs are spilled to temporary `.vtr` files, then merged via a k-way
min-heap. This keeps peak memory bounded regardless of input size.

The sort-based `group_by() |> summarise()` path (used internally when
the engine detects it is advantageous) also benefits from this spill
mechanism.

### Join memory model

Joins use a **build-right, probe-left** hash join:

1.  **Build phase**: The entire right-side table is materialized into a
    hash table in memory.
2.  **Probe phase**: Left-side batches stream through one at a time,
    probing the hash table.
3.  **Finalize phase** (full_join only): Unmatched right-side rows are
    emitted in chunks of 65,536 rows.

The memory cost of a join is proportional to the right-side table size.
The left side streams and does not accumulate.

## The .vtr file format

The `.vtr` format is vectra’s native binary columnar format. It is
designed for fast sequential reads with row-group-level granularity.

### Layout

    Header:
      magic bytes ("VTR1")
      version (1 or 2)
      n_cols, n_rowgroups
      per-column: name + type byte [+ annotation string in v2]
      row group index (byte offsets)

    Row groups (repeated):
      per-column:
        validity bitmap (bit-packed)
        typed data array (int64/double/bool/string)

### Version history

- **Version 1**: Base format with typed columns and validity bitmaps.

- **Version 2**: Adds per-column annotation strings for Date, POSIXct,
  and factor roundtripping.

- **Version 3** (current): Adds per-column per-rowgroup statistics
  (min/max) enabling predicate pushdown. Writing always produces v3. All
  versions are readable.

## Query optimizer

[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
runs the optimizer before printing so you see the actual execution plan.
Two optimization passes run automatically:

### Predicate pushdown

When a `FilterNode` sits above a `ScanNode` reading a v3 `.vtr` file,
the filter predicate is attached to the scan. The scan uses per-rowgroup
min/max statistics to skip row groups that cannot contain matching rows.
This is visible in
[`explain()`](https://gillescolling.com/vectra/reference/explain.md) as
`predicate pushdown` on the ScanNode.

### Column pruning

The optimizer walks the plan tree top-down and determines which columns
each node actually needs from its child. At scan nodes, unneeded columns
are excluded from disk reads. This is visible in
[`explain()`](https://gillescolling.com/vectra/reference/explain.md) as
`2/5 cols (pruned)`.

### Hidden mutate insertion

When
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
contains nested expressions like `mean(x + y)`, the optimizer
auto-inserts a `ProjectNode` (visible as `hidden mutate` in
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)) to
compute the intermediate result before aggregation.

## explain() contract

[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
prints the optimized plan tree without executing it. The output shows:

- Node types in execution order (leaf to root)

- Per-node annotations: streaming/materializing, column pruning,
  predicate pushdown, v3 stats, hidden mutate

- Grouping columns if present

- Output schema (column names and types)

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

The plan tree is a description of what will happen, not a guarantee of
how it will happen internally. Node ordering and naming may change
between versions.

## Current limitations

- **slice_tail materializes**: There is no reverse-scan optimization.

- **distinct with .keep_all**: Falls back to R when `.keep_all = TRUE`
  with a column subset.

- **Limited string operations**:
  [`nchar()`](https://rdrr.io/r/base/nchar.html),
  [`substr()`](https://rdrr.io/r/base/substr.html),
  [`grepl()`](https://rdrr.io/r/base/grep.html) (fixed match only),
  [`tolower()`](https://rdrr.io/r/base/chartr.html),
  [`toupper()`](https://rdrr.io/r/base/chartr.html),
  [`trimws()`](https://rdrr.io/r/base/trimws.html),
  [`paste0()`](https://rdrr.io/r/base/paste.html) (2 args),
  [`gsub()`](https://rdrr.io/r/base/grep.html)/[`sub()`](https://rdrr.io/r/base/grep.html)
  (fixed patterns),
  [`startsWith()`](https://rdrr.io/r/base/startsWith.html),
  [`endsWith()`](https://rdrr.io/r/base/startsWith.html) are supported.
  No regex or multi-argument
  [`paste()`](https://rdrr.io/r/base/paste.html). Use
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  first for unsupported string operations.

- **Single-threaded**: The engine uses one thread. Parallelism is not
  planned.

- **Predicate pushdown is .vtr only**: CSV, SQLite, and TIFF scans do
  not benefit from predicate pushdown or column pruning.

## Fallback behavior

vectra has these fallback paths to base R:

1.  **bind_rows with mismatched column names**: If column names differ
    across inputs, all tables are collected and combined via
    [`rbind()`](https://rdrr.io/r/base/cbind.html) in R.

2.  **distinct with .keep_all and column subset**: Falls back to
    [`duplicated()`](https://rdrr.io/r/base/duplicated.html) in R (emits
    a message).

3.  **slice_tail**: Must see all rows to take the last n; returns a
    data.frame.

4.  **slice_min/slice_max with `with_ties = TRUE`** (the default):
    Collects all data to identify ties at the boundary; returns a
    data.frame.

5.  **reframe**: Always collects and evaluates in R; returns a
    data.frame.

All other operations execute entirely in C. There is no silent fallback
to dplyr or any other package.

## Grouping preservation

All verbs preserve
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
metadata:
[`filter()`](https://gillescolling.com/vectra/reference/filter.md),
[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`rename()`](https://gillescolling.com/vectra/reference/rename.md),
[`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
and
[`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
pass grouping through.
[`rename()`](https://gillescolling.com/vectra/reference/rename.md)
additionally updates group column names to match the rename.
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
drops grouping according to its `.groups` argument.

## Package conflicts

vectra defines its own S3 generics for dplyr-like verbs (`filter`,
`select`, `mutate`, etc.) and utility functions (`glimpse`, `collect`).
If dplyr is also loaded, whichever package was attached last will mask
the other’s generics. vectra’s methods will still dispatch correctly on
`vectra_node` objects regardless of masking order.
