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
| `write_tiff(x, path, pixel_type)` | GeoTIFF | streams batch-by-batch; `pixel_type`: float64/float32/int16/int32/uint8/uint16 |

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
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html)
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
cannot participate in arithmetic or comparison with numeric columns;
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
accumulates incoming batches into column builders in memory. After each
batch, the sort node estimates total memory usage across all builders.
When the estimate exceeds the memory budget (default 1 GB, defined as
`DEFAULT_MEM_BUDGET`), the node flushes the accumulated data as a sorted
run:

1.  The builders are finished into arrays.

2.  The arrays are sorted in-place using a parallel merge sort (OpenMP
    task spawning above 32,768 rows, sequential below).

3.  The sorted data is written to a temporary `.vtr` file in the system
    temp directory, split into row groups of 65,536 rows each.

4.  The builders are reset and accumulation continues.

This spill cycle repeats as many times as needed. A 10 GB dataset with a
1 GB budget produces approximately 10 spill files.

Once all input is consumed, the sort node enters the merge phase. It
opens all spill files as `Vtr1File` readers and runs a k-way merge using
a min-heap. Each heap entry holds a reference to one merge run (one
spill file) and tracks the current row group and cursor position within
that run. The merge emits batches of up to 65,536 rows, reading row
groups from spill files on demand. Peak memory during the merge phase is
proportional to k (number of runs) times the row group size, not the
total dataset size.

If all data fits within the 1 GB budget, no spill occurs. The node sorts
in memory and emits the result directly. This is the common case for
datasets under ~100 million rows (depending on column count and types).

The sort-based `group_by() |> summarise()` path (used internally when
the engine detects it is advantageous) also benefits from this spill
mechanism. Temporary spill files are deleted when the sort node is
freed.

### Join memory model

Joins use a **build-right, probe-left** hash join:

1.  **Build phase**: The entire right-side table is materialized into a
    hash table in memory. Key columns are hashed using FNV-1a. For
    single-key joins, the hash is computed directly on the key bytes.
    For multi-key joins, per-key hashes are combined via XOR and FNV
    prime multiplication. The hash table uses open addressing. Key
    hashing is OpenMP-parallelized for batches above 32,768 rows.
2.  **Probe phase**: Left-side batches stream through one at a time,
    probing the hash table. For each left row, the engine computes the
    key hash, looks up the hash table, and verifies key equality (hash
    collisions are resolved by comparison). Probe-side hashing is also
    parallelized.
3.  **Finalize phase** (full_join only): Unmatched right-side rows are
    emitted in chunks of 65,536 rows. A matched-flag array tracks which
    right-side rows participated in the join.

The memory cost of a join is proportional to the right-side table size
(data arrays plus hash table overhead). The left side streams and does
not accumulate. For this reason, place the smaller table on the right
side of the join. The `right_join` verb handles this automatically by
swapping sides internally and remapping columns in the output.

## The .vtr file format

The `.vtr` format is vectra’s native binary columnar format. It is
designed for fast sequential reads with row-group-level granularity.

### Layout

    Header:
      magic bytes ("VTR1")
      version (uint16: 1–4)
      n_cols, n_rowgroups
      per-column: name + type byte [+ annotation string in v2+]
      row group index (byte offsets)

    Row groups (repeated):
      per-column:
        validity bitmap (bit-packed)
        [v4] encoding tag (1 byte) + compression tag (1 byte)
        [v4] uncompressed_size (uint32)
        typed data array (int64/double/bool/string)
      [v3+] per-column statistics (min/max)

### Version history

- **Version 1**: Base format with typed columns and validity bitmaps.

- **Version 2**: Adds per-column annotation strings for Date, POSIXct,
  and factor roundtripping.

- **Version 3**: Adds per-column per-rowgroup statistics (min/max)
  enabling zone-map predicate pushdown.

- **Version 4** (current): Adds a two-layer encoding and compression
  stack. Writing always produces v4. All versions (v1–v4) are readable.

### Encoding and compression (v4)

v4 applies two transformations to each column chunk (one column in one
row group), in order: an **encoding** step and a **compression** step.

Encoding transforms data logically to expose redundancy. The encoder
picks the best encoding per column per row group automatically:

| Encoding | Applies to | Condition | Mechanism |
|:---|:---|:---|:---|
| PLAIN | all types | default | Raw bytes, no transformation |
| DICTIONARY | string | unique ratio \< 50% | Builds a string dictionary; stores indices as RLE (run-length encoded) |
| DELTA | int64 | monotonically increasing | Stores first value + deltas (all \>= 0) |

DICTIONARY encoding is the most impactful for typical categorical string
columns. The encoder counts distinct values in a single pass using an
open-addressing hash table (70% load factor, dynamic resizing). If fewer
than half the values are unique, it emits a dictionary (offset array +
packed strings) followed by RLE-encoded indices. The RLE step collapses
runs of repeated indices into (value, count) pairs, which is effective
when rows with the same category are clustered (e.g. after a sort). If
more than half the values are unique, the encoder aborts dictionary
encoding and falls back to PLAIN with zero overhead.

DELTA encoding stores an initial value followed by the difference
between consecutive values. It targets auto-increment IDs, timestamps,
and other monotonic integer sequences where the deltas are small and
compress well.

Compression squeezes bytes physically after encoding. vectra uses a
built-in LZ77 compressor (LZ-VTR), approximately 120 lines of C with no
external dependencies. The compressor uses a 3-byte minimum match,
256-byte maximum offset, and a hash table for match finding. It skips
column chunks smaller than 64 bytes (not worth the overhead). If
compression does not reduce size, the chunk is stored uncompressed.
There is no configuration knob; the format always attempts compression
on eligible chunks.

The two-layer design is intentional. Encoding and compression solve
different problems. DICTIONARY and DELTA reduce the entropy of the data
(fewer distinct byte patterns, smaller integer ranges). LZ-VTR then
exploits the reduced entropy at the byte level. Applying both layers
yields better ratios than either layer alone, particularly for
RLE-encoded dictionary indices where long runs of identical small
integers compress to near zero.

## Query optimizer

[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
runs the optimizer before printing so you see the actual execution plan.
Two optimization passes run automatically:

### Predicate pushdown

When a `FilterNode` sits above a `ScanNode` reading a `.vtr` file (v3+),
the filter predicate is attached to the scan. The scan then applies up
to three pruning strategies on its first `next_batch()` call, in
priority order:

1.  **Hash index pushdown** (highest priority). If a `.vtri` sidecar
    index exists for the predicate column(s), the scan probes the index
    to build a row-group bitmap. Row groups not in the bitmap are
    skipped entirely. This handles `==` and `%in%` predicates. For
    composite indexes, AND-combined equality predicates on the indexed
    columns are matched and probed as a single composite key. See the
    Hash indexes section below for details.

2.  **Binary search on sorted columns**. If the `.vtr` file records that
    a column is sorted and the predicate is a simple comparison (`==`,
    `<`, `<=`, `>`, `>=`) against a literal, the scan binary-searches
    the row group stats to find the first and last row groups that could
    contain matching rows. The scan range is narrowed to
    `[first_rg, last_rg)`. For AND-combined predicates on the same
    sorted column (e.g. `x >= 10 & x < 100`), both bounds are applied.
    For OR-combined predicates, the union of both ranges is used.

3.  **Zone-map pruning** (applied per row group during iteration). Each
    row group in a v3+ file stores per-column min/max statistics. Before
    reading a row group’s data from disk, the scan evaluates the
    pushed-down predicate against the row group’s stats. If the
    predicate is provably false for the entire row group
    (e.g. `filter(x > 100)` on a row group where max(x) = 50), that row
    group is skipped entirely without touching the underlying bytes.
    Zone-map pruning handles comparison operators on numeric and string
    columns, AND/OR combinations, and `%in%` predicates (checking
    whether any set value falls within the row group’s min/max range).
    String zone maps use a packed 8-byte prefix representation for
    efficient comparison.

These three strategies compose. The scan first applies hash index and
binary search to narrow the set of candidate row groups, then checks
zone-map stats on each candidate before reading data. In
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
output, predicate pushdown appears as `predicate pushdown` and
`v3 stats` annotations on the ScanNode.

### Column pruning

The optimizer walks the plan tree top-down and determines which columns
each node actually needs from its child. The required column set at each
node is the union of: columns referenced in the node’s own expressions
(filter predicates, mutate expressions, aggregation functions), columns
passed through to the parent, and join key columns. At scan nodes,
unneeded columns are excluded from disk reads by setting a column mask.
For a 100-column `.vtr` file where only 3 columns are needed, this means
97 columns are never deserialized, never decompressed, and never
decoded. This is visible in
[`explain()`](https://gillescolling.com/vectra/reference/explain.md) as
`3/100 cols (pruned)`. Column pruning applies to all `.vtr` scans
regardless of format version.

### Hidden mutate insertion

When
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
contains nested expressions like `mean(x + y)`, the optimizer
auto-inserts a `ProjectNode` (visible as `hidden mutate` in
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)) to
compute the intermediate expression `x + y` before aggregation. This is
necessary because the C-level aggregation nodes (GroupAgg) operate on
single columns, not on expression trees. The hidden mutate computes the
intermediate column and gives it a generated name, which the aggregation
node then references. The same mechanism applies to `sum(log(x))`,
`n_distinct(a + b)`, and similar patterns. The inserted node is visible
in [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
output but invisible to the user’s pipeline.

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

## Hash indexes (.vtri)

vectra supports persistent on-disk hash indexes stored as `.vtri`
sidecar files alongside `.vtr` data files. An index names the row groups
that may hold a key, so an equality predicate reads those groups and
skips the rest.

### Creating indexes

Both functions take the path of the `.vtr` file:

``` r

# Single-column index
create_index("data.vtr", "species")

# Case-insensitive index
create_index("data.vtr", "species", ci = TRUE)

# Composite (multi-column) index
create_index("data.vtr", c("country", "year"))

# Check whether a usable index exists
has_index("data.vtr", "species")
```

[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
reads the indexed column(s) of the `.vtr` file one row group at a time
and writes a `.vtri` file holding one entry per distinct key per row
group. The file name encodes the indexed columns: `data.species.vtri`
for a single-column index, `data.country_year.vtri` for a composite.
Composite columns are canonicalized to schema order, so they may be
named in any order.

### Index format

The `.vtri` format is a sorted array of (key hash, row group) entries.
One layout covers both cases: a header with the indexed column indices,
a case-insensitive flag, and the row and row-group counts of the store
at build time, followed by the entries in ascending hash order and a
directory giving the first entry for each range of leading hash bits.

Sorting the entries rather than chaining them is what lets an index be
built in one forward pass through a fixed memory budget, since a chained
table has to know every entry’s bucket before it can write the first
one. It also drops the per-entry chain pointer and the bucket array: 12
bytes an entry rather than about 36.

Hashing uses FNV-1a on the raw column bytes; a case-insensitive index
folds ASCII uppercase to lowercase before hashing. A composite key
combines the per-column FNV-1a hashes by XOR and multiplication with the
FNV prime, giving one 64-bit hash per entry.

The stored row and row-group counts are what make an index safe to leave
on disk. `vtri_open()` compares them with the store being queried and
reports no index when they disagree, so an index that no longer
describes its store is ignored rather than used to prune row groups that
have moved. Formats written before 0.11.8 lack the counts and read as
absent;
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
rebuilds them.

### How the scan uses indexes

On its first batch, a `ScanNode` looks at its pushed-down predicate,
picks the column an equality or `%in%` clause offers, and opens that
column’s `.vtri` if there is one. For composite indexes it collects the
AND-combined equality clauses and opens the sidecar covering exactly
those columns. Opening is deferred to this point, so a query reads an
index only for the column it filters on.

On match, the scan probes the index to produce a row-group bitmap. For
`%in%` predicates, the scan probes once per set element and ORs the
bitmaps together. Row groups with a 0 bit in the bitmap are never read
from disk. This is the first pruning step, applied before binary search
and zone-map checks.

### Performance characteristics

A probe is a hash computation plus one chain walk, repeated per query
key (once for `==`, n times for `%in%`). Opening the index reads the
whole sidecar into memory once per
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md), and that
read is what the entry-per-distinct-key layout keeps small: an index
over a column of few distinct values stays the same size as the store
grows, so the cost of a lookup does not follow the size of the store.
For tables with many row groups and selective equality predicates, index
pushdown can reduce I/O by orders of magnitude compared to zone-map
pruning alone.

## Materialized blocks

[`materialize()`](https://gillescolling.com/vectra/reference/materialize.md)
consumes a vectra node and stores the result as a persistent in-memory
columnar block. Unlike nodes, which are consumed on
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) and
cannot be reused, blocks persist and support repeated lookups.

``` r

blk <- tbl("backbone.vtr") |>
  select(taxonID, canonicalName, genus) |>
  materialize()
```

### Exact lookups

[`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
performs hash-based lookups on a string column. Hash indices are built
lazily on first use and cached for subsequent calls. The return value is
a data.frame with a `query_idx` column (1-based position in the input
keys vector) plus all columns from the block.

``` r

hits <- block_lookup(blk, "canonicalName",
                     c("Quercus robur", "Pinus sylvestris"))

# Case-insensitive
hits <- block_lookup(blk, "canonicalName",
                     c("quercus robur"), ci = TRUE)
```

### Fuzzy lookups

[`block_fuzzy_lookup()`](https://gillescolling.com/vectra/reference/block_fuzzy_lookup.md)
computes string distances between query keys and a block column. Three
distance methods are available: Damerau-Levenshtein (`"dl"`, default),
Levenshtein (`"levenshtein"`), and Jaro-Winkler (`"jw"`). Results are
filtered by a maximum normalized distance threshold (default 0.2).

``` r

fuzzy <- block_fuzzy_lookup(blk, "canonicalName",
                            c("Quercus robar", "Pinus silvestris"),
                            method = "dl", max_dist = 0.2)
```

An optional blocking column reduces the search space by requiring exact
matches on a second column before computing distances. This is useful
for taxonomic lookups where genus is known:

``` r

fuzzy <- block_fuzzy_lookup(blk, "canonicalName",
                            c("Quercus robar"),
                            block_col = "genus",
                            block_keys = c("Quercus"),
                            n_threads = 4L)
```

Fuzzy lookups are OpenMP-parallelized. The `n_threads` parameter
controls the thread count (default 4).

### Use case

Materialized blocks are designed for repeated lookups against a
reference table. The typical pattern is: load a backbone or codelist
once with
[`materialize()`](https://gillescolling.com/vectra/reference/materialize.md),
then probe it many times with different query vectors. The block stays
in memory across calls, and its internal hash index (for exact lookups)
is built once and reused.

## OpenMP parallelization

The C engine uses OpenMP for CPU-bound operations where the per-element
cost is high enough to justify thread management overhead. Parallelism
is conditional: operations fall back to single-threaded execution when
the batch size is below a threshold or when the platform does not
support OpenMP.

### Which operations parallelize

| Operation | Threshold | Schedule | Notes |
|:---|:---|:---|:---|
| [`filter()`](https://gillescolling.com/vectra/reference/filter.md) (selection vector build) | 32,768 rows | parallel prefix sum | Two-phase: count matches per thread, then write at offsets |
| [`grepl()`](https://rdrr.io/r/base/grep.html) with regex | 1,000 rows | `dynamic, 64` | Per-thread regex compilation for thread safety |
| `levenshtein()`, `dl_dist()`, `jaro_winkler()` | 1,000 rows | `dynamic, 64` | Fuzzy string distance in mutate expressions |
| Sort (merge sort) | 32,768 rows | `task` | Recursive task spawning for parallel merge sort |
| Sort (key extraction) | 32,768 rows | `static` | Parallel extraction of sort keys into index arrays |
| Join (build-side hashing) | 32,768 rows | `static` | Parallel hash computation for build-side keys |
| Join (probe-side hashing) | 32,768 rows | `static` | Parallel hash computation for probe-side keys |
| Window (data copy) | 32,768 rows | `static` | Parallel copy of partition data |
| Window (group dispatch) | 64 groups | `dynamic` | Parallel window computation across groups |
| Collect (column append) | 8 columns | `static` | Parallel column-by-column append to R vectors |
| Zone-map stat computation | 32,768 rows | `static` with `reduction` | Parallel min/max scan during write |
| Block fuzzy lookup | always (if \> 0 keys) | `dynamic, 1` per key / `dynamic, 16` per row | Parallelized by query key and by block row |
| Literal fill (broadcast) | 32,768 rows | `static` | Parallel fill for constant columns |

The general-purpose threshold is 32,768 rows (`VEC_OMP_THRESHOLD`).
String operations use a lower threshold of 1,000 rows because their
per-element cost (regex compilation, edit distance matrices) is much
higher than arithmetic.

### Thread safety

Regex operations (`grepl` with regex patterns) compile the POSIX regex
once per thread. Each thread owns its own `regex_t` instance, allocated
in thread-local scope inside the `#pragma omp parallel` block. This
avoids both contention and the overhead of recompiling the regex per
row.

The filter node uses a parallel prefix sum to build the selection vector
without locking. Each thread counts matches in its chunk, a sequential
scan computes prefix offsets, then each thread writes selected indices
at its computed offset.

## Current limitations

- **slice_tail materializes**: There is no reverse-scan optimization.

- **distinct with .keep_all**: Falls back to R when `.keep_all = TRUE`
  with a column subset.

- **Predicate pushdown is .vtr only**: CSV, SQLite, and TIFF scans do
  not benefit from predicate pushdown, column pruning, or hash index
  acceleration.

- **No SIMD**: Arithmetic and comparison operations use scalar loops.
  The compiler may auto-vectorize some patterns, but there are no
  explicit SIMD intrinsics.

- **OpenMP availability varies**: On macOS, R ships without OpenMP by
  default. Users must install `libomp` (e.g. via Homebrew) and configure
  the compiler flags. On Windows (rtools) and Linux, OpenMP is typically
  available out of the box. When OpenMP is not available, all operations
  run single-threaded with no functional difference.

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
