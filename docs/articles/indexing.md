# Indexing and Query Optimization

## Introduction

When a dataset has millions of rows spread across hundreds of row
groups, a naive scan reads every byte from disk even if the query only
touches a handful of rows. vectra’s optimizer exists to prevent that. It
applies several techniques automatically (zone-map pruning, predicate
pushdown, column pruning) and exposes one manual mechanism: hash
indexes.

The goal in every case is the same: skip work. Skip row groups whose
statistics prove they contain nothing relevant. Skip columns the query
never references. Skip disk reads entirely when a hash index can
identify the target row groups in constant time.

This vignette walks through each optimization, starting from the ones
that happen automatically and moving toward the ones that require
explicit setup. We will build a synthetic ecological dataset, write it
to `.vtr`, then run queries with and without indexes to see what
changes. The
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
function is our primary diagnostic tool throughout: it prints the query
plan and annotates which optimizations fired.

The engine applies these optimizations on its own, without intervention.
Knowing what the optimizer does (and what it cannot do) helps when
designing file layouts, choosing row group sizes, and deciding whether
an index is worth the disk space.

We will start by creating a dataset to work with.

``` r

library(vectra)
#> 
#> Attaching package: 'vectra'
#> The following object is masked from 'package:stats':
#> 
#>     filter
#> The following object is masked from 'package:graphics':
#> 
#>     grid

set.seed(42)
n <- 50000

sites <- paste0("site_", sprintf("%03d", 1:200))
species <- paste0("sp_", sprintf("%03d", 1:80))

eco <- data.frame(
  site     = sample(sites, n, replace = TRUE),
  year     = sample(2000:2023, n, replace = TRUE),
  species  = sample(species, n, replace = TRUE),
  value    = round(runif(n, 0, 100), 2),
  quality  = sample(c("good", "moderate", "poor"), n, replace = TRUE),
  stringsAsFactors = FALSE
)
```

We write this to a `.vtr` file with a relatively small row group size so
that pruning effects are visible in the plan output. In production,
larger row groups (100k to 500k rows) are typical; we use 5000 here so
that 50,000 rows split into 10 row groups.

``` r

f <- tempfile(fileext = ".vtr")
write_vtr(eco, f, batch_size = 5000)
```

## Zone-map pruning

Every row group in a v3+ `.vtr` file carries per-column min/max
statistics in its header. When a
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) sits
directly above a scan, the engine checks each row group’s statistics
against the predicate before reading any data. If a row group’s min/max
range for the filtered column does not overlap the predicate’s range,
the entire row group is skipped.

The min/max statistics are computed at write time. When
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
flushes each row group, it scans every column and records the smallest
and largest values in the row group header. For numeric columns (integer
and double), the comparison is straightforward. For string columns, the
engine stores the lexicographically smallest and largest strings, which
lets it answer range and equality predicates using byte-wise comparison.
Boolean columns store min/max too, but since the domain is just
TRUE/FALSE, pruning only helps when an entire row group is uniformly one
value.

Not all column types benefit equally. Numeric columns with natural
ordering produce the tightest zone maps: timestamps that increase over
time, sensor readings that cluster by location, sequential IDs. String
columns can also prune well if the values have a predictable
lexicographic distribution (e.g., ISO country codes or zero-padded
identifiers like `"site_042"`), but free-text strings with arbitrary
prefixes tend to span the full alphabet in every row group, making the
min/max bounds too wide to exclude anything.

This works for numeric comparisons: `>`, `<`, `>=`, `<=`, `==`, `!=`,
and combinations that define a range. It also works for string columns
where lexicographic ordering makes the min/max bounds meaningful.

``` r

tbl(f) |>
  filter(value > 95) |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The plan output shows `v4 stats` on the ScanNode, indicating that
zone-map statistics are available and the engine will use them. With our
uniform random data, roughly 5% of rows have `value > 95`. Some row
groups will have a max below 95 and be skipped entirely; others will be
read and filtered conventionally.

The effectiveness of zone-map pruning depends on two things: the
distribution of data within row groups, and the size of those row
groups. If data arrives sorted on the filter column, each row group
covers a narrow range and most can be eliminated. If data is randomly
shuffled, every row group’s range spans nearly the full domain, and zone
maps provide little benefit.

``` r

eco_sorted <- eco[order(eco$value), ]
f_sorted <- tempfile(fileext = ".vtr")
write_vtr(eco_sorted, f_sorted, batch_size = 5000)
```

When the data is sorted by `value`, each row group covers a contiguous
slice of the value range. A filter for `value > 95` now only needs the
last row group or two.

``` r

tbl(f_sorted) |>
  filter(value > 95) |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

Row group size is a trade-off. Smaller row groups give finer-grained
pruning boundaries but increase the number of zone-map entries in the
header and the number of read operations. Each row group carries its own
set of column min/max values, validity bitmaps, and byte offsets, so
splitting the same data into 1000 groups instead of 10 multiplies the
metadata by 100x. On the read side, each row group that survives pruning
triggers a separate seek and read. Many small reads can be slower than
fewer large reads, especially on spinning disks or network-mounted
storage where seek latency dominates.

Larger row groups amortize per-group overhead but make pruning coarser.
A single row group of 500k rows either gets read or skipped as a whole;
if the predicate matches even one row in that group, the engine reads
all 500k and filters in memory. For most workloads, 100k to 500k rows
per group is a reasonable default. If queries frequently filter on a
column whose values cluster spatially (timestamps, sorted IDs,
geographic coordinates), smaller groups around 10k to 50k can pay off
because each group covers a narrow value range and most groups fall
cleanly outside the predicate.

The relationship between sorting and row group size is multiplicative.
Sorted data with small row groups is the best-case scenario for zone
maps: each group covers a contiguous, non-overlapping slice of the value
domain, and the engine can binary-search the row group index to find the
relevant range. Unsorted data with small row groups is the worst case:
every group spans nearly the full domain, none get pruned, and we pay
the overhead of many small reads with no pruning benefit.

Zone-map pruning is fully automatic. There is nothing to configure and
no sidecar file to maintain. It composes with every other optimization
described below.

## Hash indexes

Zone maps work well for range predicates on columns with spatial
locality, but they do nothing for equality lookups on high-cardinality
string columns. If we filter for a specific site name, the engine must
check every row group’s string min/max bounds, which often span the
entire alphabet. This is where hash indexes help.

A hash index is a `.vtri` sidecar file that maps column values to the
row groups that contain them. Internally the file is a serialized
chained hash table using FNV-1a as the hash function: a bucket array, an
entry array, and a next-index per entry linking the entries that share a
bucket. Each entry pairs one distinct key hash with one row group
holding it, so a key present in six row groups takes six entries, and a
key repeated a million times inside one row group takes one. The bucket
count is the next power of two above twice the entry count, which holds
the load factor at or under 50% and keeps the chains short. At query
time, “probing the index” means hashing the filter value, walking the
chain for its bucket, and setting a bit for each row group the matching
entries name. That bitmap is what the scan then uses to skip groups. The
cost of a probe tracks the number of row groups holding the key; the
number of rows in the file does not enter it.

Because an entry covers a distinct key within a row group, the size of
an index follows the number of distinct keys. A column of 200 site names
indexes to the same file size whether the store holds a hundred thousand
rows or a hundred million.

This design favors high-cardinality columns. When a column has thousands
of distinct values spread across hundreds of row groups, each value maps
to a small subset of groups, and the index eliminates most of the file
from the scan. A column with only 5 distinct values (like `quality` in
our dataset) maps each value to nearly every row group, so the bitmap is
almost fully set and the index saves little work. Zone maps handle that
case adequately on their own.

Once built, the scan node can identify the relevant row groups in
constant time, skipping all others without reading their data.

``` r

tbl(f) |>
  filter(site == "site_042") |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

Without an index, the plan shows a FilterNode above a ScanNode with
predicate pushdown and zone-map stats, but no index annotation. The
engine reads every row group and filters in-memory.

Now we create an index on the `site` column.

``` r

create_index(f, "site")
has_index(f, "site")
#> [1] TRUE
```

``` r

tbl(f) |>
  filter(site == "site_042") |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats, hash index (site)] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The plan now shows `hash index` (or equivalent annotation) on the
ScanNode. Instead of scanning all 10 row groups, the engine probes the
index to find which row groups contain `"site_042"` and reads only
those. With 200 sites spread across 10 row groups, most row groups will
contain at least one occurrence, but in real data where sites cluster
geographically or temporally, the savings can be dramatic.

We can see the difference in timing on a moderately sized dataset. The
effect is most pronounced when the predicate is highly selective (few
row groups match) and the file has many row groups.

``` r

# Without index -- drop the existing one first
unlink(paste0(f, ".site.vtri"))

t_no_idx <- system.time({
  for (i in 1:50) {
    tbl(f) |> filter(site == "site_042") |> collect()
  }
})

# With index
create_index(f, "site")

t_idx <- system.time({
  for (i in 1:50) {
    tbl(f) |> filter(site == "site_042") |> collect()
  }
})

cat("Without index:", t_no_idx["elapsed"], "s\n")
#> Without index: 0.13 s
cat("With index:   ", t_idx["elapsed"], "s\n")
#> With index:    0.11 s
```

The magnitude of the speedup depends on how many row groups the index
can eliminate. On this small dataset with only 10 row groups and
uniformly distributed site names, the difference may be modest. On a
file with 1000 row groups where the target site appears in only 3 of
them, the index turns a full scan into three targeted reads.

The index file sits alongside the data file with the naming convention
`<original>.column.vtri`. The scan opens it for the column its predicate
filters on, so a store can carry an index on each of several columns and
a query pays only for the one it uses.

## Composite indexes

When queries filter on multiple columns simultaneously, a composite
index can prune more aggressively than individual single-column indexes.
A composite index hashes the combined values of two or more columns
together using FNV-1a hash combining, mapping compound keys to row
groups.

``` r

create_index(f, c("site", "year"))
has_index(f, c("site", "year"))
#> [1] TRUE
```

This index accelerates AND-combined equality predicates on both columns
at once.

``` r

tbl(f) |>
  filter(site == "site_042", year == 2015) |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats, hash index (site + year)] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The engine detects that both predicates in the AND-combination match the
composite index columns and probes the index with the combined hash.
This is more selective than probing two single-column indexes
independently, because the composite index encodes the co-occurrence of
values within row groups.

When should we prefer a composite index over two single-column indexes?
The composite wins when the query always filters on both columns
together. If sometimes we filter on `site` alone and sometimes on `site`
and `year` together, we need a single-column index on `site` in addition
to the composite. A composite index on `(site, year)` does not
accelerate a filter on just `site` or just `year`.

A composite index uses the same `.vtri` layout as a single-column one,
with several column indices in its header. The naming convention encodes
both columns: `<file>.site_year.vtri` (or similar). The columns may be
named to
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
in any order, since the file name and the compound hash are both built
in schema order. The scan node loads a composite index when the
predicate structure matches.

The FNV-1a hash combining means that `(site = "A", year = 2020)` and
`(site = "B", year = 2020)` produce different hashes even though they
share the year value. There is no prefix-matching behavior as in B-tree
indexes. The composite index is purely an equality lookup on the full
compound key.

## Case-insensitive indexes

Ecological datasets often contain species names, location names, or
observer codes with inconsistent casing. `"Quercus robur"`,
`"quercus robur"`, and `"QUERCUS ROBUR"` might all appear in the same
column. A standard hash index treats these as distinct values, so a
filter for one spelling misses rows stored under a different case.

The `ci = TRUE` option builds a case-insensitive index that normalizes
all values to lowercase before hashing.

``` r

# Create a dataset with mixed-case species
eco_mixed <- eco
eco_mixed$species[1:100] <- toupper(eco_mixed$species[1:100])
f_mixed <- tempfile(fileext = ".vtr")
write_vtr(eco_mixed, f_mixed, batch_size = 5000)

create_index(f_mixed, "species", ci = TRUE)
```

With this index in place, a filter for `"sp_001"` will match rows
regardless of whether the stored value is `"sp_001"`, `"SP_001"`, or
`"Sp_001"`. The case normalization happens at index creation time, so
there is no runtime cost during queries beyond the normal hash probe.

Case-insensitive indexes are useful whenever the filter values might not
match the stored casing exactly. Reference tables, user-entered location
names, and taxonomic databases with inconsistent capitalization all
benefit from this option.

## `%in%` acceleration

Hash indexes accelerate single-value equality predicates and also `%in%`
predicates. When we filter a column against a set of values, the engine
probes the index for each value in the set and takes the union of
matching row groups.

``` r

target_sites <- c("site_010", "site_042", "site_100", "site_150")

tbl(f) |>
  filter(site %in% target_sites) |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats, hash index (site)] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The scan node probes the site index four times and builds a bitmap of
row groups that contain at least one of the target values. Only those
row groups are read from disk.

This matters most when the set is small relative to the number of
distinct values. Filtering 4 sites out of 200 in a file with 1000 row
groups might reduce the scan to 20 row groups. Filtering 190 out of 200
sites offers less benefit since most row groups will match anyway.

The `%in%` acceleration works with both single-column and composite
indexes. For composite indexes, each element of the filter set must be a
complete compound key. In practice, `%in%` on composite indexes is less
common; the typical use case is `%in%` on a single high-cardinality
column like species, site, or sample ID.

``` r

t_in_no_idx <- system.time({
  for (i in 1:50) {
    tbl(f) |>
      filter(site %in% c("site_010", "site_042")) |>
      collect()
  }
})

cat("With index, %in% filter:", t_in_no_idx["elapsed"], "s\n")
#> With index, %in% filter: 0.13 s
```

Without an index, the same query reads all row groups and filters in
memory. The difference grows with file size.

## Column pruning

Every query touches only a subset of the columns in a file. vectra’s
optimizer walks the plan tree top-down, collects the set of columns each
node references, and propagates that information to the scan node.
Columns that no node in the tree needs are excluded from disk reads
entirely.

``` r

tbl(f) |>
  filter(value > 90) |>
  select(site, value) |>
  explain()
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   FilterNode [streaming] 
#>     ScanNode [streaming, 2/5 cols (pruned), predicate pushdown, tdc stats] 
#> 
#> Output columns (2):
#>   site <string>
#>   value <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The ScanNode annotation shows something like `2/5 cols (pruned)` or
`3/5 cols (pruned)`, depending on how many columns the filter and select
together require. The `year`, `species`, and `quality` columns are never
read from disk.

Column pruning is especially effective on wide tables. A dataset with 50
columns where a query touches 3 of them avoids reading 94% of the column
data. Since `.vtr` files store columns independently within each row
group, skipping a column means skipping its entire byte range on disk.

``` r

wide <- data.frame(
  id = seq_len(1000),
  matrix(rnorm(1000 * 20), ncol = 20,
         dimnames = list(NULL, paste0("v", 1:20)))
)
f_wide <- tempfile(fileext = ".vtr")
write_vtr(wide, f_wide)

tbl(f_wide) |>
  select(id, v1, v2) |>
  explain()
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   ScanNode [streaming, 3/21 cols (pruned), tdc stats] 
#> 
#> Output columns (3):
#>   id <int64>
#>   v1 <double>
#>   v2 <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The plan shows that only 3 of 21 columns are read. The remaining 18
columns never touch memory. This optimization is always active and
requires no user action.

## Predicate pushdown

When a
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) node
sits above a scan node, the engine pushes the predicate down into the
scan itself. This allows the scan to apply zone-map pruning and hash
index lookups before any data is materialized.

``` r

tbl(f) |>
  filter(value > 50) |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The plan annotates the ScanNode with `predicate pushdown`, indicating
that the filter condition has been absorbed into the scan. The separate
FilterNode still appears in the plan (it handles any remaining rows that
pass the zone-map check but fail the actual predicate), but the scan
does the heavy lifting of eliminating row groups.

Predicate pushdown works when the filter is directly above the scan or
when only “transparent” nodes (like select/rename) sit between them. If
a mutate or join intervenes, the predicate cannot be pushed past it
because the filter might reference columns that the intermediate node
creates.

``` r

tbl(f) |>
  mutate(scaled = value / 100) |>
  filter(scaled > 0.9) |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ProjectNode [streaming] 
#>     ScanNode [streaming, 5 cols, tdc stats] 
#> 
#> Output columns (6):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#>   scaled <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

Here the filter references `scaled`, which is created by the mutate. The
predicate stays on the FilterNode above the ProjectNode. The ScanNode
has no predicate pushdown annotation. To recover pushdown in this
situation, restructure the query so the filter on `value` happens before
the mutate:

``` r

tbl(f) |>
  filter(value > 90) |>
  mutate(scaled = value / 100) |>
  explain()
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   FilterNode [streaming] 
#>     ScanNode [streaming, 5 cols, predicate pushdown, tdc stats] 
#> 
#> Output columns (6):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#>   scaled <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

Now the filter on `value` pushes down to the scan, and the mutate
operates only on the surviving rows. Query structure matters.

## Reading `explain()` output

The [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
function prints the query plan as an indented tree. Each line shows a
node type, its execution mode, and any optimization annotations. Reading
this output is the fastest way to understand what the engine will do.

``` r

tbl(f) |>
  filter(site == "site_042", value > 50) |>
  select(site, year, value) |>
  mutate(decade = year - (year %% 10)) |>
  explain()
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   ProjectNode [streaming] 
#>     FilterNode [streaming] 
#>       ScanNode [streaming, 3/5 cols (pruned), predicate pushdown, tdc stats, hash index (site)] 
#> 
#> Output columns (4):
#>   site <string>
#>   year <int64>
#>   value <double>
#>   decade <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The tree reads bottom-up (scan at the bottom, output at the top). Each
node pulls batches from its child. The annotations on each line tell us:

**Node types:**

- `ScanNode`: reads from disk. Annotations include column count, pruning
  info, predicate pushdown, zone-map stats (`v4 stats`), hash index
  usage.

- `FilterNode`: applies a predicate row-by-row. When pushdown succeeds,
  this node still appears but does less work.

- `ProjectNode`: handles
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  and
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md).
  Always streaming.

- `SortNode`: materializes all input, sorts, then streams output.

- `GroupAggNode`: hash-based grouping and aggregation. Materializes
  groups.

- `JoinNode`: hash join. Materializes the build side (right table).

- `WindowNode`: window functions. May materialize partitions.

- `TopNNode`: combined sort + limit. More efficient than separate sort
  and limit for small N.

- `ConcatNode`: union of multiple inputs
  ([`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)).
  Streaming.

**Annotations:**

- `streaming`: the node processes one batch at a time without buffering
  the full input. Memory usage is bounded by the batch size.

- `materializing`: the node must consume all input before producing
  output. Sorts and some aggregations are materializing.

- `predicate pushdown`: filter condition was absorbed into the scan.

- `v4 stats`: zone-map statistics are available for row group pruning.

- `hash index`: a `.vtri` index is being used for row group
  identification.

- `N/M cols (pruned)`: only N of M columns in the file are actually
  read.

When diagnosing a slow query, start at the ScanNode and work upward. The
scan is where data enters the pipeline, so any inefficiency there
propagates through every downstream node.

If the ScanNode lacks a `predicate pushdown` annotation but the query
has a
[`filter()`](https://gillescolling.com/vectra/reference/filter.md), the
most common cause is an intermediate node sitting between the filter and
the scan. A
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) that
creates a column referenced by the filter blocks pushdown, because the
scan cannot evaluate an expression that depends on a column that does
not exist on disk. The fix is to rewrite the query so that filters on
raw columns happen before any mutate. Sometimes two filters are needed:
one on raw columns (pushed down) and one on computed columns (applied
later). This is a valid pattern and the engine handles it correctly.

Another common surprise: the plan shows `predicate pushdown` but the
query is still slow. This can happen when the predicate is not selective
enough to prune row groups. A filter like `value > 0` on a column where
every row group’s min is below 0 and max is above 0 will push down
successfully but prune nothing. The
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
output confirms that the machinery is in place; actual pruning depends
on the data distribution and row group boundaries.

If the ScanNode reads all columns, check whether adding a
[`select()`](https://gillescolling.com/vectra/reference/select.md)
before a heavy operation (join, sort, aggregation) could shrink the
column set. On wide tables this can cut I/O by an order of magnitude. If
the ScanNode lacks a hash index annotation for an equality filter,
consider creating one.

A useful diagnostic workflow: run
[`explain()`](https://gillescolling.com/vectra/reference/explain.md),
note the ScanNode annotations, make a structural change (reorder verbs,
add an index, sort the file), then run
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
again and compare. The annotations change immediately; there is no
caching or stale state to worry about.

``` r

tbl(f) |>
  filter(year >= 2020) |>
  group_by(site) |>
  summarise(avg = mean(value), n = n()) |>
  explain()
#> vectra execution plan
#> 
#> GroupAggNode [materializes, 1 keys] 
#>   SortNode [materializes] 
#>     FilterNode [streaming] 
#>       ScanNode [streaming, 3/5 cols (pruned), predicate pushdown, tdc stats] 
#> 
#> Output columns (3):
#>   site <string>
#>   avg <double>
#>   n <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

This plan shows a GroupAggNode above a FilterNode above a ScanNode. The
filter pushes down, zone maps may prune row groups where all years are
below 2020, and the aggregation hashes on `site`. Column pruning
eliminates `species` and `quality` since neither the filter nor the
aggregation references them.

## Materialized blocks

Sometimes we have a reference table that we need to look up against
repeatedly. A taxonomy backbone, a site metadata table, a species trait
database. Joining this table into every query works but re-reads the
reference data each time.
[`materialize()`](https://gillescolling.com/vectra/reference/materialize.md)
loads a query result into memory once and keeps it there as a
`vectra_block` for repeated O(1) lookups.

``` r

# Build a small reference table
ref <- data.frame(
  species = species,
  common   = paste("Common name for", species),
  family   = sample(paste0("fam_", 1:10), 80, replace = TRUE),
  stringsAsFactors = FALSE
)
f_ref <- tempfile(fileext = ".vtr")
write_vtr(ref, f_ref)

blk <- materialize(tbl(f_ref))
blk
#> vectra_block [materialized in memory]
```

The block builds an internal hash table lazily on first lookup.
Subsequent lookups on the same column are immediate.

``` r

hits <- block_lookup(blk, "species", c("sp_010", "sp_042", "sp_001"))
hits
#>   query_idx species                 common family
#> 1         1  sp_010 Common name for sp_010  fam_8
#> 2         2  sp_042 Common name for sp_042  fam_7
#> 3         3  sp_001 Common name for sp_001  fam_5
```

[`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
returns a data.frame with a `query_idx` column indicating which query
key each row matched, plus all columns from the block. This is
equivalent to a semi-join filtered to the query keys, but without
rebuilding a hash table each time.

For approximate matching,
[`block_fuzzy_lookup()`](https://gillescolling.com/vectra/reference/block_fuzzy_lookup.md)
computes string distances between query keys and the block column. This
is useful for reconciling species names across databases where spelling
varies.

``` r

# Deliberately misspelled queries
fuzzy_hits <- block_fuzzy_lookup(
  blk, "species",
  keys = c("sp_10", "sp_42"),
  method = "dl",
  max_dist = 0.3
)
fuzzy_hits
#>   query_idx fuzzy_dist species                 common family
#> 1         1  0.1666667  sp_010 Common name for sp_010  fam_8
#> 2         2  0.1666667  sp_042 Common name for sp_042  fam_7
```

The `method` argument accepts `"dl"` (Damerau-Levenshtein, the default),
`"levenshtein"`, or `"jw"` (Jaro-Winkler). The `max_dist` threshold
controls how permissive the matching is; lower values require closer
matches.

When the reference table has a blocking column (e.g., genus) that can
narrow the search space, use `block_col` and `block_keys` to restrict
comparisons to rows sharing the same block value. This turns an O(N \*
M) fuzzy search into O(N \* m) where m is the average block size.

``` r

# Add a genus column for blocking
ref2 <- ref
ref2$genus <- substr(ref2$species, 1, 5)
f_ref2 <- tempfile(fileext = ".vtr")
write_vtr(ref2, f_ref2)

blk2 <- materialize(tbl(f_ref2))

fuzzy_blocked <- block_fuzzy_lookup(
  blk2, "species",
  keys       = c("sp_10", "sp_42"),
  method     = "dl",
  max_dist   = 0.3,
  block_col  = "genus",
  block_keys = c("sp_01", "sp_04")
)
fuzzy_blocked
#>   query_idx fuzzy_dist species                 common family genus
#> 1         1  0.1666667  sp_010 Common name for sp_010  fam_8 sp_01
#> 2         2  0.1666667  sp_042 Common name for sp_042  fam_7 sp_04
```

A block differs from a hash index in a fundamental way: it holds the
entire table in RAM, not just a mapping from values to row group IDs.
This makes lookups faster (no disk I/O at all), but the memory cost is
the full uncompressed size of the table. A reference table with 10,000
rows and 5 columns is negligible. A table with 10 million rows might
consume hundreds of megabytes, and at that scale a streaming hash join
against a `.vtr` file is the better approach.

Blocks are also ephemeral. They live in the R session and are not
persisted to disk. When the session ends, the block and its internal
hash table disappear. A block is a cache layer; the `.vtr` file stays
the source of truth. If we need the reference table again in a later
session, we re-materialize from the `.vtr` file, which takes a fraction
of a second for small tables. There is no `.vtri`-style sidecar to
maintain or worry about going stale.

When a block is better than an index: the reference table is small
(under 100k rows), lookups happen many times in the same session (e.g.,
inside a loop or across multiple queries), and the lookup pattern is
exact-match or fuzzy-match on a key column. The block’s internal hash
table builds lazily on the first
[`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
call for a given column and stays resident for all subsequent calls on
that column. If the workflow involves a single lookup and then
discarding the result, the setup cost of materializing the block may not
be worth it compared to a simple join.

## Practical guidance

The optimizations described above interact in predictable ways. Here is
a decision framework for choosing which ones to invest in.

**When to create a hash index.** Indexes pay off when three conditions
hold: the column has high cardinality (hundreds or thousands of distinct
values), the filter is an equality predicate or `%in%` check, and the
query runs more than once. A column with 5 distinct values benefits
little from an index because zone maps already identify the relevant row
groups in most layouts. A column filtered with range predicates (`>`,
`<`, `between`) gets no benefit because the index only supports equality
lookups.

**When NOT to create an index.** Small files (under 100k rows) are fast
enough to scan entirely. Files that are always read in full (no
filtering) gain nothing. Columns used exclusively in range predicates
should rely on zone maps and sorted layouts instead.

One-off queries also don’t justify an index. Building the `.vtri` file
requires a full pass over the data, which is roughly as expensive as the
scan we’re trying to avoid. If the query runs once, we’ve spent the same
I/O budget twice (once to build, once to query) for no net gain. Indexes
amortize their creation cost over many queries. As a rough heuristic, if
the query will run fewer than 5 times, a full scan is likely cheaper in
total wall time.

Disk space is another consideration. A `.vtri` file stores 20 bytes per
(distinct key, row group) pair plus 8 bytes per bucket, and no key
values, only their hashes. A column of 100,000 distinct strings spread
over a few row groups each indexes to a few megabytes. A column whose
values are nearly all distinct puts one entry per row into the index,
which can rival the size of the data file. Zone maps and sorted-column
binary search cover that case without a sidecar.
[`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
reports whether a usable index exists; checking the file size directly
shows its cost.

**Row group sizing for zone maps.** Smaller row groups give zone maps
finer-grained pruning boundaries. If the data is sorted on the primary
filter column, 10k to 50k rows per group lets the engine skip precisely.
If the data is unsorted, small groups add overhead without improving
pruning. For unsorted data, 100k to 500k rows per group is a safer
default that balances scan overhead against I/O efficiency.

**Sort order matters.** If one column dominates filter predicates,
sorting the data on that column before writing dramatically improves
zone-map pruning. The engine can also apply binary search within row
groups on sorted columns. A common pattern is to sort ecological
observation data by timestamp or site ID, since most queries filter on
one of these.

``` r

eco_by_site <- eco[order(eco$site), ]
f_by_site <- tempfile(fileext = ".vtr")
write_vtr(eco_by_site, f_by_site, batch_size = 5000)

tbl(f_by_site) |>
  filter(site == "site_042") |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

When data is sorted by `site`, each site occupies a contiguous range of
row groups. Zone maps on string columns prune all row groups outside
that range, and an index can pinpoint the exact groups.

**Index maintenance.** A row append rewrites every row group, which
moves the rows a key sits in, so each of the store’s indexes is rebuilt
as part of `append_vtr(along = "rows")`. The rebuild is a single
sequential pass over the indexed column, hashing each value and
recording its row group membership; on a file of a few million rows it
takes well under a second, against the full restream the row append
itself performs.

``` r

append_vtr(eco[1:100, ], f)

# The index covers the appended rows too
has_index(f, "site")
#> [1] TRUE
tbl(f) |>
  filter(site == "site_042") |>
  explain()
#> vectra execution plan
#> 
#> FilterNode [streaming] 
#>   ScanNode [streaming, 5 cols, predicate pushdown, tdc stats, hash index (site)] 
#> 
#> Output columns (5):
#>   site <string>
#>   year <int64>
#>   species <string>
#>   value <double>
#>   quality <string>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

A column append (`along = "cols"`) leaves row group boundaries and
existing column data untouched, so an index over the original columns
stays valid and is not rebuilt.

An index records the row and row-group counts of the store it was built
against. A query that finds those changed reads the store and leaves the
index alone. An index invalidated some other way, such as overwriting
the file with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
therefore costs the acceleration and never the rows.
[`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
reports `FALSE` in that state, which is the signal to call
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
again:

``` r

g <- tempfile(fileext = ".vtr")
write_vtr(eco, g, batch_size = 5000)
create_index(g, "site")

write_vtr(eco[1:2000, ], g, batch_size = 5000)   # overwrite under the index
has_index(g, "site")                             # FALSE: rebuild needed
#> [1] FALSE
nrow(collect(filter(tbl(g), site == "site_042")))  # still correct
#> [1] 14
```

**Decision tree.** For a new `.vtr` file that will be queried
repeatedly:

1.  Choose a sort column based on the most common filter predicate. Sort
    the data before writing.
2.  Pick a row group size. Start with 100k rows. If queries filter on
    the sort column, try 10k to 50k and benchmark.
3.  If queries filter on equality predicates on a non-sort column,
    create a hash index on that column.
4.  If queries filter on two columns simultaneously (AND-combined
    equality), consider a composite index.
5.  Use
    [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
    after each change to verify the optimizer is picking up the new
    structure.

**Composing optimizations.** All optimizations compose. A single query
can benefit from column pruning (fewer columns read), zone-map pruning
(fewer row groups read), hash index pushdown (constant-time row group
identification), and predicate pushdown (filtering inside the scan). The
engine applies every applicable technique automatically. The only manual
step is creating indexes.

``` r

tbl(f) |>
  filter(site == "site_042", value > 80) |>
  select(site, year, value) |>
  explain()
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   FilterNode [streaming] 
#>     ScanNode [streaming, 3/5 cols (pruned), predicate pushdown, tdc stats, hash index (site)] 
#> 
#> Output columns (3):
#>   site <string>
#>   year <int64>
#>   value <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

This plan uses the hash index on `site` to identify candidate row
groups, zone maps on `value` to further prune within those groups, reads
only 3 of 5 columns, and pushes both predicates into the scan.
