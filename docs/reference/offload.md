# Spill a query to disk and stream it back (the offload functor)

Materializes a query once to disk and returns a stream that holds the
same rows, so every later pass is a disk scan instead of a re-run of the
upstream pipeline. The materialization streams batch by batch, so peak
memory stays at one batch regardless of result size. This is the bridge
from the bounded single-pass world of
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
to out-of-core fits.

## Usage

``` r
offload(
  x,
  by = NULL,
  n = NULL,
  method = c("auto", "level", "range", "hash"),
  path = NULL,
  compress = c("fast", "small", "none")
)
```

## Arguments

- x:

  A `vectra_node` to materialize.

- by:

  Optional name (string) of a partition key column. When supplied, the
  result is a partition rather than a single node.

- n:

  Number of buckets for `method = "range"` or `"hash"`. Ignored for a
  one-shard-per-value partition.

- method:

  Partition strategy: `"auto"` (default; one shard per value for a
  discrete key, `n` ranges for a numeric key), `"level"` (one shard per
  distinct value), `"range"` (`n` equal-width value ranges), or `"hash"`
  (`n` buckets by a stable hash of the key, co-locating each key).

- path:

  Optional file path for a durable replay-cache spill (used only when
  `by` is `NULL`). When `NULL` a temporary file is used and removed when
  the returned node is garbage-collected.

- compress:

  Compression for spill files, passed to
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md):
  `"fast"` (default), `"small"`, or `"none"`.

## Value

A `vectra_node` (no `by`) or a `vectra_partition` (with `by`), each
carrying a cost grade shown by
[`print()`](https://rdrr.io/r/base/print.html) and
[`explain()`](https://gillescolling.com/vectra/reference/explain.md).

## Details

With no `by`, `offload()` returns a **replay cache**: a `vectra_node`
backed by one `.vtr` file. Feed it to a pull-based consumer such as
[`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) through
[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md),
which accepts an offloaded node directly, so each iteratively reweighted
pass reads the prepared columns from disk rather than rebuilding them.
Bake the selects and mutates into the query you offload, and replay does
no further work.

With `by`, `offload()` returns a **partition**: the rows split into
disjoint shards, one per key value (discrete key) or per value range
(`method = "range"`, or any numeric key), written in a single streaming
pass. A partition prints as a list of shards and behaves like one:
[`length()`](https://rdrr.io/r/base/length.html),
[`names()`](https://rdrr.io/r/base/names.html) (the keys), `p[["key"]]`
(a shard node), and `lapply(p, ...)` all work. Fold it with
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
(supplying `combine`). The union of the shards reproduces the input; row
totals are checked.

## See also

[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
(accepts an offloaded node),
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
for the partitioned monoidal reduce, and
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md) for
the external-sort instance.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

# Replay cache: same rows, now on disk.
s <- offload(tbl(f) |> filter(cyl > 4) |> select(mpg, wt, hp))
nrow(collect(s))

# Partition by a key: a list of per-shard nodes.
p <- offload(tbl(f), by = "cyl")
names(p)
length(p)
nrow(collect(p[[1]]))
unlink(f)
```
