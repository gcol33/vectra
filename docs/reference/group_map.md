# Apply a function to each shard of a partition

Run a function once per shard of a partition (`offload(x, by = ...)`)
and gather the results. Each shard is read into memory as a data.frame
and passed to `.f` together with its key, so a model that couples rows
within a group becomes a set of independent per-shard fits. This is the
per-group counterpart to
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md),
which instead merges every shard into a single accumulator.

## Usage

``` r
group_map(.data, .f, ...)

# S3 method for class 'vectra_partition'
group_map(.data, .f, ...)

group_modify(.data, .f, ...)

# S3 method for class 'vectra_partition'
group_modify(.data, .f, ...)
```

## Arguments

- .data:

  A `vectra_partition` from
  [`offload()`](https://gillescolling.com/vectra/reference/offload.md)
  with a `by` key.

- .f:

  A function applied to each shard. It receives the shard as a
  data.frame and the shard key (a string) as its first two arguments;
  any further arguments in `...` follow. A purrr-style formula such as
  `~ lm(y ~ x, .x)` also works, with `.x` the shard data and `.y` the
  key. For `group_modify()`, `.f` must return a data.frame.

- ...:

  Additional arguments passed on to `.f`.

## Value

`group_map()` returns a named list with one element per shard.
`group_modify()` returns a single data.frame: the per-shard results
row-bound, with the shard key restored as a column when `.f` dropped it.

## Details

`group_map()` returns a named list, one element per shard keyed by the
shard key, and places no constraint on what `.f` returns. Use it for
per-group results that do not rebind into a table, such as fitted
models.

`group_modify()` expects `.f` to return a data.frame for each shard and
binds those frames into one. When a shard's result does not already
carry the partition key column, the key is added as a leading column
(named after the partition's `by`), so every row records the shard it
came from. Use it for per-group summaries that recombine into a single
table.

Each shard is materialized in full before `.f` sees it, so partition the
query on a key whose groups fit in memory. For a reduction that stays
bounded without ever holding a whole group, fold the partition with
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
instead.

## See also

[`offload()`](https://gillescolling.com/vectra/reference/offload.md) to
build a partition, and
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
for the partitioned monoidal reduce.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
p <- offload(tbl(f), by = "cyl")

# One fit per shard, returned as a named list keyed by cyl.
fits <- group_map(p, function(d, cyl) coef(lm(mpg ~ wt, data = d)))
fits

# Per-shard summaries recombined into one table, key restored as a column.
group_modify(p, function(d, cyl)
  data.frame(n = nrow(d), mean_mpg = mean(d$mpg)))
unlink(f)
```
