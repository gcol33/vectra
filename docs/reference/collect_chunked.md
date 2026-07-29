# Fold a function over a query, one batch at a time

Streams a lazy query through R in bounded pieces and reduces them with
`f`, instead of materializing the whole result the way
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
does. The engine pulls one batch (a data.frame of up to a few hundred
thousand rows) at a time; `f` is called as `f(acc, chunk)` and its
return value becomes the accumulator for the next batch. Peak memory is
one batch plus whatever the accumulator holds, so a result far larger
than RAM can be reduced to a small summary in a single pass.

## Usage

``` r
collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)

# Default S3 method
collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)

# S3 method for class 'vectra_node'
collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)

# S3 method for class 'vectra_offload'
collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)

# S3 method for class 'vectra_shard'
collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)

# S3 method for class 'vectra_partition'
collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  ... and any chain of verbs). It is consumed: after `collect_chunked()`
  returns, the stream is drained and `x` cannot be collected again.

- f:

  A function of two arguments `function(acc, chunk)` returning the
  updated accumulator. `chunk` is a data.frame holding the next batch of
  rows.

- .init:

  Initial accumulator value. Passed to `f` with the first batch and
  returned unchanged if the query yields no rows. When `combine` is
  supplied this is also the monoid identity (the value `combine` leaves
  unchanged).

- combine:

  Optional function `function(acc, acc)` that merges two accumulators.
  Supplying it declares the reduction a monoid with `.init` as identity,
  which is what lets the fold run over the independent shards of a
  partition (`offload(x, by = ...)`) and have the partial results merged
  correctly. For a plain node the stream is a single sequence, so
  `combine` is not needed and is ignored.

- commutative:

  Logical; declare that `combine` does not depend on the order of its
  arguments. Lets a partitioned fold merge shards in any order (no
  stable sort required). Default `FALSE`.

## Value

The final accumulator. For a node: `f` applied left-to-right across
every batch, seeded with `.init`. For a partition: each shard folded
with `f`/`.init`, then those per-shard accumulators merged with
`combine`.

## Details

This is the streaming counterpart to a fold
([`Reduce()`](https://rdrr.io/r/base/funprog.html)): use it when the
query returns more rows than fit in memory but the *reduction* is small.
A running count, per-group sufficient statistics, the cross-products
`X'X` and `X'y` behind a linear fit, an online mean or histogram - all
accumulate in bounded space across the stream. When you instead need the
model-fitting consumer to drive the iteration (and to re-read the data
on each pass, as an iteratively reweighted GLM does), use
[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md).

## See also

[`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
for pull-based consumers such as
[`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html),
[`offload()`](https://gillescolling.com/vectra/reference/offload.md) for
the replay cache and the partitioned monoidal reduce,
[`group_map()`](https://gillescolling.com/vectra/reference/group_map.md)
and
[`group_modify()`](https://gillescolling.com/vectra/reference/group_map.md)
for per-shard application, and
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) to
materialize the full result.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

# Row count without materializing the result.
collect_chunked(tbl(f), function(acc, chunk) acc + nrow(chunk), .init = 0L)

# Accumulate the normal-equation pieces X'X and X'y for an exact OLS fit
# of mpg ~ wt + hp, in one streaming pass.
acc <- collect_chunked(
  tbl(f) |> select(mpg, wt, hp),
  function(acc, chunk) {
    X <- cbind(1, chunk$wt, chunk$hp)
    y <- chunk$mpg
    list(XtX = acc$XtX + crossprod(X), Xty = acc$Xty + crossprod(X, y))
  },
  .init = list(XtX = matrix(0, 3, 3), Xty = matrix(0, 3, 1))
)
solve(acc$XtX, acc$Xty)            # same as coef(lm(mpg ~ wt + hp, mtcars))
unlink(f)
```
