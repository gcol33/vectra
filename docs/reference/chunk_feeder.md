# Turn a query into a resettable chunk generator

Wraps a query so a pull-based consumer can read it one chunk at a time
and re-read it from the start as many times as needed. The returned
closure follows the `data(reset)` protocol that
[`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) expects:
called with `reset = TRUE` it rewinds to the beginning of the data, and
called with `reset = FALSE` it returns the next chunk as a data.frame,
or `NULL` once the data is exhausted. This lets `bigglm()` fit a
generalized linear model on a dataset larger than RAM, streaming each
iteratively reweighted pass through the engine without ever holding the
full design matrix.

## Usage

``` r
chunk_feeder(.source)
```

## Arguments

- .source:

  Either a function of no arguments returning a fresh `vectra_node` each
  time it is called (e.g.
  `function() tbl_csv("occ.csv") |> select(presence, bio1, bio12)`), or
  an offloaded node from
  [`offload()`](https://gillescolling.com/vectra/reference/offload.md).
  Every chunk must contain all variables the consumer's formula
  references.

## Value

A function `function(reset = FALSE)`. With `reset = TRUE` it rewinds and
returns `invisible(NULL)`; with `reset = FALSE` it returns the next
chunk as a data.frame, or `NULL` at end of stream.

## Details

Because a vectra node is consumed as it streams, re-reading requires a
fresh node on each pass. `chunk_feeder()` accepts either form: a
*factory*, a function of no arguments that returns a new node each time
it is called; or an offloaded node from
[`offload()`](https://gillescolling.com/vectra/reference/offload.md),
which is backed by a file and replays from disk directly. On every
`reset = TRUE` a fresh stream is started, so the same query is replayed
on each pass.

Prefer feeding an
[`offload()`](https://gillescolling.com/vectra/reference/offload.md) of
the prepared query: the pipeline (scan, joins, mutate) runs once into
the spill, and every reweighted pass is then a disk scan of the prepared
columns rather than a re-run of the pipeline.

## See also

[`offload()`](https://gillescolling.com/vectra/reference/offload.md) for
the replay cache, and
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
for single-pass reductions that vectra drives.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

feed <- chunk_feeder(function() tbl(f) |> select(mpg, wt, hp))
feed(reset = TRUE)       # rewind to the start of the stream
first <- feed()          # first chunk as a data.frame
head(first)

# \donttest{
# Out-of-core GLM: prepare once with offload(), then bigglm() replays it.
if (requireNamespace("biglm", quietly = TRUE)) {
  s <- offload(tbl(f) |> select(mpg, wt, hp))
  fit <- biglm::bigglm(mpg ~ wt + hp, data = chunk_feeder(s),
                       family = gaussian())
  coef(fit)
}
# }
unlink(f)
```
