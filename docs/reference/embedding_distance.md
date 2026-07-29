# Embedding distance functions

Distance and similarity functions over embedding columns (see
[`as_embedding()`](https://gillescolling.com/vectra/reference/as_embedding.md)),
usable inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) /
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) like
any other expression. The query side is either another embedding column
or a constant numeric vector. Decoding and the arithmetic run in C, one
row at a time, parallelized over rows.

## Usage

``` r
cosine(x, y)

l2(x, y)

dot(x, y)
```

## Arguments

- x:

  An embedding column.

- y:

  An embedding column, or a constant numeric query vector.

## Value

A double column.

## Details

`cosine()` returns cosine distance (`1 - similarity`) and `l2()`
Euclidean distance, so smaller means nearer – pair them with
[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
for nearest-neighbour search. `dot()` returns the inner product, where
larger means nearer
([`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)).

## See also

[`as_embedding()`](https://gillescolling.com/vectra/reference/as_embedding.md)

## Examples

``` r
if (FALSE) { # \dontrun{
q <- rnorm(128)
tbl("vecs.vtr") |>
  mutate(d = cosine(emb, q)) |>
  slice_min(d, n = 10) |>
  collect()
} # }
```
