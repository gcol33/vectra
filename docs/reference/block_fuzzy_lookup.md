# Fuzzy-match query keys against a materialized block

Computes string distances between query keys and a string column in a
materialized block. Optionally uses exact-match blocking on a second
column (e.g., genus) to reduce the search space.

## Usage

``` r
block_fuzzy_lookup(
  block,
  column,
  keys,
  method = "dl",
  max_dist = 0.2,
  block_col = NULL,
  block_keys = NULL,
  n_threads = 4L
)
```

## Arguments

- block:

  A `vectra_block` from
  [`materialize()`](https://gillescolling.com/vectra/reference/materialize.md).

- column:

  Character scalar. Name of the string column to fuzzy-match against.

- keys:

  Character vector. Query strings to match.

- method:

  Character. Distance method: `"dl"` (Damerau-Levenshtein, default),
  `"levenshtein"`, or `"jw"` (Jaro-Winkler).

- max_dist:

  Numeric. Maximum normalized distance (default 0.2).

- block_col:

  Optional character scalar. Column name for exact-match blocking (e.g.,
  genus). When provided, only rows where `block_col` matches the
  corresponding `block_keys` value are compared.

- block_keys:

  Optional character vector (same length as `keys`). Exact-match values
  for blocking. Required when `block_col` is provided.

- n_threads:

  Integer. Number of OpenMP threads (default 4L).

## Value

A data.frame with columns `query_idx` (1-based position in `keys`),
`fuzzy_dist` (normalized distance), plus all columns from the block.
