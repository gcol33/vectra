# Probe a materialized block by column value

Performs a hash lookup on a string column of a materialized block.
Returns all rows where the column value matches one of the query keys.
Hash indices are built lazily on first use and cached for subsequent
calls.

## Usage

``` r
block_lookup(block, column, keys, ci = FALSE)
```

## Arguments

- block:

  A `vectra_block` from
  [`materialize()`](https://gillescolling.com/vectra/reference/materialize.md).

- column:

  Character scalar. Name of the string column to match against.

- keys:

  Character vector. Query values to look up.

- ci:

  Logical. Case-insensitive matching (default `FALSE`).

## Value

A data.frame with column `query_idx` (1-based position in `keys`) plus
all columns from the block, for each (query, block_row) match pair.

## Examples

``` r
if (FALSE) { # \dontrun{
hits <- block_lookup(blk, "canonicalName", c("Quercus robur"))
ci_hits <- block_lookup(blk, "canonicalName", c("quercus robur"), ci = TRUE)
} # }
```
