# Materialize a vectra node into a reusable in-memory block

Consumes a vectra node (pulling all batches) and stores the result as a
persistent columnar block in memory. Unlike nodes, blocks can be probed
repeatedly via
[`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
without re-scanning.

## Usage

``` r
materialize(.data)
```

## Arguments

- .data:

  A `vectra_node` (consumed; cannot be used after this call).

## Value

A `vectra_block` object (external pointer to C-level ColumnBlock).

## Examples

``` r
if (FALSE) { # \dontrun{
blk <- materialize(tbl("backbone.vtr") |> select(taxonID, canonicalName))
hits <- block_lookup(blk, "canonicalName", c("Quercus robur", "Pinus sylvestris"))
} # }
```
