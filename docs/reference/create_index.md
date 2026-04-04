# Create a hash index on a .vtr file column

Builds a persistent hash index stored as a `.vtri` sidecar file
alongside the `.vtr` file. The index maps key hashes to row group
indices, enabling O(1) row group identification for equality predicates
(`filter(col == value)`).

## Usage

``` r
create_index(path, column, ci = FALSE)
```

## Arguments

- path:

  Path to a `.vtr` file.

- column:

  Character vector. Name(s) of column(s) to index.

- ci:

  Logical. Build a case-insensitive index? Default `FALSE`.

## Value

Invisible `NULL`. The index is written as a `.vtri` sidecar file.

## Details

For composite indexes on multiple columns, pass a character vector.
Composite indexes accelerate AND-combined equality predicates (e.g.,
`filter(col1 == "a", col2 == "b")`).

The index is automatically loaded by
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) when
present. It composes with zone-map pruning and binary search on sorted
columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE), f)
create_index(f, "id")
tbl(f) |> filter(id == "m") |> collect()
unlink(c(f, paste0(f, ".id.vtri")))
```
