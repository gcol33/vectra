# Create a hash index on a .vtr file column

Builds a persistent hash index stored as a `.vtri` sidecar file
alongside the `.vtr` file. The index maps key hashes to row group
indices, so an equality predicate (`filter(col == value)`) names the row
groups that may hold a key without reading any column data.

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
`filter(col1 == "a", col2 == "b")`). The columns may be named in any
order.

A query opens the index for the column it filters on, so a store can
carry an index on each of several columns and a query pays only for the
one it uses.
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
reports the index a scan will probe. The index composes with zone-map
pruning and binary search on sorted columns.

The index holds one entry per distinct key per row group rather than one
per row, so an index over a column with few distinct values stays small
however many rows the store holds – which is what keeps a lookup off the
size of the store.

[`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
with `along = "rows"` rewrites every row group, which moves the rows a
key sits in; it rebuilds each of the store's indexes for that reason. An
index left behind by any other change of the store is reported as absent
by
[`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
and ignored by queries rather than pruning row groups that may now hold
matching rows. Indexes written by vectra 0.11.7 and earlier are
superseded and read as absent; call `create_index()` again to rebuild
them.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE), f)
create_index(f, "id")
tbl(f) |> filter(id == "m") |> collect()
unlink(c(f, paste0(f, ".id.vtri")))
```
