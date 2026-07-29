# Compute the logical diff between two .vtr files

Streams both files and computes a set-level diff keyed on `key_col`.
Returns a list with two elements:

## Usage

``` r
diff_vtr(old_path, new_path, key_col)
```

## Arguments

- old_path:

  Path to the older `.vtr` file.

- new_path:

  Path to the newer `.vtr` file.

- key_col:

  Name of the column to use as the row key (must exist in both files
  with the same type).

## Value

A named list with elements `added` (a `vectra_node`) and `deleted` (a
vector of key values).

## Details

- `added`: a `vectra_node` (lazy
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md)) of rows
  present in `new_path` but not `old_path` (matched on `key_col`). Call
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  to materialise. The underlying temp file is deleted when the node is
  garbage-collected **or** when the calling R session ends via
  [`on.exit()`](https://rdrr.io/r/base/on.exit.html).

- `deleted`: a vector of key values present in `old_path` but not
  `new_path`.

This is a **logical diff** (key-based set difference), not a binary file
diff. Rows with the same key that have changed values are not reported
as modified — use `added` and `deleted` together to detect updates (a
key that appears in both means a row was replaced).

`key_col` is treated as a **primary key**: keys are assumed unique
within each file, so if the same key appears on several rows of
`new_path` only one is reported in `added`.

Both files are streamed through the external sort (keyed by `key_col`)
and merged in a single forward pass, so peak memory is bounded by the
sort's spill budget
([`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md))
rather than by the number of distinct keys. The added rows stream to a
temp file; only the returned `deleted` key vector is materialised (its
size is the number of deleted keys).

## Examples

``` r
f1 <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
df1 <- data.frame(id = 1:5, val = letters[1:5], stringsAsFactors = FALSE)
df2 <- data.frame(id = c(3L, 4L, 5L, 6L, 7L),
                  val = c("C", "d", "e", "f", "g"),
                  stringsAsFactors = FALSE)
write_vtr(df1, f1)
write_vtr(df2, f2)

d <- diff_vtr(f1, f2, "id")
# Rows 1 and 2 deleted; rows 6 and 7 added
stopifnot(all(d$deleted %in% c(1, 2)))
stopifnot(all(collect(d$added)$id %in% c(6, 7)))

unlink(c(f1, f2))
```
