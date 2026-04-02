# Logically delete rows from a .vtr file

Marks the specified 0-based physical row indices as deleted by writing
(or updating) a tombstone side file (`<path>.del`). The original `.vtr`
file is never modified. The next call to
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) on the same
path will automatically exclude the deleted rows.

## Usage

``` r
delete_vtr(path, row_ids)
```

## Arguments

- path:

  File path of the `.vtr` file to delete rows from.

- row_ids:

  A numeric vector of **0-based** physical row indices to delete.
  Out-of-range indices are silently ignored on read (they will never
  match a real row).

## Value

Invisible `NULL`.

## Details

Tombstone files are cumulative: calling `delete_vtr()` multiple times on
the same file merges all deletions (union, deduplicated). To undo
deletions, remove the `.del` file manually with
`unlink(paste0(path, ".del"))`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

# Delete the first and third rows (0-based indices 0 and 2)
delete_vtr(f, c(0, 2))

result <- tbl(f) |> collect()
stopifnot(nrow(result) == nrow(mtcars) - 2L)

unlink(c(f, paste0(f, ".del")))
```
