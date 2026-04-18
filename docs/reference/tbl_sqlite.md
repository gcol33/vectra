# Create a lazy table reference from a SQLite database

Opens a SQLite database and lazily scans a table. Column types are
inferred from declared types in the CREATE TABLE statement. All
filtering, grouping, and aggregation is handled by vectra's C engine —
no SQL parsing needed. No data is read until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_sqlite(path, table, batch_size = .DEFAULT_BATCH_SIZE)
```

## Arguments

- path:

  Path to a SQLite database file.

- table:

  Name of the table to scan.

- batch_size:

  Number of rows per batch (default 65536).

## Value

A `vectra_node` object representing a lazy scan of the table.

## Examples

``` r
# \donttest{
f <- tempfile(fileext = ".sqlite")
write_sqlite(mtcars, f, "cars")
node <- tbl_sqlite(f, "cars")
node |> filter(cyl == 6) |> collect()
unlink(f)
# }
```
