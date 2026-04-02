# Create a lazy table reference from a SQLite database

Opens a SQLite database and lazily scans a table. Column types are
inferred from declared types in the CREATE TABLE statement. All
filtering, grouping, and aggregation is handled by vectra's C engine —
no SQL parsing needed. No data is read until
[`collect()`](https://gcol33.github.io/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_sqlite(path, table, batch_size = 65536L)
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
if (FALSE) { # \dontrun{
node <- tbl_sqlite("data.sqlite", "measurements")
node |> filter(year > 2020) |> collect()
} # }
```
