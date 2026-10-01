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
#>    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4
#> 3 21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1
#> 4 18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1
#> 5 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4
#> 6 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
#> 7 19.7   6 145.0 175 3.62 2.770 15.50  0  1    5    6
unlink(f)
# }
```
