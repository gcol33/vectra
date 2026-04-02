# Write query results or a data.frame to a SQLite table

For `vectra_node` inputs, data is streamed batch-by-batch to disk
without materializing the full result in memory. For `data.frame`
inputs, the data is written directly.

## Usage

``` r
write_sqlite(x, path, table, ...)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path for the SQLite database.

- table:

  Name of the table to create/write into.

- ...:

  Reserved for future use.

## Value

Invisible `NULL`.

## Examples

``` r
db <- tempfile(fileext = ".sqlite")
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars[1:5, ], f)
tbl(f) |> write_sqlite(db, "cars")
unlink(c(f, db))
```
