# Create a lazy table reference from a CSV file

Opens a CSV file for lazy, streaming query execution. Column types are
inferred from the first 1000 rows. No data is read until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_csv(path, batch_size = 65536L)
```

## Arguments

- path:

  Path to a `.csv` file.

- batch_size:

  Number of rows per batch (default 65536).

## Value

A `vectra_node` object representing a lazy scan of the CSV file.

## Examples

``` r
f <- tempfile(fileext = ".csv")
write.csv(mtcars, f, row.names = FALSE)
node <- tbl_csv(f)
print(node)
unlink(f)
```
