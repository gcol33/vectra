# Write query results or a data.frame to a CSV file

For `vectra_node` inputs, data is streamed batch-by-batch to disk
without materializing the full result in memory. For `data.frame`
inputs, the data is written directly.

## Usage

``` r
write_csv(x, path, ...)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path for the output CSV file.

- ...:

  Reserved for future use.

## Value

Invisible `NULL`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars[1:5, ], f)
csv <- tempfile(fileext = ".csv")
tbl(f) |> write_csv(csv)
unlink(c(f, csv))
```
