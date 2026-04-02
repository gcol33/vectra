# Execute a lazy query and return a data.frame

Pulls all batches from the execution plan and materializes the result as
an R data.frame.

## Usage

``` r
collect(x, ...)
```

## Arguments

- x:

  A `vectra_node` object.

- ...:

  Ignored.

## Value

A data.frame with the query results.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
result <- tbl(f) |> collect()
head(result)
unlink(f)
```
