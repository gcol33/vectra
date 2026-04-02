# Print the execution plan for a vectra query

Shows the node types, column schemas, and structure of the lazy query
plan.

## Usage

``` r
explain(x, ...)
```

## Arguments

- x:

  A `vectra_node` object.

- ...:

  Ignored.

## Value

Invisible `x`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> filter(cyl > 4) |> select(mpg, cyl) |> explain()
unlink(f)
```
