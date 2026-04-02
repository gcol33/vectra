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
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   FilterNode [streaming] 
#>     ScanNode [streaming, 2/11 cols (pruned), predicate pushdown, v3 stats] 
#> 
#> Output columns (2):
#>   mpg <double>
#>   cyl <double>
unlink(f)
```
