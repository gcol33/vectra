# Select columns from a vectra query

Select columns from a vectra query

## Usage

``` r
select(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Column names (unquoted).

## Value

A new `vectra_node` with only the selected columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> select(mpg, cyl) |> collect() |> head()
#>    mpg cyl
#> 1 21.0   6
#> 2 21.0   6
#> 3 22.8   4
#> 4 21.4   6
#> 5 18.7   8
#> 6 18.1   6
unlink(f)
```
