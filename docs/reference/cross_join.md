# Cross join two vectra tables

Returns every combination of rows from `x` and `y` (Cartesian product).
Both tables are collected before joining.

## Usage

``` r
cross_join(x, y, suffix = c(".x", ".y"), ...)
```

## Arguments

- x:

  A `vectra_node` object or data.frame.

- y:

  A `vectra_node` object or data.frame.

- suffix:

  Suffixes for disambiguating column names (default `c(".x", ".y")`).

- ...:

  Ignored.

## Value

A data.frame with `nrow(x) * nrow(y)` rows.

## Examples

``` r
f1 <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
write_vtr(data.frame(a = 1:2), f1)
write_vtr(data.frame(b = c("x", "y", "z"), stringsAsFactors = FALSE), f2)
cross_join(tbl(f1), tbl(f2))
#>   a b
#> 1 1 x
#> 2 1 y
#> 3 1 z
#> 4 2 x
#> 5 2 y
#> 6 2 z
unlink(c(f1, f2))
```
