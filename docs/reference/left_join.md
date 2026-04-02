# Join two vectra tables

Join two vectra tables

## Usage

``` r
left_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

inner_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

right_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

full_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

semi_join(x, y, by = NULL, ...)

anti_join(x, y, by = NULL, ...)
```

## Arguments

- x:

  A `vectra_node` object (left table).

- y:

  A `vectra_node` object (right table).

- by:

  A character vector of column names to join by, or a named vector like
  `c("a" = "b")`. `NULL` for natural join (common columns).

- suffix:

  A character vector of length 2 for disambiguating non-key columns with
  the same name (default `c(".x", ".y")`).

- ...:

  Ignored.

## Value

A `vectra_node` with the joined result.

## Examples

``` r
f1 <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
left_join(tbl(f1), tbl(f2), by = "id") |> collect()
#>   id  x   y
#> 1  1 10 100
#> 2  2 20 200
#> 3  3 30  NA
unlink(c(f1, f2))
```
