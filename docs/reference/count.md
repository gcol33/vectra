# Count observations by group

Count observations by group

## Usage

``` r
count(x, ..., wt = NULL, sort = FALSE, name = NULL)

tally(x, wt = NULL, sort = FALSE, name = NULL)
```

## Arguments

- x:

  A `vectra_node` object.

- ...:

  Grouping columns (unquoted).

- wt:

  Column to weight by (unquoted). If `NULL`, counts rows.

- sort:

  If `TRUE`, sort output in descending order of `n`.

- name:

  Name of the count column (default `"n"`).

## Value

A `vectra_node` with group columns and a count column.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> count(cyl) |> collect()
#>   cyl  n
#> 1   4 11
#> 2   6  7
#> 3   8 14
unlink(f)
```
