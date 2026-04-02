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
unlink(f)
```
