# Group a vectra query by columns

Group a vectra query by columns

## Usage

``` r
group_by(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Grouping column names (unquoted).

## Value

A `vectra_node` with grouping information stored.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> group_by(cyl) |> summarise(avg = mean(mpg)) |> collect()
unlink(f)
```
