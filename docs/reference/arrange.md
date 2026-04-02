# Sort rows by column values

Sort rows by column values

## Usage

``` r
arrange(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Column names (unquoted). Wrap in
  [`desc()`](https://gcol33.github.io/vectra/reference/desc.md) for
  descending order.

## Value

A new `vectra_node` with sorted rows.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> arrange(desc(mpg)) |> collect() |> head()
#>    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> 1 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
#> 2 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
#> 3 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
#> 4 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2
#> 5 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1
#> 6 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2
unlink(f)
```
