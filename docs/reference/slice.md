# Select rows by position

Select rows by position

## Usage

``` r
slice(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Integer row indices (positive or negative).

## Value

A data.frame with the selected rows.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> slice(1, 3, 5)
#>    mpg cyl disp  hp drat   wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.62 16.46  0  1    4    4
#> 3 22.8   4  108  93 3.85 2.32 18.61  1  1    4    1
#> 5 18.7   8  360 175 3.15 3.44 17.02  0  0    3    2
unlink(f)
```
