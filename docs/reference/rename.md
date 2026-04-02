# Rename columns

Rename columns

## Usage

``` r
rename(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Rename pairs: `new_name = old_name`.

## Value

A new `vectra_node` with renamed columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> rename(miles_per_gallon = mpg) |> collect() |> head()
#>   miles_per_gallon cyl disp  hp drat    wt  qsec vs am gear carb
#> 1             21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2             21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3             22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
#> 4             21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
#> 5             18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
#> 6             18.1   6  225 105 2.76 3.460 20.22  1  0    3    1
unlink(f)
```
