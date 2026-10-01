# Relocate columns

Relocate columns

## Usage

``` r
relocate(.data, ..., .before = NULL, .after = NULL)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Column names to move.

- .before:

  Column name to place before (unquoted).

- .after:

  Column name to place after (unquoted).

## Value

A new `vectra_node` with reordered columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> relocate(hp, wt, .before = cyl) |> collect() |> head()
#>    mpg  hp    wt cyl disp drat  qsec vs am gear carb
#> 1 21.0 110 2.620   6  160 3.90 16.46  0  1    4    4
#> 2 21.0 110 2.875   6  160 3.90 17.02  0  1    4    4
#> 3 22.8  93 2.320   4  108 3.85 18.61  1  1    4    1
#> 4 21.4 110 3.215   6  258 3.08 19.44  1  0    3    1
#> 5 18.7 175 3.440   8  360 3.15 17.02  0  0    3    2
#> 6 18.1 105 3.460   6  225 2.76 20.22  1  0    3    1
unlink(f)
```
