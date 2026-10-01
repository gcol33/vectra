# Get a glimpse of a vectra table

Shows column names, types, and a preview of the first few values without
collecting the full result.

## Usage

``` r
glimpse(x, width = 5L, ...)
```

## Arguments

- x:

  A `vectra_node` object.

- width:

  Maximum number of preview rows to fetch (default 5).

- ...:

  Ignored.

## Value

Invisible `x`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> glimpse()
#> vectra lazy table [32 x 11]
#> $ mpg             <double> 21.0, 21.0, 22.8, 21.4, 18.7
#> $ cyl             <double> 6, 6, 4, 6, 8
#> $ disp            <double> 160, 160, 108, 258, 360
#> $ hp              <double> 110, 110, 93, 110, 175
#> $ drat            <double> 3.90, 3.90, 3.85, 3.08, 3.15
#> $ wt              <double> 2.620, 2.875, 2.320, 3.215, 3.440
#> $ qsec            <double> 16.46, 17.02, 18.61, 19.44, 17.02
#> $ vs              <double> 0, 0, 1, 1, 0
#> $ am              <double> 1, 1, 1, 0, 0
#> $ gear            <double> 4, 4, 4, 3, 3
#> $ carb            <double> 4, 4, 1, 1, 2
unlink(f)
```
