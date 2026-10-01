# Filter rows of a vectra query

Filter rows of a vectra query

## Usage

``` r
filter(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Filter expressions (combined with `&`).

## Value

A new `vectra_node` with the filter applied.

## Details

Filter uses zero-copy selection vectors: matching rows are indexed
without copying data. Multiple conditions are combined with `&`.
Supported expression types: arithmetic (`+`, `-`, `*`, `/`, `%%`),
comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`), boolean (`&`, `|`, `!`),
[`is.na()`](https://rdrr.io/r/base/NA.html), and string functions
([`nchar()`](https://rdrr.io/r/base/nchar.html),
[`substr()`](https://rdrr.io/r/base/substr.html),
[`grepl()`](https://rdrr.io/r/base/grep.html); patterns are regex by
default, as in base R, or literal with `fixed = TRUE`).

NA comparisons return NA (SQL semantics). Use
[`is.na()`](https://rdrr.io/r/base/NA.html) to filter NAs explicitly.

This is a streaming operation (constant memory per batch).

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> filter(cyl > 4) |> collect() |> head()
#>    mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
#> 4 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
#> 5 18.1   6  225 105 2.76 3.460 20.22  1  0    3    1
#> 6 14.3   8  360 245 3.21 3.570 15.84  0  0    3    4
unlink(f)
```
