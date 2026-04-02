# Add or transform columns

Add or transform columns

## Usage

``` r
mutate(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Named expressions for new or transformed columns.

## Value

A new `vectra_node` with mutated columns.

## Details

Supported expression types: arithmetic (`+`, `-`, `*`, `/`, `%%`),
comparison, boolean, [`is.na()`](https://rdrr.io/r/base/NA.html),
[`nchar()`](https://rdrr.io/r/base/nchar.html),
[`substr()`](https://rdrr.io/r/base/substr.html),
[`grepl()`](https://rdrr.io/r/base/grep.html) (fixed match only). Window
functions (`row_number()`, [`rank()`](https://rdrr.io/r/base/rank.html),
`dense_rank()`, [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
[`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
[`cummin()`](https://rdrr.io/r/base/cumsum.html),
[`cummax()`](https://rdrr.io/r/base/cumsum.html)) are detected
automatically and routed to a dedicated window node.

When grouped, window functions respect partition boundaries.

This is a streaming operation for regular expressions; window functions
materialize all rows within each partition.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> mutate(kpl = mpg * 0.425144) |> collect() |> head()
unlink(f)
```
