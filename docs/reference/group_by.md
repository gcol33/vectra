# Group a vectra query by columns

Group a vectra query by columns

## Usage

``` r
group_by(.data, ..., .add = FALSE)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Grouping columns. Bare column names, injected symbols
  (`!!rlang::sym(k)`, `!!!rlang::syms(ks)`), the `.data` pronoun
  (`.data[[k]]`),
  [`across()`](https://gillescolling.com/vectra/reference/across.md)/`pick()`
  with a tidyselect selection (`across(all_of(ks))`), or expressions,
  which add a computed column first (`group_by(decade = year %/% 10)`;
  an unnamed expression is named after its text, as in dplyr).

- .add:

  If `FALSE` (default), replace the existing grouping; if `TRUE`, add to
  it.

## Value

A `vectra_node` with grouping information stored.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> group_by(cyl) |> summarise(avg = mean(mpg)) |> collect()
#>   cyl      avg
#> 1   4 26.66364
#> 2   6 19.74286
#> 3   8 15.10000
unlink(f)
```
