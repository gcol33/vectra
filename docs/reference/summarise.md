# Summarise grouped data

Summarise grouped data

## Usage

``` r
summarise(.data, ..., .groups = NULL)

summarize(.data, ..., .groups = NULL)
```

## Arguments

- .data:

  A grouped `vectra_node` (from
  [`group_by()`](https://gcol33.github.io/vectra/reference/group_by.md)).

- ...:

  Named aggregation expressions using `n()`,
  [`sum()`](https://rdrr.io/r/base/sum.html),
  [`mean()`](https://rdrr.io/r/base/mean.html),
  [`min()`](https://rdrr.io/r/base/Extremes.html),
  [`max()`](https://rdrr.io/r/base/Extremes.html).

- .groups:

  How to handle groups in the result. One of `"drop_last"` (default),
  `"drop"`, or `"keep"`.

## Value

A `vectra_node` with one row per group.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> group_by(cyl) |> summarise(avg_mpg = mean(mpg)) |> collect()
#>   cyl  avg_mpg
#> 1   4 26.66364
#> 2   6 19.74286
#> 3   8 15.10000
unlink(f)
```
