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
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)).

- ...:

  Named aggregation expressions using `n()`,
  [`sum()`](https://rdrr.io/r/base/sum.html),
  [`mean()`](https://rdrr.io/r/base/mean.html),
  [`min()`](https://rdrr.io/r/base/Extremes.html),
  [`max()`](https://rdrr.io/r/base/Extremes.html),
  [`sd()`](https://rdrr.io/r/stats/sd.html),
  [`var()`](https://rdrr.io/r/stats/cor.html), `first()`, `last()`,
  [`any()`](https://rdrr.io/r/base/any.html),
  [`all()`](https://rdrr.io/r/base/all.html),
  [`median()`](https://rdrr.io/r/stats/median.html), `n_distinct()`.

- .groups:

  How to handle groups in the result. One of `"drop_last"` (default),
  `"drop"`, or `"keep"`.

## Value

A `vectra_node` with one row per group.

## Details

Aggregation is hash-based by default. When the engine detects it is
advantageous, it switches to a sort-based path that can spill to disk,
keeping memory bounded regardless of group count.

All aggregation functions accept `na.rm = TRUE` to skip NA values.
Without `na.rm`, any NA in a group poisons the result (returns NA).
R-matching edge cases: `sum(na.rm = TRUE)` on all-NA returns 0,
`mean(na.rm = TRUE)` on all-NA returns NaN, `min/max(na.rm = TRUE)` on
all-NA returns Inf/-Inf with a warning.

This is a materializing operation.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> group_by(cyl) |> summarise(avg_mpg = mean(mpg)) |> collect()
unlink(f)
```
