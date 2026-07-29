# Time-based rolling aggregates

Trailing rolling aggregates over a datetime window, usable inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md). For
each row, the aggregate covers the rows whose `time` falls in
`(time - every, time]` – the row itself and everything within one window
before it. With an upstream
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md),
windows are computed within each group. `NA` values are skipped.

## Usage

``` r
roll_sum(x, time, every)

roll_mean(x, time, every)

roll_min(x, time, every)

roll_max(x, time, every)

roll_n(time, every)
```

## Arguments

- x:

  Value column to aggregate (not needed for `roll_n`).

- time:

  Datetime column defining the window.

- every:

  Window span as a fixed-width duration string.

## Value

A double column.

## Details

`time` must be a `Date` or `POSIXct` column; `every` is a fixed-width
duration string (`"15 min"`, `"1 hour"`, `"7 days"`) – calendar units
(month, year) are not allowed because their length varies.

## See also

[`resample()`](https://gillescolling.com/vectra/reference/resample.md)

## Examples

``` r
if (FALSE) { # \dontrun{
tbl("readings.vtr") |>
  group_by(sensor) |>
  mutate(avg_1h = roll_mean(temp, t, "1 hour"),
         n_1h   = roll_n(t, "1 hour")) |>
  collect()
} # }
```
