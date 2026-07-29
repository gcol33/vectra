# Floor a datetime column to a calendar grid

Truncates a `Date` or `POSIXct` column down to a unit boundary – the
basis for time bucketing. Usable inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) /
[`filter()`](https://gillescolling.com/vectra/reference/filter.md);
[`resample()`](https://gillescolling.com/vectra/reference/resample.md)
calls it for you.

## Usage

``` r
floor_time(t, unit)
```

## Arguments

- t:

  A `Date` or `POSIXct` column (stored as days / seconds since the
  epoch).

- unit:

  A bucket size as a string: a count and a unit, e.g. `"hour"`,
  `"15 min"`, `"day"`, `"week"`, `"3 months"`, `"quarter"`, `"year"`.

## Value

A numeric (epoch) column.

## Details

The result is returned in the column's own scale (days for `Date`,
seconds for `POSIXct`), so it remains a valid instant. The engine does
not re-attach the `Date`/`POSIXct` class to a computed column, so a
floored column collects as the underlying numeric epoch; wrap with
`as.POSIXct(x, origin = "1970-01-01", tz = "UTC")` or
`as.Date(x, origin = "1970-01-01")` if you need the class back.

## See also

[`resample()`](https://gillescolling.com/vectra/reference/resample.md)
