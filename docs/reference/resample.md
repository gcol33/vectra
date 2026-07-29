# Resample a time series to a calendar grid

Buckets a datetime column to a grid (`every`) and aggregates within each
bucket – the time-series form of
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md) +
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md).
Equivalent to `mutate(<time> = floor_time(<time>, every))`,
`group_by(<time>)`, then `summarise(...)`.

## Usage

``` r
resample(.data, time, every, ..., .name = NULL)
```

## Arguments

- .data:

  A `vectra_node`.

- time:

  The datetime column to bucket (unquoted).

- every:

  Bucket size as a string, e.g. `"1 hour"`, `"15 min"`, `"day"`,
  `"month"` (see
  [`floor_time()`](https://gillescolling.com/vectra/reference/floor_time.md)).

- ...:

  Named aggregation expressions, as in
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  (e.g. `mean_temp = mean(temp)`).

- .name:

  Optional name for the bucket column. Defaults to the name of `time`
  (the original column, replaced by its floored value).

## Value

A `vectra_node` with one row per occupied bucket: the bucket column
followed by the aggregates. The bucket collects as a numeric epoch value
(see
[`floor_time()`](https://gillescolling.com/vectra/reference/floor_time.md)
on restoring the date class).

## See also

[`floor_time()`](https://gillescolling.com/vectra/reference/floor_time.md),
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)

## Examples

``` r
if (FALSE) { # \dontrun{
tbl("readings.vtr") |>
  resample(timestamp, "1 hour", mean_temp = mean(temp), n = n()) |>
  collect()
} # }
```
