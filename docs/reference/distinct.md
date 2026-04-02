# Keep distinct/unique rows

Keep distinct/unique rows

## Usage

``` r
distinct(.data, ..., .keep_all = FALSE)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Column names (unquoted). If empty, uses all columns.

- .keep_all:

  If `TRUE`, keep all columns (not just those in `...`).

## Value

A `vectra_node` with unique rows.

## Details

Uses hash-based grouping with zero aggregations. When `.keep_all = TRUE`
with a column subset, falls back to R's
[`duplicated()`](https://rdrr.io/r/base/duplicated.html) with a message.

This is a materializing operation.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> distinct(cyl) |> collect()
unlink(f)
```
