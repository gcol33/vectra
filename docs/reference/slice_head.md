# Select first or last rows

Select first or last rows

## Usage

``` r
slice_head(.data, n = 1L)

slice_tail(.data, n = 1L)

slice_min(.data, order_by, n = 1L, with_ties = TRUE)

slice_max(.data, order_by, n = 1L, with_ties = TRUE)
```

## Arguments

- .data:

  A `vectra_node` object.

- n:

  Number of rows to select.

- order_by:

  Column to order by (for `slice_min`/`slice_max`).

- with_ties:

  If `TRUE` (default), includes all rows that tie with the nth value. If
  `FALSE`, returns exactly `n` rows.

## Value

A `vectra_node` for `slice_head()` and
`slice_min/max(..., with_ties = FALSE)`. A data.frame for `slice_tail()`
and `slice_min/max(..., with_ties = TRUE)` (the default), since these
must materialize all rows.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> slice_head(n = 3) |> collect()
tbl(f) |> slice_min(order_by = mpg, n = 3) |> collect()
tbl(f) |> slice_max(order_by = mpg, n = 3) |> collect()
unlink(f)
```
