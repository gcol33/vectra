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

  If `TRUE`, include ties. Currently ignored.

## Value

A `vectra_node` or data.frame.
