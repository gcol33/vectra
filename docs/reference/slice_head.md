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

A `vectra_node` for `slice_head()`, for grouped
`slice_min()`/`slice_max()`, and for ungrouped
`slice_min/max(..., with_ties = FALSE)`. A data.frame for `slice_tail()`
and ungrouped `slice_min/max(..., with_ties = TRUE)` (the default),
since these must materialize all rows.

## Details

When `slice_min()`/`slice_max()` follow
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md),
the n smallest/largest rows are taken within each group and the whole
winning row is kept (every column, including geometry carried as a
string). `with_ties = FALSE` returns exactly `n` rows per group via a
deterministic ordered `row_number()`; `with_ties = TRUE` keeps rows tied
at the nth value via min-rank. The grouped path buffers its input (like
all window operations) rather than streaming.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> slice_head(n = 3) |> collect()
tbl(f) |> slice_min(order_by = mpg, n = 3) |> collect()
tbl(f) |> slice_max(order_by = mpg, n = 3) |> collect()
# earliest row per group, geometry/attrs preserved:
tbl(f) |> group_by(cyl) |> slice_min(mpg, n = 1, with_ties = FALSE) |> collect()
unlink(f)
```
