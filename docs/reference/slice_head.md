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
string). `with_ties = FALSE` returns exactly `n` rows per group;
`with_ties = TRUE` keeps rows tied at the nth value via min-rank. The
`n = 1`, `with_ties = FALSE` case streams: it holds only the running
winner per group, so memory scales with the number of groups (the result
size), not the input. Other grouped cases buffer their input.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> slice_head(n = 3) |> collect()
#>    mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
tbl(f) |> slice_min(order_by = mpg, n = 3) |> collect()
#>     mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 15 10.4   8  472 205 2.93 5.250 17.98  0  0    3    4
#> 16 10.4   8  460 215 3.00 5.424 17.82  0  0    3    4
#> 24 13.3   8  350 245 3.73 3.840 15.41  0  0    3    4
tbl(f) |> slice_max(order_by = mpg, n = 3) |> collect()
#>     mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 20 33.9   4 71.1  65 4.22 1.835 19.90  1  1    4    1
#> 18 32.4   4 78.7  66 4.08 2.200 19.47  1  1    4    1
#> 19 30.4   4 75.7  52 4.93 1.615 18.52  1  1    4    2
#> 28 30.4   4 95.1 113 3.77 1.513 16.90  1  1    5    2
# earliest row per group, geometry/attrs preserved:
tbl(f) |> group_by(cyl) |> slice_min(mpg, n = 1, with_ties = FALSE) |> collect()
#>    mpg cyl  disp  hp drat   wt  qsec vs am gear carb
#> 1 21.4   4 121.0 109 4.11 2.78 18.60  1  1    4    2
#> 2 17.8   6 167.6 123 3.92 3.44 18.90  1  0    4    4
#> 3 10.4   8 472.0 205 2.93 5.25 17.98  0  0    3    4
unlink(f)
```
