# Sort rows by column values

Sort rows by column values

## Usage

``` r
arrange(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Column names (unquoted). Wrap in
  [`desc()`](https://gillescolling.com/vectra/reference/desc.md) for
  descending order.

## Value

A new `vectra_node` with sorted rows.

## Details

Uses an external merge sort with a 1 GB memory budget. When data exceeds
this limit, sorted runs are spilled to temporary `.vtr` files and merged
via a k-way min-heap. NAs sort last in ascending order.

This is a materializing operation.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> arrange(desc(mpg)) |> collect() |> head()
unlink(f)
```
