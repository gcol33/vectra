# Bind rows or columns from multiple vectra tables

Bind rows or columns from multiple vectra tables

## Usage

``` r
bind_rows(..., .id = NULL)

bind_cols(...)
```

## Arguments

- ...:

  `vectra_node` objects or data.frames to combine.

- .id:

  Optional column name for a source identifier.

## Value

A `vectra_node` (streaming) when all inputs are `vectra_node` with
identical schemas and `.id` is NULL. Otherwise a data.frame.

## Details

When all inputs are `vectra_node` objects with identical column names
and types and no `.id` is requested, `bind_rows` creates a streaming
`ConcatNode` that iterates children sequentially without materializing.

Otherwise, inputs are collected and combined in R. Missing columns are
filled with NA.

`bind_cols` requires the same number of rows in each input.

## Examples

``` r
f1 <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
write_vtr(data.frame(x = 1:3, y = 4:6), f1)
write_vtr(data.frame(x = 7:9, y = 10:12), f2)
bind_rows(tbl(f1), tbl(f2)) |> collect()
bind_cols(tbl(f1), tbl(f2))
unlink(c(f1, f2))
```
