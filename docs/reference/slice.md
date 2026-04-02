# Select rows by position

Select rows by position

## Usage

``` r
slice(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Integer row indices (positive or negative).

## Value

A data.frame with the selected rows.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> slice(1, 3, 5)
unlink(f)
```
