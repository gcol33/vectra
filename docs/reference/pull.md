# Extract a single column as a vector

Extract a single column as a vector

## Usage

``` r
pull(.data, var = -1)
```

## Arguments

- .data:

  A `vectra_node` object.

- var:

  Column name (unquoted) or positive integer position.

## Value

A vector.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> pull(mpg) |> head()
unlink(f)
```
