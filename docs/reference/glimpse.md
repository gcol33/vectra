# Get a glimpse of a vectra table

Shows column names, types, and a preview of the first few values without
collecting the full result.

## Usage

``` r
glimpse(x, width = 5L, ...)
```

## Arguments

- x:

  A `vectra_node` object.

- width:

  Maximum number of preview rows to fetch (default 5).

- ...:

  Ignored.

## Value

Invisible `x`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> glimpse()
unlink(f)
```
