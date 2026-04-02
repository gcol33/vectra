# Remove grouping from a vectra query

Remove grouping from a vectra query

## Usage

``` r
ungroup(x, ...)
```

## Arguments

- x:

  A `vectra_node` object.

- ...:

  Ignored.

## Value

An ungrouped `vectra_node`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> group_by(cyl) |> ungroup()
unlink(f)
```
