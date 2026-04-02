# Keep only columns from mutate expressions

Like [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
but drops all other columns.

## Usage

``` r
transmute(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Named expressions.

## Value

A new `vectra_node` with only the computed columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> transmute(kpl = mpg * 0.425) |> collect() |> head()
unlink(f)
```
