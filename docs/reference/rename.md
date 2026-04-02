# Rename columns

Rename columns

## Usage

``` r
rename(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Rename pairs: `new_name = old_name`.

## Value

A new `vectra_node` with renamed columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> rename(miles_per_gallon = mpg) |> collect() |> head()
unlink(f)
```
