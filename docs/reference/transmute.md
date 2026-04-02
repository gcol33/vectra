# Keep only columns from mutate expressions

Like [`mutate()`](https://gcol33.github.io/vectra/reference/mutate.md)
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
