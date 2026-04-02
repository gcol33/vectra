# Summarise with variable-length output per group

Like
[`summarise()`](https://gcol33.github.io/vectra/reference/summarise.md)
but allows expressions that return more than one row per group.
Currently implemented via
[`collect()`](https://gcol33.github.io/vectra/reference/collect.md)
fallback.

## Usage

``` r
reframe(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Named expressions.

## Value

A data.frame (not a lazy node).
