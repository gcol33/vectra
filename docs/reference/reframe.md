# Summarise with variable-length output per group

Like
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
but allows expressions that return more than one row per group.
Currently implemented via
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
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

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(g = c("a", "a", "b"), x = c(1, 2, 3)), f)
tbl(f) |> group_by(g) |> reframe(range_x = range(x))
unlink(f)
```
