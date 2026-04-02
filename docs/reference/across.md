# Apply a function across multiple columns

Used inside
[`mutate()`](https://gcol33.github.io/vectra/reference/mutate.md) or
[`summarise()`](https://gcol33.github.io/vectra/reference/summarise.md)
to apply a function to multiple columns selected with tidyselect.
Returns a named list of expressions.

## Usage

``` r
across(.cols, .fns, ..., .names = NULL)
```

## Arguments

- .cols:

  Column selection (tidyselect).

- .fns:

  A function, formula, or named list of functions.

- ...:

  Additional arguments passed to `.fns`.

- .names:

  A glue-style naming pattern. Uses `{.col}` and `{.fn}`. Default:
  `"{.col}"` if `.fns` is a single function, `"{.col}_{.fn}"` if `.fns`
  is a named list.

## Value

A named list used internally by mutate/summarise.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
# In summarise (conceptual; across is expanded to individual expressions)
unlink(f)
```
