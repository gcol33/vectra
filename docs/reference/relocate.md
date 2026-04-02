# Relocate columns

Relocate columns

## Usage

``` r
relocate(.data, ..., .before = NULL, .after = NULL)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Column names to move.

- .before:

  Column name to place before (unquoted).

- .after:

  Column name to place after (unquoted).

## Value

A new `vectra_node` with reordered columns.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> relocate(hp, wt, .before = cyl) |> collect() |> head()
unlink(f)
```
