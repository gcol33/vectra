# Filter rows of a vectra query

Filter rows of a vectra query

## Usage

``` r
filter(.data, ...)
```

## Arguments

- .data:

  A `vectra_node` object.

- ...:

  Filter expressions (combined with `&`).

## Value

A new `vectra_node` with the filter applied.

## Details

Filter uses zero-copy selection vectors: matching rows are indexed
without copying data. Multiple conditions are combined with `&`.
Supported expression types: arithmetic (`+`, `-`, `*`, `/`, `%%`),
comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`), boolean (`&`, `|`, `!`),
[`is.na()`](https://rdrr.io/r/base/NA.html), and string functions
([`nchar()`](https://rdrr.io/r/base/nchar.html),
[`substr()`](https://rdrr.io/r/base/substr.html),
[`grepl()`](https://rdrr.io/r/base/grep.html) with fixed patterns).

NA comparisons return NA (SQL semantics). Use
[`is.na()`](https://rdrr.io/r/base/NA.html) to filter NAs explicitly.

This is a streaming operation (constant memory per batch).

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> filter(cyl > 4) |> collect() |> head()
unlink(f)
```
