# Write a data.frame to a .vtr file

Serializes an R data.frame into the vectra1 on-disk format.

## Usage

``` r
write_vtr(df, path, batch_size = nrow(df))
```

## Arguments

- df:

  A data.frame to write. Supported column types: integer, double,
  logical, character, and bit64::integer64.

- path:

  File path for the output .vtr file.

- batch_size:

  Number of rows per row group. Defaults to all rows in a single row
  group.

## Value

Invisible `NULL`.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
unlink(f)
```
