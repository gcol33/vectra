# Check whether a .vtr column has a usable hash index

`TRUE` when the `.vtri` sidecar is present, readable, in the current
format, and built against the store as it now stands. An index that no
longer matches the store, or that cannot be read, reads as `FALSE`,
because queries ignore it and fall back to a scan;
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
rebuilds it.

## Usage

``` r
has_index(path, column)
```

## Arguments

- path:

  Path to a `.vtr` file.

- column:

  Character vector. Name(s) of column(s), in any order.

## Value

Logical scalar: `TRUE` if the index exists and can be used.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE), f)
has_index(f, "id")   # FALSE
create_index(f, "id")
has_index(f, "id")   # TRUE
unlink(c(f, paste0(f, ".id.vtri")))
```
