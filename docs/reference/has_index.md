# Check if a hash index exists for a .vtr column

Check if a hash index exists for a .vtr column

## Usage

``` r
has_index(path, column)
```

## Arguments

- path:

  Path to a `.vtr` file.

- column:

  Character vector. Name(s) of column(s).

## Value

Logical scalar: `TRUE` if a `.vtri` index file exists.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE), f)
has_index(f, "id")   # FALSE
create_index(f, "id")
has_index(f, "id")   # TRUE
unlink(c(f, paste0(f, ".id.vtri")))
```
