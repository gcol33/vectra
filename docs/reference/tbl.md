# Create a lazy table reference from a .vtr file

Opens a vectra1 file and returns a lazy query node. No data is read
until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl(path)
```

## Arguments

- path:

  Path to a `.vtr` file.

## Value

A `vectra_node` object representing a lazy scan of the file.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
node <- tbl(f)
print(node)
unlink(f)
```
