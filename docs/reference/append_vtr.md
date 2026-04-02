# Append rows to an existing .vtr file

Appends one or more new row groups to the end of an existing `.vtr` file
without touching or recompressing existing row groups. The schema of `x`
must exactly match the schema of the target file (same column names and
types, in the same order).

## Usage

``` r
append_vtr(x, path, ...)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path of an existing `.vtr` file to append to.

- ...:

  Additional arguments passed to methods.

## Value

Invisible `NULL`.

## Details

The operation is not fully atomic: if the process is interrupted after
new row groups are written but before the header is patched, the file
will be in a corrupted state. Use
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
for safety-critical write-once workloads.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars[1:10, ], f)
append_vtr(mtcars[11:20, ], f)
result <- tbl(f) |> collect()
stopifnot(nrow(result) == 20L)
unlink(f)
```
