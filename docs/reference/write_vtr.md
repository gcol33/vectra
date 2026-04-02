# Write data to a .vtr file

For `vectra_node` inputs (lazy queries from any format: CSV, SQLite,
TIFF, or another .vtr), data is streamed batch-by-batch to disk without
materializing the full result in memory. Each batch becomes one row
group. The output file is written atomically (via temp file + rename) so
readers never see a partial file.

## Usage

``` r
write_vtr(x, path, ...)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path for the output .vtr file.

- ...:

  Additional arguments passed to methods.

## Value

Invisible `NULL`.

## Details

For `data.frame` inputs, the data is written directly from memory.

## Examples

``` r
# From a data.frame
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

# Streaming format conversion (CSV -> VTR)
csv <- tempfile(fileext = ".csv")
write.csv(mtcars, csv, row.names = FALSE)
f2 <- tempfile(fileext = ".vtr")
tbl_csv(csv) |> write_vtr(f2)

unlink(c(f, f2, csv))
```
