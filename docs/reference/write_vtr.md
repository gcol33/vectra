# Write data to a .vtr file

For `vectra_node` inputs (lazy queries from any format: CSV, SQLite,
TIFF, or another .vtr), data is streamed batch-by-batch to disk without
materializing the full result in memory. Each batch becomes one row
group. The output file is written atomically (via temp file + rename) so
readers never see a partial file.

## Usage

``` r
write_vtr(
  x,
  path,
  compress = c("fast", "small", "none"),
  batch_size = NULL,
  col_types = NULL,
  quantize = NULL,
  spatial = NULL,
  ...
)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path for the output .vtr file.

- compress:

  Compression level: `"fast"` (default, byte-shuffle + greedy LZ),
  `"small"` (per-block adaptive — tries greedy LZ, separated-streams LZ,
  and LZ + Huffman entropy coding, and writes whichever shrank the block
  the most; never worse than `"fast"` on any block, typically 10-25
  percent smaller files at the cost of slower encode), or `"none"`.

- batch_size:

  Target number of rows per row group in the output file. Defaults to
  131072 for data.frames (1 MB per double column, cache-friendly for
  decompression). For nodes, defaults to `NULL` (one row group per
  upstream batch).

- col_types:

  Optional named character vector specifying narrow integer storage
  types. Names must match column names; values must be `"int8"`,
  `"int16"`, or `"int32"`. Only applies to integer columns. Example:
  `col_types = c(age = "int8", year = "int16")`.

- quantize:

  Optional named list for lossy quantization of `double` columns. Each
  element is named after a column and is itself a named list with
  `scale` (or `precision = 1/scale`), `type` (`"int8"`, `"int16"`,
  `"int32"`; default `"int16"`), and optionally `offset` (default 0).
  Example:
  `quantize = list(temp = list(precision = 0.001, type = "int16"))`.

- spatial:

  Optional list for 2D spatial predictor encoding. Either a global spec
  applied to all numeric columns (`list(nx = 2000, ny = 2000)`) or
  per-column specs (`list(temp = list(nx = 2000, ny = 2000))`). When
  provided, a spatial predictor removes smooth 2D trends before
  compression, dramatically improving compression of raster data.
  Combines with `quantize` for maximum effect.

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
