# Create a lazy table reference from Parquet files

Streams one or more Apache Parquet files as a single lazy table. The
reader is native: no Arrow installation is needed. Only the columns a
query uses are read from disk, and a
[`filter()`](https://gillescolling.com/vectra/reference/filter.md)
directly above the scan skips row groups whose footer statistics (min,
max, null count) show that no row can match. Row groups are decoded one
page at a time into batches of at most `batch_size` rows, so a row group
larger than RAM still streams. No data is read until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_parquet(path, batch_size = .DEFAULT_BATCH_SIZE, list_sep = ";")
```

## Arguments

- path:

  Path to a Parquet file, a character vector of files, or a directory of
  Parquet files.

- batch_size:

  Maximum number of rows per batch (default 65536).

- list_sep:

  Separator placed between the elements of a list column (default
  `";"`).

## Value

A `vectra_node` object representing a lazy scan of the files.

## Details

`path` may name several files, or a directory. A directory is read
recursively; files whose name starts with `_` or `.` (Spark/Hive markers
such as `_SUCCESS`, checksum files) are skipped, so a partitioned
dataset directory or a GBIF snapshot folder can be passed as is. All
files must share the first file's columns and types; extra columns in
later files are ignored.

**Supported.** Data pages v1 and v2; `PLAIN`, `PLAIN_DICTIONARY` /
`RLE_DICTIONARY`, `RLE`, `DELTA_BINARY_PACKED`,
`DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY` and `BYTE_STREAM_SPLIT`
encodings; `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD`, `LZ4_RAW` and
(Hadoop-framed) `LZ4` compression. `BROTLI` and `LZO` are not supported
and raise an error naming the column.

**Types.** Booleans become logical; `INT32` (and 8/16-bit and unsigned
8/16-bit integers) becomes integer; unsigned 32-bit integers and `INT64`
become 64-bit integers (returned as double); unsigned 64-bit integers,
`FLOAT`, `DOUBLE`, decimals of any width and half floats become double;
`DATE` becomes `Date`; `TIMESTAMP` and legacy `INT96` timestamps become
`POSIXct` in seconds (UTC when the file marks them adjusted to UTC,
otherwise without a time zone), with nanosecond timestamps rounded to
double precision; `TIME` becomes seconds since midnight as double; byte
arrays (strings, JSON, ENUM, and unannotated binary) become character,
and UUIDs their canonical text form.

**Nested columns.** Struct fields are flattened into one column each,
named by their path (`address.city`). A list column (one level of
repetition, including a map's keys and values) becomes one character
column whose elements are written as text and joined by `list_sep`: a
null list is `NA`, an empty list `""`, a null element `"NA"`. Lists of
lists cannot be read; such a column stays in the schema, and reading it
raises an error naming it, so drop it with `select(-name)` first.

## See also

[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
[`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
[`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)

## Examples

``` r
f <- system.file("extdata", "example.parquet", package = "vectra")
tbl_parquet(f) |> collect() |> head()
#>   id          species dbh_cm   surveyed
#> 1  1    Quercus robur   10.0 2020-01-01
#> 2  2  Fagus sylvatica   17.3 2020-01-12
#> 3  3      Picea abies   24.6 2020-01-23
#> 4  4 Pinus sylvestris   31.9 2020-02-03
#> 5  5       Abies alba   39.2 2020-02-14
#> 6  6    Quercus robur   46.5 2020-02-25
tbl_parquet(f) |> filter(id > 95) |> select(id, species) |> collect()
#>    id          species
#> 1  96    Quercus robur
#> 2  97  Fagus sylvatica
#> 3  98      Picea abies
#> 4  99 Pinus sylvestris
#> 5 100       Abies alba
```
