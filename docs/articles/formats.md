# Format Backends

## Introduction

vectra reads and writes five formats: its native `.vtr`, CSV, SQLite,
Excel, and GeoTIFF. Each reader returns the same `vectra_node` object.
Once a node exists, the engine treats it identically regardless of where
the data came from. A filter on a CSV scan produces the same plan tree
as a filter on a `.vtr` scan. The same verbs work, the same expressions
evaluate, and
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
materializes an R data.frame either way.

This matters because format choice becomes a deployment decision, not a
code decision. We can prototype a pipeline against a CSV export, convert
the data to `.vtr` for production, and the pipeline code stays the same
apart from the opening `tbl_*()` call.

Writers mirror the readers.
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
[`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md),
[`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md),
and
[`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md)
are all S3 generics dispatching on both `vectra_node` and `data.frame`.
When the input is a node, writes stream batch-by-batch through the plan
tree. The full dataset never needs to fit in memory. This is the core of
vectra’s format conversion story: pipe a reader into a writer, and data
flows from source format to target format in bounded memory.

``` r

library(vectra)
#> 
#> Attaching package: 'vectra'
#> The following object is masked from 'package:stats':
#> 
#>     filter
#> The following object is masked from 'package:graphics':
#> 
#>     grid

# Write mtcars to .vtr, then read it back lazily
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
node <- tbl(f)
node
#> vectra query node
#> Columns (11):
#>   mpg <double>
#>   cyl <double>
#>   disp <double>
#>   hp <double>
#>   drat <double>
#>   wt <double>
#>   qsec <double>
#>   vs <double>
#>   am <double>
#>   gear <double>
#>   carb <double>
```

The node prints its schema but holds no data. Columns, types, and row
count are in the file header; the actual values stay on disk until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).

## The .vtr format

`.vtr` is vectra’s native binary columnar format. It stores data in row
groups, where each row group contains all columns for a slice of rows.
The current version (v4) applies a two-stage encoding stack per column
per row group: first a logical encoding (dictionary for low-cardinality
strings, delta for monotonic integers, or plain pass-through), then
byte-level compression via Zstandard when it actually shrinks the data.

This layout gives the scan node several optimization paths that other
formats cannot support. Zone-map statistics (min/max per column per row
group) let the engine skip entire row groups that cannot match a filter
predicate. Column pruning avoids reading columns that the query never
references. And hash indexes (`.vtri` sidecar files) enable O(1) row
group lookup on equality predicates.

``` r

f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

tbl(f) |>
  filter(cyl == 6) |>
  select(mpg, cyl, hp) |>
  collect()
#>    mpg cyl  hp
#> 1 21.0   6 110
#> 2 21.0   6 110
#> 3 21.4   6 110
#> 4 18.1   6 105
#> 5 19.2   6 123
#> 6 17.8   6 123
#> 7 19.7   6 175
```

When we call
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
with a `vectra_node` input, the writer streams batches from the upstream
plan and writes each batch as one row group. The full result never
materializes in memory. For `data.frame` inputs, the data is written
directly from R’s memory in a single row group.

The `batch_size` parameter on
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
controls how many rows accumulate before flushing a row group. Smaller
row groups mean more granular zone-map statistics and finer predicate
pruning. Larger row groups reduce per-group overhead and compress
better.

``` r

f <- tempfile(fileext = ".vtr")
csv <- tempfile(fileext = ".csv")
write.csv(mtcars, csv, row.names = FALSE)

# Convert CSV to .vtr with 10-row row groups
tbl_csv(csv) |> write_vtr(f, batch_size = 10)

# The file now has multiple row groups
tbl(f) |> collect() |> nrow()
#> [1] 32
```

[`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
adds new row groups to an existing file without rewriting it. This is
useful for incremental data ingestion where new batches arrive over
time.

## CSV

[`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
opens a CSV file for streaming reads. Column types are inferred from the
first 1000 rows: numeric-looking columns become doubles, integer-
looking columns become integers, and everything else is a string. Gzip-
compressed files (`.csv.gz`) are detected and decompressed
transparently.

``` r

csv <- tempfile(fileext = ".csv")
write.csv(mtcars, csv, row.names = FALSE)

tbl_csv(csv) |>
  filter(hp > 200) |>
  select(mpg, hp, wt) |>
  collect()
#>    mpg  hp    wt
#> 1 14.3 245 3.570
#> 2 10.4 205 5.250
#> 3 10.4 215 5.424
#> 4 14.7 230 5.345
#> 5 13.3 245 3.840
#> 6 15.8 264 3.170
#> 7 15.0 335 3.570
```

The `batch_size` parameter controls how many rows the CSV scanner reads
per internal batch. The default (65536) works well for most files.
Smaller values reduce peak memory at the cost of more read calls.

[`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md)
streams from any node. It writes a standard comma-separated file with a
header row. There is no gzip output option; if we need compressed
output, we convert to `.vtr` instead, which applies Zstandard
automatically.

``` r

f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

out_csv <- tempfile(fileext = ".csv")
tbl(f) |>
  filter(cyl == 4) |>
  write_csv(out_csv)

# Verify the output
read.csv(out_csv) |> head()
#>    mpg cyl  disp hp drat    wt  qsec vs am gear carb
#> 1 22.8   4 108.0 93 3.85 2.320 18.61  1  1    4    1
#> 2 24.4   4 146.7 62 3.69 3.190 20.00  1  0    4    2
#> 3 22.8   4 140.8 95 3.92 3.150 22.90  1  0    4    2
#> 4 32.4   4  78.7 66 4.08 2.200 19.47  1  1    4    1
#> 5 30.4   4  75.7 52 4.93 1.615 18.52  1  1    4    2
#> 6 33.9   4  71.1 65 4.22 1.835 19.90  1  1    4    1
```

CSV has no predicate pushdown or column pruning. Every row and every
column is read and parsed, then the engine’s filter and project nodes
discard what the query does not need. For one-off analyses this is fine.
For repeated queries on the same data, converting to `.vtr` once and
querying many times is faster.

## SQLite

[`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)
connects to a SQLite database and streams a table through vectra’s
engine. Column types are inferred from the declared types in the
`CREATE TABLE` statement. All filtering, grouping, and aggregation
happen in vectra’s C engine, not via SQL queries. The SQLite library is
used only as a row source.

``` r

db <- tempfile(fileext = ".sqlite")
write_sqlite(mtcars, db, "cars")

tbl_sqlite(db, "cars") |>
  filter(mpg > 25) |>
  select(mpg, cyl, wt) |>
  collect()
#>    mpg cyl    wt
#> 1 32.4   4 2.200
#> 2 30.4   4 1.615
#> 3 33.9   4 1.835
#> 4 27.3   4 1.935
#> 5 26.0   4 2.140
#> 6 30.4   4 1.513
```

The `table` argument names which table to scan. A database can hold many
tables; each
[`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)
call opens one.

SQLite is a natural choice when data already lives in a database, or
when other tools (Python, command-line utilities, web applications) need
to read the same file. The format is self-contained, widely supported,
and handles concurrent reads well.

[`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md)
streams from any node into a SQLite table. If the table already exists,
rows are appended. This makes it straightforward to export vectra query
results for consumption by non-R tools.

``` r

f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

db <- tempfile(fileext = ".sqlite")
tbl(f) |>
  filter(cyl == 8) |>
  write_sqlite(db, "v8_cars")

# Read it back through vectra
tbl_sqlite(db, "v8_cars") |> collect()
#>     mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> 1  18.7   8 360.0 175 3.15 3.440 17.02  0  0    3    2
#> 2  14.3   8 360.0 245 3.21 3.570 15.84  0  0    3    4
#> 3  16.4   8 275.8 180 3.07 4.070 17.40  0  0    3    3
#> 4  17.3   8 275.8 180 3.07 3.730 17.60  0  0    3    3
#> 5  15.2   8 275.8 180 3.07 3.780 18.00  0  0    3    3
#> 6  10.4   8 472.0 205 2.93 5.250 17.98  0  0    3    4
#> 7  10.4   8 460.0 215 3.00 5.424 17.82  0  0    3    4
#> 8  14.7   8 440.0 230 3.23 5.345 17.42  0  0    3    4
#> 9  15.5   8 318.0 150 2.76 3.520 16.87  0  0    3    2
#> 10 15.2   8 304.0 150 3.15 3.435 17.30  0  0    3    2
#> 11 13.3   8 350.0 245 3.73 3.840 15.41  0  0    3    4
#> 12 19.2   8 400.0 175 3.08 3.845 17.05  0  0    3    2
#> 13 15.8   8 351.0 264 4.22 3.170 14.50  0  1    5    4
#> 14 15.0   8 301.0 335 3.54 3.570 14.60  0  1    5    8
```

## Excel

[`tbl_xlsx()`](https://gillescolling.com/vectra/reference/tbl_xlsx.md)
reads a sheet from an Excel workbook. It requires the `openxlsx2`
package, which is listed in Suggests. The sheet is specified by name or
by 1-based index (default: first sheet).

Unlike the other backends, Excel support is read-only. There is no
`write_xlsx()` function. Excel’s format is complex and writing it adds
little value when CSV or SQLite serve the same interoperability purpose
with less overhead.

Under the hood,
[`tbl_xlsx()`](https://gillescolling.com/vectra/reference/tbl_xlsx.md)
reads the sheet into a data.frame via `openxlsx2` and then converts it
to a vectra node. This means the sheet does load into memory during the
initial read, but all subsequent operations (filter, mutate, join) are
lazy and stream through the C engine.

``` r

# Not run (requires openxlsx2)
node <- tbl_xlsx("survey_results.xlsx", sheet = "Q1_2025")
node |>
  filter(score >= 80) |>
  select(respondent, score, region) |>
  collect()
```

For large Excel files that do not fit in memory, export to CSV first and
use
[`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md).

## GeoTIFF

[`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md)
reads raster data. Each pixel becomes a row with `x` and `y` columns
(pixel center coordinates derived from the geotransform) and one column
per band (`band1`, `band2`, etc.). NoData values become `NA`. This
tabular representation lets us apply the full verb set to raster data:
filter by spatial extent, threshold band values, compute derived
indices, join with point observations.

The default `batch_size` is 256 raster rows, much smaller than the
CSV/SQLite default. Raster data is dense. A 10,000 x 10,000 single-band
TIFF has 100 million pixels, so each raster row of 10,000 pixels already
constitutes a meaningful batch.

``` r

# Not run (requires a GeoTIFF file)
tbl_tiff("temperature.tif") |>
  filter(band1 > 25, x >= 10, x <= 20) |>
  collect()
```

[`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md)
writes query results back to a GeoTIFF. The data must contain `x` and
`y` columns and one or more numeric band columns. Grid dimensions and
the geotransform are inferred from the coordinate arrays. The `compress`
parameter enables DEFLATE compression.

### Integer pixel types

By default,
[`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md)
writes 64-bit float pixels. The `pixel_type` parameter selects a smaller
representation: `"int16"`, `"int32"`, `"uint8"`, `"uint16"`, or
`"float32"`. Smaller types compress better and produce much smaller
files — a global climate raster stored as `int16` + DEFLATE can be 5-10x
smaller than the equivalent `float64`.

``` r

# Scale temperature to integer: value * 100, stored as int16
df |>
  mutate(band1 = round(band1 * 100)) |>
  write_tiff("temperature_int16.tif", compress = TRUE, pixel_type = "int16")
```

`NA` values in the input are automatically mapped to the integer nodata
value (-32768 for `int16`, 65535 for `uint16`, etc.) and tagged in the
GDAL_NODATA header. The reader converts them back to `NA` on read.

### Embedded metadata

The `metadata` parameter writes a GDAL_METADATA XML string into TIFF tag
42112. This is useful for recording scale factors, offsets, variable
names, or provenance without a sidecar file.

``` r

xml <- '<GDALMetadata>
  <Item name="scale">0.01</Item>
  <Item name="offset">0</Item>
  <Item name="variable">bio1</Item>
</GDALMetadata>'

write_tiff(df, "bio1.tif", pixel_type = "int16", metadata = xml)

# Read the metadata back
tiff_metadata("bio1.tif")
```

[`tiff_metadata()`](https://gillescolling.com/vectra/reference/tiff_metadata.md)
returns the XML string, or `NA` if the tag is absent.

### Point extraction

[`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
samples band values at specific `(x, y)` coordinates directly from the
TIFF file. Only the strips containing query points are read, making it
efficient for sparse lookups on large rasters. No spatial package is
needed at runtime.

``` r

pts <- data.frame(x = c(10.5, 11.2), y = c(47.1, 47.3))
tiff_extract_points("temperature.tif", pts)
```

This round-trip capability makes vectra useful for raster ETL: read a
TIFF, filter or transform it with standard verbs, write a new TIFF. No
dependency on spatial packages is needed for the read-process-write loop
itself, though `terra::rast(df, type = "xyz")` can convert results to
`SpatRaster` objects when spatial operations are needed.

## Streaming conversion pipelines

The practical payoff of uniform node types is format conversion with
zero full-dataset materialization. We pipe a reader into a writer, and
data flows from source to target one batch at a time. Memory use stays
proportional to one batch, not the full dataset.

The simplest case: convert CSV to `.vtr`.

``` r

csv <- tempfile(fileext = ".csv")
write.csv(mtcars, csv, row.names = FALSE)

vtr <- tempfile(fileext = ".vtr")
tbl_csv(csv) |> write_vtr(vtr)

tbl(vtr) |> collect() |> head()
#>    mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
#> 4 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
#> 5 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
#> 6 18.1   6  225 105 2.76 3.460 20.22  1  0    3    1
```

We can filter during conversion. Only rows passing the predicate are
written.

``` r

csv <- tempfile(fileext = ".csv")
write.csv(mtcars, csv, row.names = FALSE)

vtr <- tempfile(fileext = ".vtr")
tbl_csv(csv) |>
  filter(mpg > 20) |>
  mutate(kpl = mpg * 0.425144) |>
  write_vtr(vtr)

tbl(vtr) |> collect()
#>     mpg cyl  disp  hp drat    wt  qsec vs am gear carb       kpl
#> 1  21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4  8.928024
#> 2  21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4  8.928024
#> 3  22.8   4 108.0  93 3.85 2.320 18.61  1  1    4    1  9.693283
#> 4  21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1  9.098082
#> 5  24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2 10.373514
#> 6  22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2  9.693283
#> 7  32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1 13.774666
#> 8  30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2 12.924378
#> 9  33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1 14.412382
#> 10 21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1  9.140596
#> 11 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1 11.606431
#> 12 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2 11.053744
#> 13 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2 12.924378
#> 14 21.4   4 121.0 109 4.11 2.780 18.60  1  1    4    2  9.098082
```

Multi-step ETL works the same way. Read from one format, transform,
write to another.

``` r

# CSV -> filter + transform -> SQLite
csv <- tempfile(fileext = ".csv")
write.csv(mtcars, csv, row.names = FALSE)

db <- tempfile(fileext = ".sqlite")
tbl_csv(csv) |>
  filter(cyl >= 6) |>
  select(mpg, cyl, hp, wt) |>
  mutate(power_weight = hp / wt) |>
  write_sqlite(db, "powerful_cars")

# SQLite -> VTR
vtr <- tempfile(fileext = ".vtr")
tbl_sqlite(db, "powerful_cars") |> write_vtr(vtr)

tbl(vtr) |> collect()
#>     mpg cyl  hp    wt power_weight
#> 1  21.0   6 110 2.620     41.98473
#> 2  21.0   6 110 2.875     38.26087
#> 3  21.4   6 110 3.215     34.21462
#> 4  18.7   8 175 3.440     50.87209
#> 5  18.1   6 105 3.460     30.34682
#> 6  14.3   8 245 3.570     68.62745
#> 7  19.2   6 123 3.440     35.75581
#> 8  17.8   6 123 3.440     35.75581
#> 9  16.4   8 180 4.070     44.22604
#> 10 17.3   8 180 3.730     48.25737
#> 11 15.2   8 180 3.780     47.61905
#> 12 10.4   8 205 5.250     39.04762
#> 13 10.4   8 215 5.424     39.63864
#> 14 14.7   8 230 5.345     43.03087
#> 15 15.5   8 150 3.520     42.61364
#> 16 15.2   8 150 3.435     43.66812
#> 17 13.3   8 245 3.840     63.80208
#> 18 19.2   8 175 3.845     45.51365
#> 19 15.8   8 264 3.170     83.28076
#> 20 19.7   6 175 2.770     63.17690
#> 21 15.0   8 335 3.570     93.83754
```

Each arrow in the pipeline is a streaming connection. The CSV scanner
reads a batch, the filter discards rows, the project computes new
columns, and the SQLite writer inserts the result. Then the next batch
flows through. At no point does the full dataset exist in memory. For
datasets that dwarf available RAM, this is the only way these
conversions can work.

Conversion pipelines also compose with joins. We can read two sources in
different formats and join them.

``` r

f1 <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".csv")

cars_main <- mtcars[, c("mpg", "cyl", "hp")]
cars_extra <- data.frame(cyl = c(4, 6, 8), label = c("small", "mid", "big"))

write_vtr(cars_main, f1)
write.csv(cars_extra, f2, row.names = FALSE)

tbl(f1) |>
  left_join(tbl_csv(f2), by = "cyl") |>
  collect() |>
  head()
#>    mpg cyl  hp label
#> 1 21.0   6 110   mid
#> 2 21.0   6 110   mid
#> 3 22.8   4  93 small
#> 4 21.4   6 110   mid
#> 5 18.7   8 175   big
#> 6 18.1   6 105   mid
```

## Batch size

The `batch_size` parameter appears on readers and on
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md).
It controls different things depending on context.

On **readers** (`tbl_csv`, `tbl_sqlite`), batch size sets how many rows
the scanner reads per pull from the source. The default 65536 balances
memory use against per-call overhead. Larger values read more rows per
call, which can be faster for simple scan-heavy queries. Smaller values
keep peak memory lower, which matters when individual rows are wide
(many columns or long strings).

On
**[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)**,
batch size controls how many rows accumulate before flushing a row group
to disk. This directly affects the structure of the output file. Each
row group carries its own zone-map statistics (min and max per column),
so more row groups mean finer-grained predicate pushdown. Fewer row
groups mean less metadata overhead and better compression ratios,
because the compressor sees more data per block.

``` r

csv <- tempfile(fileext = ".csv")
big <- data.frame(
  id = seq_len(1000),
  value = rnorm(1000)
)
write.csv(big, csv, row.names = FALSE)

# Small row groups: more granular zone maps
f_small <- tempfile(fileext = ".vtr")
tbl_csv(csv) |> write_vtr(f_small, batch_size = 100)

# Default: single row group for 1000 rows
f_default <- tempfile(fileext = ".vtr")
tbl_csv(csv) |> write_vtr(f_default)

cat("Small batches:", file.size(f_small), "bytes\n")
#> Small batches: 8251 bytes
cat("Default:      ", file.size(f_default), "bytes\n")
#> Default:       8251 bytes
```

For `tbl_tiff`, the default batch size is 256 raster rows, reflecting
the high per-row density of raster data. Increasing it speeds up reads
on files with few bands. Decreasing it helps when many bands produce
very wide rows.

There is no batch size parameter on
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) (the `.vtr`
reader). Row groups are defined by the file; the scanner reads one row
group per `next_batch()` call, respecting whatever batch size was chosen
at write time.

## Format comparison

The table below summarizes what each format supports. The right format
depends on the workload: `.vtr` for repeated analytical queries, CSV for
interchange, SQLite for multi-tool access, Excel for one-off imports,
GeoTIFF for raster data.

| Feature               | .vtr         | CSV         | SQLite   | Excel  | GeoTIFF       |
|:----------------------|:-------------|:------------|:---------|:-------|:--------------|
| Streaming read        | yes          | yes         | yes      | no     | yes           |
| Streaming write       | yes          | yes         | yes      | –      | yes           |
| Predicate pushdown    | yes          | no          | no       | no     | no            |
| Column pruning        | yes          | no          | no       | no     | no            |
| Zone-map skip         | yes          | no          | no       | no     | no            |
| Hash index support    | yes          | no          | no       | no     | no            |
| Compression           | zstd         | gzip (read) | –        | –      | deflate       |
| Size on disk          | small        | large       | medium   | medium | variable      |
| Random access         | by row group | no          | by rowid | no     | by raster row |
| External tool support | vectra only  | universal   | wide     | wide   | GIS tools     |

`.vtr` wins on query performance because it is the only format where the
scanner can skip data before reading it. Zone maps, column pruning, and
hash indexes all reduce I/O. For a dataset queried repeatedly,
converting to `.vtr` once pays for itself quickly.

CSV and SQLite have broad tool support. If the data needs to flow to
Python, a dashboard, or a command-line tool, these formats avoid
lock-in. SQLite adds transactional writes and concurrent read access
that flat files lack.

GeoTIFF is specialized. It carries spatial metadata (coordinate
reference system, geotransform) that the other formats do not. When the
output is a raster, GeoTIFF is the right choice; when the output is a
table derived from raster data, `.vtr` or CSV may be more practical.
