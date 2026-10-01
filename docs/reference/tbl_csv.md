# Create a lazy table reference from a delimited text file

Opens a delimited text file (CSV, TSV, or any single-character
separator) for lazy, streaming query execution. Column types are
inferred from the first `guess_max` rows (default 1000). No data is read
until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called. Gzip-compressed files (`.csv.gz`, `.tsv.gz`) are supported
transparently.

## Usage

``` r
tbl_csv(
  path,
  batch_size = .DEFAULT_BATCH_SIZE,
  delim = ",",
  guess_max = 1000,
  col_types = NULL
)
```

## Arguments

- path:

  Path to a delimited text file, optionally gzip-compressed.

- batch_size:

  Number of rows per batch (default 65536).

- delim:

  Single-character field separator (default `","`). Use `"\t"` for
  tab-separated and `";"` for semicolon-separated files.

- guess_max:

  Number of rows scanned to infer column types (default 1000). A value
  that only reveals a column's true type later in the file (for example
  an integer column that turns out to hold a decimal past row 1000) is
  otherwise read as `NA`; pass a larger value, or `Inf` to scan the
  whole file, at the cost of an extra read pass.

- col_types:

  Optional named character vector forcing the type of specific columns,
  overriding inference. Names are column names; values are one of
  `"character"`, `"double"`, `"integer"`, or `"logical"`. Use this to
  keep a zero-padded identifier column (ZIP codes, accession IDs) as
  text instead of letting it be read as a number, e.g.
  `col_types = c(zip = "character")`.

## Value

A `vectra_node` object representing a lazy scan of the file.

## Details

The field separator is set by `delim`, so tab-separated files (GBIF
occurrence exports, TSV dumps) and semicolon-separated files (many
European exports) are read natively without a transcode step. Quoting
follows RFC 4180 for every delimiter: a field wrapped in double quotes
may contain the delimiter, newlines, and doubled quotes.

## Examples

``` r
f <- tempfile(fileext = ".csv")
write.csv(mtcars, f, row.names = FALSE)
node <- tbl_csv(f)
print(node)
#> vectra query node
#> Columns (11):
#>   mpg <double>
#>   cyl <int64>
#>   disp <double>
#>   hp <int64>
#>   drat <double>
#>   wt <double>
#>   qsec <double>
#>   vs <int64>
#>   am <int64>
#>   gear <int64>
#>   carb <int64>
unlink(f)

# Tab-separated (e.g. a GBIF occurrence export)
g <- tempfile(fileext = ".tsv")
write.table(mtcars, g, sep = "\t", row.names = FALSE, quote = FALSE)
tbl_csv(g, delim = "\t") |> collect() |> head()
#>    mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
#> 4 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
#> 5 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
#> 6 18.1   6  225 105 2.76 3.460 20.22  1  0    3    1
unlink(g)
```
