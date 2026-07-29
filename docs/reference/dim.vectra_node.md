# Dimensions of a lazy query

Reports the shape of a `vectra_node` from plan metadata, without running
the query. Defining [`dim()`](https://rdrr.io/r/base/dim.html) is what
makes base R's [`nrow()`](https://rdrr.io/r/base/nrow.html) and
[`ncol()`](https://rdrr.io/r/base/nrow.html) work on a node, since both
read `dim(x)`.

## Usage

``` r
# S3 method for class 'vectra_node'
dim(x)
```

## Arguments

- x:

  A `vectra_node` object.

## Value

A length-2 vector `c(rows, cols)`, integer unless the row count exceeds
the largest integer R can hold, in which case both are doubles. `rows`
is `NA` when the count is not derivable from metadata.

## Details

The column count always comes from the plan's schema. The row count is
available when it can be read from metadata: a `.vtr` table reports the
count stored in its row-group index (minus any rows
[`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
has tombstoned), and the row-preserving verbs carry it through –
[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`rename()`](https://gillescolling.com/vectra/reference/rename.md),
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
[`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
window functions, [`head()`](https://rdrr.io/r/utils/head.html),
[`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md),
[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)/[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md),
and
[`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
over counted inputs.

Verbs whose output length depends on the data –
[`filter()`](https://gillescolling.com/vectra/reference/filter.md), the
joins,
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
[`distinct()`](https://gillescolling.com/vectra/reference/distinct.md) –
report `NA` rows. Counting those means running the query, which on a
larger-than-RAM table is a full pass, so
[`nrow()`](https://rdrr.io/r/base/nrow.html) reports what it knows
rather than starting one. To get the exact count, run the query:

    tbl(f) |> filter(x > 0) |> count() |> collect()

A CSV, SQLite, or TIFF source reports `NA` rows too: those formats carry
no row count to read.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

dim(tbl(f))
nrow(tbl(f))                       # 32, straight from the row-group index
ncol(tbl(f) |> select(mpg, cyl))   # 2
nrow(tbl(f) |> head(5))            # 5
nrow(tbl(f) |> filter(cyl == 4))   # NA: needs the query to run

# exact count for a filtered query
tbl(f) |> filter(cyl == 4) |> count() |> collect()

unlink(f)
```
