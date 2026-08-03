# Append rows or columns to an existing .vtr file

Grows an existing `.vtr` store in place. `along = "rows"` (the default)
adds row groups to the end; `along = "cols"` attaches new columns to the
rows already there.

## Usage

``` r
append_vtr(
  x,
  path,
  along = c("rows", "cols"),
  compress = c("fast", "small", "none"),
  ...
)
```

## Arguments

- x:

  A `vectra_node` (lazy query) or a `data.frame`.

- path:

  File path of an existing `.vtr` file to append to.

- along:

  `"rows"` to add rows (default), `"cols"` to add columns.

- compress:

  Compression for the appended rows or columns: `"fast"`, `"small"`, or
  `"none"`.

- ...:

  Additional arguments passed to methods.

## Value

Invisible `NULL`.

## Appending rows

The schema of `x` must exactly match the schema of the target file (same
column names and types, in the same order). The row groups already in
the store are neither read nor rewritten – the new ones are encoded and
attached on their own – so the cost tracks the rows being appended
rather than the size of the store.

That is what lets a table too long to hold in memory be built a batch at
a time: write the first batch with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
then append each later one as it is produced, with a peak of one batch
rather than the whole table. Building a store this way costs one pass
over the rows written, however many calls it takes.

Existing row groups keep their positions, so any `.vtri` index built
with
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
stays valid across a row append: each one takes in the appended row
groups and keeps the rest, rather than being rebuilt from the whole
store.

## Appending columns

`x` supplies whole new columns for the rows already in the store: it
must have exactly as many rows as the store holds, and column names that
do not collide with the existing ones. The existing columns are never
read or rewritten – the new columns are encoded and attached on their
own – so the cost tracks what is being added rather than the size of the
store.

That is what lets a table too wide to hold in memory be built a block of
columns at a time: write the first block with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
then append each later block as it is produced, with a peak of one block
rather than the whole table. Row order is preserved, and the rows of `x`
are matched to the store's rows by position.

Existing row-group boundaries and column data are untouched, so any
`.vtri` index built with
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
over the original columns stays valid across a column append.

## Interruption

Either direction writes everything past the end of the existing data and
rewrites the file header last, so an interruption – a crash, a full
disk, a killed process – leaves the store readable exactly as it was
before the call. The appended bytes are referenced by nothing and the
next append writes over them.

The trade is that a store grown in place is stamped so that readers
predating this format refuse it rather than misread it, and is
random-access only, which is how vectra reads a `.vtr` anyway.

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars[1:10, ], f)
append_vtr(mtcars[11:20, ], f)
result <- tbl(f) |> collect()
stopifnot(nrow(result) == 20L)

# Attach two more columns to those same 20 rows
extra <- data.frame(kpl = result$mpg * 0.425,
                    heavy = result$wt > 3)
append_vtr(extra, f, along = "cols")
wide <- tbl(f) |> collect()
stopifnot(nrow(wide) == 20L, "kpl" %in% names(wide))
unlink(f)
```
