# Package index

## Data sources

Open files for lazy query execution

- [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) : Create
  a lazy table reference from a .vtr file
- [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md) :
  Create a lazy table reference from a CSV file
- [`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)
  : Create a lazy table reference from a SQLite database
- [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md)
  : Create a lazy table reference from a GeoTIFF raster

## Data sinks

Write query results or data.frames to disk

- [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
  : Write data to a .vtr file
- [`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md)
  : Write query results or a data.frame to a CSV file
- [`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md)
  : Write query results or a data.frame to a SQLite table
- [`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md)
  : Write query results to a GeoTIFF file

## File operations

Incremental updates to .vtr files

- [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  : Append rows to an existing .vtr file
- [`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
  : Logically delete rows from a .vtr file
- [`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md)
  : Compute the logical diff between two .vtr files

## Single-table verbs

Transform, filter, and reshape

- [`filter()`](https://gillescolling.com/vectra/reference/filter.md) :
  Filter rows of a vectra query
- [`select()`](https://gillescolling.com/vectra/reference/select.md) :
  Select columns from a vectra query
- [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) :
  Add or transform columns
- [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
  : Keep only columns from mutate expressions
- [`rename()`](https://gillescolling.com/vectra/reference/rename.md) :
  Rename columns
- [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md)
  : Relocate columns
- [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md) :
  Sort rows by column values
- [`desc()`](https://gillescolling.com/vectra/reference/desc.md) : Mark
  a column for descending sort order
- [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md)
  : Keep distinct/unique rows
- [`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md)
  [`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md)
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  : Select first or last rows
- [`slice()`](https://gillescolling.com/vectra/reference/slice.md) :
  Select rows by position
- [`pull()`](https://gillescolling.com/vectra/reference/pull.md) :
  Extract a single column as a vector
- [`head(`*`<vectra_node>`*`)`](https://gillescolling.com/vectra/reference/head.vectra_node.md)
  : Limit results to first n rows

## Grouping and aggregation

- [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
  : Group a vectra query by columns
- [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  [`summarize()`](https://gillescolling.com/vectra/reference/summarise.md)
  : Summarise grouped data
- [`ungroup()`](https://gillescolling.com/vectra/reference/ungroup.md) :
  Remove grouping from a vectra query
- [`count()`](https://gillescolling.com/vectra/reference/count.md)
  [`tally()`](https://gillescolling.com/vectra/reference/count.md) :
  Count observations by group
- [`reframe()`](https://gillescolling.com/vectra/reference/reframe.md) :
  Summarise with variable-length output per group
- [`across()`](https://gillescolling.com/vectra/reference/across.md) :
  Apply a function across multiple columns

## Joins

- [`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  [`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  [`right_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  [`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  [`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  : Join two vectra tables
- [`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md)
  : Cross join two vectra tables

## Combining tables

- [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  [`bind_cols()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  : Bind rows or columns from multiple vectra tables

## Diagnostics

- [`explain()`](https://gillescolling.com/vectra/reference/explain.md) :
  Print the execution plan for a vectra query
- [`collect()`](https://gillescolling.com/vectra/reference/collect.md) :
  Execute a lazy query and return a data.frame
- [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md) :
  Get a glimpse of a vectra table
- [`print(`*`<vectra_node>`*`)`](https://gillescolling.com/vectra/reference/print.vectra_node.md)
  : Print a vectra query node
