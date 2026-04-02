# Package index

## Data sources

Open files for lazy query execution

- [`tbl()`](https://gcol33.github.io/vectra/reference/tbl.md) : Create a
  lazy table reference from a .vtr file
- [`tbl_csv()`](https://gcol33.github.io/vectra/reference/tbl_csv.md) :
  Create a lazy table reference from a CSV file
- [`tbl_sqlite()`](https://gcol33.github.io/vectra/reference/tbl_sqlite.md)
  : Create a lazy table reference from a SQLite database
- [`tbl_tiff()`](https://gcol33.github.io/vectra/reference/tbl_tiff.md)
  : Create a lazy table reference from a GeoTIFF raster

## Data sinks

Write query results or data.frames to disk

- [`write_vtr()`](https://gcol33.github.io/vectra/reference/write_vtr.md)
  : Write a data.frame to a .vtr file
- [`write_csv()`](https://gcol33.github.io/vectra/reference/write_csv.md)
  : Write query results or a data.frame to a CSV file
- [`write_sqlite()`](https://gcol33.github.io/vectra/reference/write_sqlite.md)
  : Write query results or a data.frame to a SQLite table
- [`write_tiff()`](https://gcol33.github.io/vectra/reference/write_tiff.md)
  : Write query results to a GeoTIFF file

## Single-table verbs

Transform, filter, and reshape

- [`filter()`](https://gcol33.github.io/vectra/reference/filter.md) :
  Filter rows of a vectra query
- [`select()`](https://gcol33.github.io/vectra/reference/select.md) :
  Select columns from a vectra query
- [`mutate()`](https://gcol33.github.io/vectra/reference/mutate.md) :
  Add or transform columns
- [`transmute()`](https://gcol33.github.io/vectra/reference/transmute.md)
  : Keep only columns from mutate expressions
- [`rename()`](https://gcol33.github.io/vectra/reference/rename.md) :
  Rename columns
- [`relocate()`](https://gcol33.github.io/vectra/reference/relocate.md)
  : Relocate columns
- [`arrange()`](https://gcol33.github.io/vectra/reference/arrange.md) :
  Sort rows by column values
- [`desc()`](https://gcol33.github.io/vectra/reference/desc.md) : Mark a
  column for descending sort order
- [`distinct()`](https://gcol33.github.io/vectra/reference/distinct.md)
  : Keep distinct/unique rows
- [`slice_head()`](https://gcol33.github.io/vectra/reference/slice_head.md)
  [`slice_tail()`](https://gcol33.github.io/vectra/reference/slice_head.md)
  [`slice_min()`](https://gcol33.github.io/vectra/reference/slice_head.md)
  [`slice_max()`](https://gcol33.github.io/vectra/reference/slice_head.md)
  : Select first or last rows
- [`pull()`](https://gcol33.github.io/vectra/reference/pull.md) :
  Extract a single column as a vector
- [`head(`*`<vectra_node>`*`)`](https://gcol33.github.io/vectra/reference/head.vectra_node.md)
  : Limit results to first n rows

## Grouping and aggregation

- [`group_by()`](https://gcol33.github.io/vectra/reference/group_by.md)
  : Group a vectra query by columns
- [`summarise()`](https://gcol33.github.io/vectra/reference/summarise.md)
  [`summarize()`](https://gcol33.github.io/vectra/reference/summarise.md)
  : Summarise grouped data
- [`ungroup()`](https://gcol33.github.io/vectra/reference/ungroup.md) :
  Remove grouping from a vectra query
- [`count()`](https://gcol33.github.io/vectra/reference/count.md)
  [`tally()`](https://gcol33.github.io/vectra/reference/count.md) :
  Count observations by group
- [`reframe()`](https://gcol33.github.io/vectra/reference/reframe.md) :
  Summarise with variable-length output per group
- [`across()`](https://gcol33.github.io/vectra/reference/across.md) :
  Apply a function across multiple columns

## Joins

- [`left_join()`](https://gcol33.github.io/vectra/reference/left_join.md)
  [`inner_join()`](https://gcol33.github.io/vectra/reference/left_join.md)
  [`right_join()`](https://gcol33.github.io/vectra/reference/left_join.md)
  [`full_join()`](https://gcol33.github.io/vectra/reference/left_join.md)
  [`semi_join()`](https://gcol33.github.io/vectra/reference/left_join.md)
  [`anti_join()`](https://gcol33.github.io/vectra/reference/left_join.md)
  : Join two vectra tables

## Combining tables

- [`bind_rows()`](https://gcol33.github.io/vectra/reference/bind_rows.md)
  [`bind_cols()`](https://gcol33.github.io/vectra/reference/bind_rows.md)
  : Bind rows or columns from multiple vectra tables

## Diagnostics

- [`explain()`](https://gcol33.github.io/vectra/reference/explain.md) :
  Print the execution plan for a vectra query
- [`collect()`](https://gcol33.github.io/vectra/reference/collect.md) :
  Execute a lazy query and return a data.frame
- [`print(`*`<vectra_node>`*`)`](https://gcol33.github.io/vectra/reference/print.vectra_node.md)
  : Print a vectra query node
