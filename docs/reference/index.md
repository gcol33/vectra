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
- [`tbl_xlsx()`](https://gillescolling.com/vectra/reference/tbl_xlsx.md)
  : Create a lazy table reference from an Excel (.xlsx) file

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

## GeoTIFF helpers

Metadata and point-sampling for GeoTIFF rasters

- [`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
  : Extract raster values at point coordinates
- [`tiff_metadata()`](https://gillescolling.com/vectra/reference/tiff_metadata.md)
  : Read GDAL_METADATA from a GeoTIFF
- [`tiff_band_names()`](https://gillescolling.com/vectra/reference/tiff_band_names.md)
  : Read per-band names from a GeoTIFF
- [`tiff_crs()`](https://gillescolling.com/vectra/reference/tiff_crs.md)
  : Read CRS metadata from a GeoTIFF

## Raster data cubes

Read and write .vec rasters and time cubes out of core

- [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
  : Open a .vec raster
- [`vec_close_raster()`](https://gillescolling.com/vectra/reference/vec_close_raster.md)
  : Close a .vec raster handle
- [`vec_raster_layout()`](https://gillescolling.com/vectra/reference/vec_raster_layout.md)
  : Tile layout of an open .vec raster
- [`vec_raster_times()`](https://gillescolling.com/vectra/reference/vec_raster_times.md)
  : Distinct time stamps stored in a .vec time cube
- [`vec_read_window()`](https://gillescolling.com/vectra/reference/vec_read_window.md)
  : Read a window of pixels from a .vec raster
- [`vec_read_time_slice()`](https://gillescolling.com/vectra/reference/vec_read_time_slice.md)
  : Read a single time slice from a .vec time cube
- [`vec_read_pixel_series()`](https://gillescolling.com/vectra/reference/vec_read_pixel_series.md)
  : Read the full time series at a single pixel from a .vec time cube
- [`vec_extract_points()`](https://gillescolling.com/vectra/reference/vec_extract_points.md)
  : Extract band values at (x, y) points from a .vec raster
- [`vec_build_overviews()`](https://gillescolling.com/vectra/reference/vec_build_overviews.md)
  : Build overview pyramids for a .vec raster
- [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)
  : Write a raster matrix or 3D array to a .vec raster file
- [`vec_write_time_cube()`](https://gillescolling.com/vectra/reference/vec_write_time_cube.md)
  : Write a 4D time-cube raster to .vec
- [`vec_to_tiff()`](https://gillescolling.com/vectra/reference/vec_to_tiff.md)
  : Export a .vec raster to GeoTIFF

## File operations

Incremental updates to .vtr files

- [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  : Append rows to an existing .vtr file
- [`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
  : Logically delete rows from a .vtr file
- [`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md)
  : Compute the logical diff between two .vtr files
- [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  : Create a hash index on a .vtr file column
- [`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
  : Check if a hash index exists for a .vtr column

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
- [`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
  : Fuzzy join two vectra tables by string distance

## Star schema

Define linked dimension tables and look up columns without explicit
joins

- [`vtr_schema()`](https://gillescolling.com/vectra/reference/vtr_schema.md)
  : Create a star schema over linked vectra tables
- [`link()`](https://gillescolling.com/vectra/reference/link.md) :
  Define a link between a fact table and a dimension table
- [`lookup()`](https://gillescolling.com/vectra/reference/lookup.md) :
  Look up columns from linked dimension tables

## Combining tables

- [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  [`bind_cols()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  : Bind rows or columns from multiple vectra tables

## Block operations

In-memory materialization and lookup

- [`materialize()`](https://gillescolling.com/vectra/reference/materialize.md)
  : Materialize a vectra node into a reusable in-memory block
- [`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
  : Probe a materialized block by column value
- [`block_fuzzy_lookup()`](https://gillescolling.com/vectra/reference/block_fuzzy_lookup.md)
  : Fuzzy-match query keys against a materialized block

## Streaming consumption

Reduce or feed a query one batch at a time, for larger-than-RAM results

- [`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
  : Fold a function over a query, one batch at a time
- [`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
  : Turn a query into a resettable chunk generator

## Offloading

Spill a query to disk and stream it back, for out-of-core fits

- [`offload()`](https://gillescolling.com/vectra/reference/offload.md) :
  Spill a query to disk and stream it back (the offload functor)
- [`group_map()`](https://gillescolling.com/vectra/reference/group_map.md)
  [`group_modify()`](https://gillescolling.com/vectra/reference/group_map.md)
  : Apply a function to each shard of a partition

## Diagnostics

- [`explain()`](https://gillescolling.com/vectra/reference/explain.md) :
  Print the execution plan for a vectra query
- [`collect()`](https://gillescolling.com/vectra/reference/collect.md) :
  Execute a lazy query and return a data.frame
- [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md) :
  Get a glimpse of a vectra table
- [`print(`*`<vectra_node>`*`)`](https://gillescolling.com/vectra/reference/print.vectra_node.md)
  : Print a vectra query node
