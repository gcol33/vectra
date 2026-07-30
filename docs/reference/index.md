# Package index

## Data sources

Open files for lazy query execution

- [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) : Create
  a lazy table reference from a .vtr file
- [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md) :
  Create a lazy table reference from a delimited text file
- [`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)
  : Create a lazy table reference from a SQLite database
- [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md)
  : Create a lazy table reference from a GeoTIFF raster
- [`tbl_xlsx()`](https://gillescolling.com/vectra/reference/tbl_xlsx.md)
  : Create a lazy table reference from an Excel (.xlsx) file
- [`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md)
  : Create a lazy table reference from a FASTA file
- [`tbl_fastq()`](https://gillescolling.com/vectra/reference/tbl_fastq.md)
  : Create a lazy table reference from a FASTQ file
- [`tbl_bed()`](https://gillescolling.com/vectra/reference/tbl_bed.md) :
  Create a lazy table reference from a BED file

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
- [`st_write(`*`<vectra_node>`*`)`](https://gillescolling.com/vectra/reference/st_write.vectra_node.md)
  : Stream a vectra node's geometry to a vector file

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

## Time series

Snap datetimes to a calendar grid and aggregate over time windows

- [`floor_time()`](https://gillescolling.com/vectra/reference/floor_time.md)
  : Floor a datetime column to a calendar grid
- [`resample()`](https://gillescolling.com/vectra/reference/resample.md)
  : Resample a time series to a calendar grid
- [`roll_sum()`](https://gillescolling.com/vectra/reference/rolling.md)
  [`roll_mean()`](https://gillescolling.com/vectra/reference/rolling.md)
  [`roll_min()`](https://gillescolling.com/vectra/reference/rolling.md)
  [`roll_max()`](https://gillescolling.com/vectra/reference/rolling.md)
  [`roll_n()`](https://gillescolling.com/vectra/reference/rolling.md) :
  Time-based rolling aggregates

## Biological sequences

k-mer spectra and per-row sequence functions

- [`kmer()`](https://gillescolling.com/vectra/reference/kmer.md) : k-mer
  spectrum of a sequence column
- [`seq_expressions`](https://gillescolling.com/vectra/reference/seq_expressions.md)
  : Biological-sequence functions inside mutate(), filter(), and
  summarise()

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
- [`interval_join()`](https://gillescolling.com/vectra/reference/interval_join.md)
  : Interval (range overlap) join of two vectra tables

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

## File operations

Incremental updates to .vtr files

- [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  : Append rows or columns to an existing .vtr file
- [`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
  : Logically delete rows from a .vtr file
- [`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md)
  : Compute the logical diff between two .vtr files
- [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  : Create a hash index on a .vtr file column
- [`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
  : Check whether a .vtr column has a usable hash index

## Diagnostics

- [`explain()`](https://gillescolling.com/vectra/reference/explain.md) :
  Print the execution plan for a vectra query
- [`collect()`](https://gillescolling.com/vectra/reference/collect.md) :
  Execute a lazy query and return a data.frame
- [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md) :
  Get a glimpse of a vectra table
- [`dim(`*`<vectra_node>`*`)`](https://gillescolling.com/vectra/reference/dim.vectra_node.md)
  : Dimensions of a lazy query
- [`print(`*`<vectra_node>`*`)`](https://gillescolling.com/vectra/reference/print.vectra_node.md)
  : Print a vectra query node
- [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md)
  : Resolve the vectra memory budget

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

## Spatial operations

Stream sf vector operations one batch at a time, for vector data larger
than RAM

- [`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
  : Stream a query through an sf transform
- [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
  : Spatial join a streamed query against a resident sf object
- [`grid()`](https://gillescolling.com/vectra/reference/grid.md) :
  Define a uniform grid for a partitioned spatial join
- [`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md)
  : Keep streamed rows by their spatial relation to a resident layer
- [`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md)
  : Clip or erase a streamed layer against a resident mask
- [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
  : Dissolve geometries by group
- [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  : Self-overlay a polygon layer into disjoint pieces (QGIS-style Union)
- [`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md)
  : Rasterize a streamed point layer onto a fixed grid
- [`polygonize()`](https://gillescolling.com/vectra/reference/polygonize.md)
  : Vectorise a raster into polygons
- [`contours()`](https://gillescolling.com/vectra/reference/contours.md)
  : Extract contour iso-lines from a streamed raster
- [`zonal()`](https://gillescolling.com/vectra/reference/zonal.md) :
  Summarise raster values within zones
- [`focal()`](https://gillescolling.com/vectra/reference/focal.md) :
  Moving-window (focal) statistics over a streamed raster
- [`terrain()`](https://gillescolling.com/vectra/reference/terrain.md) :
  Terrain derivatives from a streamed elevation raster
- [`warp()`](https://gillescolling.com/vectra/reference/warp.md) :
  Resample or reproject a streamed raster onto a target grid
- [`mask()`](https://gillescolling.com/vectra/reference/mask.md) : Mask
  a streamed raster to a polygon layer
- [`mosaic()`](https://gillescolling.com/vectra/reference/mosaic.md) :
  Merge aligned rasters onto a common grid
- [`rast_calc()`](https://gillescolling.com/vectra/reference/rast_calc.md)
  : Cellwise calculation over aligned rasters (map algebra)
- [`proximity()`](https://gillescolling.com/vectra/reference/proximity.md)
  : Euclidean distance to the nearest feature (proximity)
- [`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
  : Materialize a spatial query as an sf object
- [`geom_expressions`](https://gillescolling.com/vectra/reference/geom_expressions.md)
  : Geometry functions inside mutate(), filter(), and summarise()

## Geometry construction and cleaning

Reshape, repair, simplify, and snap streamed vector geometry

- [`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
  : Build a set-wise geometry construction, optionally per group
- [`spatial_explode()`](https://gillescolling.com/vectra/reference/spatial_explode.md)
  : Explode multipart geometries into single-part features
- [`spatial_line_merge()`](https://gillescolling.com/vectra/reference/spatial_line_merge.md)
  : Merge contiguous line segments into maximal lines
- [`spatial_simplify()`](https://gillescolling.com/vectra/reference/spatial_simplify.md)
  : Simplify a polygon coverage without tearing shared edges
- [`spatial_smooth()`](https://gillescolling.com/vectra/reference/spatial_smooth.md)
  : Smooth streamed line and polygon geometry
- [`spatial_snap()`](https://gillescolling.com/vectra/reference/spatial_snap.md)
  : Snap a streamed layer toward a resident reference layer
- [`spatial_snap_grid()`](https://gillescolling.com/vectra/reference/spatial_snap_grid.md)
  : Snap a streamed layer's coordinates to a fixed grid
- [`spatial_eliminate()`](https://gillescolling.com/vectra/reference/spatial_eliminate.md)
  : Merge sliver polygons into a neighbour
- [`spatial_split()`](https://gillescolling.com/vectra/reference/spatial_split.md)
  : Split a streamed layer by a resident blade, or return its crossing
  points
- [`spatial_centerline()`](https://gillescolling.com/vectra/reference/spatial_centerline.md)
  : Trace the centerline (medial axis) of streamed polygons

## Coverage and topology

Shared-edge topology and polygonal faces of polygon coverages

- [`spatial_topology()`](https://gillescolling.com/vectra/reference/spatial_topology.md)
  : Build the shared-edge topology of a polygon coverage
- [`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md)
  : Build polygonal faces from a line network

## Spatial networks

Build routable graphs and solve shortest paths over line layers

- [`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
  : Build a routable network graph from a line layer
- [`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md)
  : Shortest paths and origin-destination costs over a network
- [`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)
  : Service areas and isochrones over a network

## Spatial queries

Nearest neighbours and linear referencing

- [`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md)
  : k nearest neighbours of a streamed layer, with distances
- [`spatial_locate()`](https://gillescolling.com/vectra/reference/spatial_locate.md)
  : Locate streamed points along a resident line layer

## Feature space

Embedding columns and nearest neighbours in predictor space

- [`as_embedding()`](https://gillescolling.com/vectra/reference/as_embedding.md)
  : Encode vectors as an embedding column
- [`cosine()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
  [`l2()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
  [`dot()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
  : Embedding distance functions
- [`feature_knn()`](https://gillescolling.com/vectra/reference/feature_knn.md)
  : Nearest neighbours of a streamed layer in predictor space
- [`rast_feature_distance()`](https://gillescolling.com/vectra/reference/rast_feature_distance.md)
  : Predictor-space nearest-neighbour distance surface over a raster
