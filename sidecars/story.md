# Story: vectra

## Claim
A columnar query engine in C that runs dplyr-style queries on files larger than memory, one row group at a time.

## Audience
R users who know dplyr and have tables (CSV, Parquet, SQLite, GeoTIFF, vector layers) too large to load into memory.

## Relation
- to: dplyr (verb interface only; dplyr is not a dependency)
- phrase: dplyr-style queries
- same: verb names and semantics (`filter()`, `mutate()`, `group_by()`, `summarise()`, joins, window functions); tidyselect helpers
- differs: lazy plan executed by vectra's own C11 engine; nothing is read until `collect()`
- home: vignettes/engine.Rmd

## Arcs

### A1 Queries on files larger than memory
- question: can I query a file that does not fit in RAM?
- answer: a pull-based engine streams one row group through the plan; sorts, joins and aggregations spill to disk
- payoff: bounded peak memory whatever the file size
- evidence: vignettes/large-data.Rmd; tests
- home: vignettes/large-data.Rmd
- also: README

### A2 One pipeline for several formats
- question: do I need a different tool per file format?
- answer: `.vtr`, Parquet, CSV, SQLite and GeoTIFF open into the same lazy nodes
- home: vignettes/formats.Rmd
- also: README

### A3 Skipping what a query does not need
- question: why is a filter on a large file fast?
- answer: column pruning, row group statistics, optional hash indexes; `explain()` shows the plan
- home: vignettes/indexing.Rmd
- also: README

### A4 Spatial and raster data out of core
- question: can GIS operations stream too?
- answer: geometry as WKB columns, GEOS through libgeos, raster ops per tile
- home: vignettes/spatial.Rmd, vignettes/streaming-spatial.Rmd
- also: README

### A5 Model fits on out-of-core data
- question: how do I fit a model on a query result larger than memory?
- answer: `collect_chunked()`, `chunk_feeder()`, `offload()`
- home: vignettes/offload.Rmd
- also: README

## Non-goals
Not stated in the docs yet.

## Documents
- DESCRIPTION: claim, formats, spatial and raster scope
- README / landing: claim, list of arcs, installation (compiled, OpenMP), first query, routing
- Get started (quickstart.Rmd): one query from a file to a collected result
- articles: engine, formats, joins, schema, string-ops, indexing, large-data, offload, spatial, streaming-spatial, sdm

## Open
- Relation phrase to dplyr to be confirmed by the author.
