# vectra — Notes for AI Coding Agents

vectra is a self-contained R columnar query engine (pure C11 backend) for
larger-than-RAM data. It exposes dplyr-style verbs that build a lazy plan; the
plan executes only when `collect()` is called. Sources include vectra's native
`.vtr` format, CSV(.gz), SQLite, Excel (.xlsx), and GeoTIFF.

## Core workflow

```r
library(vectra)

# 1. Open a source -> lazy `vectra_node`
node <- tbl_csv("measurements.csv")

# 2. Compose verbs (still lazy)
q <- node |>
  filter(temperature > 30, year >= 2020) |>
  group_by(station) |>
  summarise(avg_temp = mean(temperature), n = n())

# 3. Execute
df <- collect(q)        # returns a data.frame
explain(q)              # inspect optimized plan without executing
```

## Sources (all return a `vectra_node`)

- `tbl(path)` — `.vtr` file. Hash indexes (`.vtri` sidecars) are auto-loaded.
- `tbl_csv(path, batch_size = 65536)` — CSV / `.csv.gz`; types inferred from first 1000 rows.
- `tbl_sqlite(path, table, batch_size = 65536)` — SQLite, no `DBI` dependency.
- `tbl_xlsx(path, sheet = 1L, batch_size = 65536)` — requires `openxlsx2`; sheet is read fully into memory.
- `tbl_tiff(path, batch_size = 256)` — pixels become rows with columns `x`, `y`, `band1`, `band2`, ...

## Sinks

- `write_vtr(x, path, compress = c("fast","small","none"), batch_size = NULL, col_types = NULL, quantize = NULL, spatial = NULL)` — atomic write; streams from a node.
- `append_vtr(x, path)` — appends a row group; **schema (names, types, order) must match exactly**.
- `delete_vtr(path, row_ids)` and `diff_vtr(old_path, new_path, key_col)` — logical (key-based) diff, returns `list(added = vectra_node, deleted = key_vector)`.
- `write_csv(x, path)`, `write_sqlite(x, path, table)`.
- `write_tiff(x, path, compress = FALSE, pixel_type = "float64", metadata = NULL, crs = NULL, tiled = FALSE, tile_size = 256L, bigtiff = "auto")` — `compress` is logical; `pixel_type` ∈ `"int8"`, `"int16"`, `"int32"`, `"uint8"`, `"uint16"`, `"float32"`, `"float64"`; `tile_size` must be a multiple of 16; `crs` accepts an integer EPSG, `"EPSG:4326"`, or `list(epsg=, citation=)`.

## Star schemas

```r
s <- vtr_schema(
  fact    = tbl("obs.vtr"),
  species = link("sp_id", tbl("species.vtr")),     # unnamed key = same name in both
  site    = link(c(site_id = "id"), tbl("sites.vtr"))  # named key = remap
)
lookup(s, value, species$name, site$habitat, .join = "left", .report = TRUE) |> collect()
```

`link()` and `vtr_schema()` accept **only file-backed nodes** (created by
`tbl()` / `tbl_csv()` / `tbl_sqlite()`). Nodes that come out of verbs have no
file path and will be rejected.

## Indexes and materialized blocks

- `create_index(path, column, ci = FALSE)` writes a `.vtri` sidecar; pass a character vector for composite indexes. `has_index(path, column)` checks.
- `materialize(node)` returns a `vectra_block` (in memory, reusable). Probe with `block_lookup(block, column, keys, ci = FALSE)` or `block_fuzzy_lookup(block, column, keys, method = c("dl","levenshtein","jw"), max_dist = 0.2, block_col = NULL, block_keys = NULL, n_threads = 4L)`.

## Out-of-core model fitting and per-group work

The engine streams, so a result larger than memory reduces to a summary in one
pass, and per-group work splits into shards that each fit in memory.

- `collect_chunked(x, f, .init = NULL, combine = NULL, commutative = FALSE)` — folds `f(acc, chunk)` left-to-right over every batch of a node, holding one batch plus the accumulator. On a `vectra_partition` it folds each shard then merges the per-shard accumulators with `combine`. Use for running counts, per-group sufficient statistics, or the `X'X` / `X'y` cross-products behind an exact OLS fit.
- `chunk_feeder(.source)` — wraps a query as a resettable generator following the `data(reset = TRUE/FALSE)` protocol `biglm::bigglm()` expects. `.source` is either a factory (`function()` returning a fresh node) or an `offload()`ed node (replays from disk). Drives a GLM fit on larger-than-RAM data.
- `offload(x, by = NULL, n = NULL, method = c("auto","level","range","hash"), path = NULL, compress = c("fast","small","none"))` — with no `by`, spills a prepared query to disk once and returns a `vectra_node` that replays from the file, so an iterative consumer re-reads the spill instead of rebuilding the pipeline. With a `by` key it returns a `vectra_partition`: a named list of per-shard nodes, one per key value.
- `group_map(.data, .f, ...)` — runs `.f` on each shard of a partition, passing the shard as a data.frame and its key as a string (purrr-style `~` formulas work: `.x` is the shard, `.y` the key). `.f` is arbitrary R: any per-group computation that needs the whole group in memory at once. Returns a named list keyed by shard.
- `group_modify(.data, .f, ...)` — same, but `.f` returns a data.frame; the per-shard results are row-bound into one table with the shard key restored as a column.

```r
# Streaming OLS: accumulate X'X and X'y in one pass, never holding the matrix
acc <- tbl("survey.vtr") |> select(mpg, wt, hp) |>
  collect_chunked(function(acc, chunk) {
    X <- cbind(1, chunk$wt, chunk$hp)
    list(XtX = acc$XtX + crossprod(X), Xty = acc$Xty + crossprod(X, chunk$mpg))
  }, .init = list(XtX = matrix(0, 3, 3), Xty = matrix(0, 3, 1)))
solve(acc$XtX, acc$Xty)

# Out-of-core GLM: prepare once, then bigglm re-reads the spill on every pass
s <- offload(tbl("occ.vtr") |> select(presence, bio1, bio12))
biglm::bigglm(presence ~ bio1 + bio12, data = chunk_feeder(s), family = binomial())

# Per-key shards: group_map runs any function on each in-memory shard (here a GLM)
p <- offload(tbl("occ.vtr"), by = "region")
fits <- group_map(p, function(d, region)
  glm(presence ~ bio1 + bio12, data = d, family = binomial()))
```

## Constraints and common mistakes

- Verbs do nothing until a terminal runs them: `collect()`, `collect_chunked()`, `pull()`, `reframe()`, `glimpse()`, `explain()`, `group_map()` / `group_modify()`, or a `write_*()`.
- `mean(x, na.rm = TRUE)` etc. — engine ignores `na.rm` at the moment; NA propagation follows R semantics by default.
- TIFF rows always carry lowercase `x`, `y`, `band1`, `band2`. Reconstruct rasters with `terra::rast(df, type = "xyz")`.
- `tiff_extract_points(path, x, y)` accepts either two numeric vectors **or** a data.frame/matrix with columns named `x` and `y`.
- `lookup(s, ...)` uses `dim$col` syntax (it parses the `$` call), not `dim.col`.
- `summarize` is an alias of `summarise` — both are exported.
- `tbl_xlsx` is **not streaming**; the whole sheet is loaded. Use `tbl_csv` or convert to `.vtr` for large data.
- `append_vtr` is not fully atomic. For safety-critical writes, use `write_vtr` (which is).

## Use cases / keywords

ETL across CSV/SQLite/TIFF/.vtr; out-of-core dplyr; star-schema lookups;
streaming aggregations; out-of-core OLS/GLM fitting (`collect_chunked` and
`chunk_feeder` + `biglm`); per-key sharding for any per-group function
(`offload(by=)` + `group_map`); hash + zone-map predicate pushdown; fuzzy string
matching (Levenshtein / Damerau-Levenshtein / Jaro-Winkler) inside the engine;
GeoTIFF point sampling without `terra`; integer raster output with quantization
and spatial predictor encoding.
