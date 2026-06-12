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

## Constraints and common mistakes

- Verbs do nothing until `collect()`, `pull()`, `explain()`, `glimpse()`, or a `write_*()` runs them.
- `mean(x, na.rm = TRUE)` etc. — engine ignores `na.rm` at the moment; NA propagation follows R semantics by default.
- TIFF rows always carry lowercase `x`, `y`, `band1`, `band2`. Reconstruct rasters with `terra::rast(df, type = "xyz")`.
- `tiff_extract_points(path, x, y)` accepts either two numeric vectors **or** a data.frame/matrix with columns named `x` and `y`.
- `lookup(s, ...)` uses `dim$col` syntax (it parses the `$` call), not `dim.col`.
- `summarize` is an alias of `summarise` — both are exported.
- `tbl_xlsx` is **not streaming**; the whole sheet is loaded. Use `tbl_csv` or convert to `.vtr` for large data.
- `append_vtr` is not fully atomic. For safety-critical writes, use `write_vtr` (which is).

## Use cases / keywords

ETL across CSV/SQLite/TIFF/.vtr; out-of-core dplyr; star-schema lookups;
streaming aggregations; hash + zone-map predicate pushdown; fuzzy string
matching (Levenshtein / Damerau-Levenshtein / Jaro-Winkler) inside the engine;
GeoTIFF point sampling without `terra`; integer raster output with quantization
and spatial predictor encoding.
