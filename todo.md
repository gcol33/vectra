# vectra — TODO

## Incremental diff / append support

**Feature request:** `append_vtr(new_rows_df, path)` — write new rows as a new row group without recompressing or rewriting existing row groups.

### Background

`.vtr` uses per-column, per-rowgroup compression (independent blocks). This means the format is architecturally diff-friendly:

- **Adding rows**: a new row group can be appended to the end of the file — existing row groups are byte-identical and untouched
- **Modifying rows**: rewrite only the affected row groups (not the full file)
- **Deleting rows**: tombstone marker or row group rewrite

Currently, all updates require a full `collect() → write_vtr()` cycle, which recompresses everything even when only a small fraction of data changed.

### Why this matters

**Use case: taxify unified genus register**

taxify builds a cross-backend genus register (WFO ∪ COL ∪ GBIF genera, ~20–50k rows each) for hierarchical name matching. When a new backend is installed, only that backend's genera need to be merged in — but currently the entire register must be rebuilt from scratch.

With `append_vtr()`, installing a new backend = write one new row group, deduplicate on read. No full rebuild needed.

### Design options

**Option A — Simple append (additive only)**
```r
append_vtr(df, path)  # writes df as a new row group; no dedup
```
Caller handles deduplication by reading + filtering before append. Covers 95% of use cases (new backends add new genera, rarely modify existing).

**Option B — Delta log (full diff support)**
Immutable data files + a transaction log (like Delta Lake / Iceberg):
```
data/part-0001.vtr       ← immutable
data/part-0002.vtr       ← appended rows
_log/000001.json         ← "add part-0002, delete taxonID 5,6,7"
```
`tbl(path)` resolves the log transparently. Enables deletes and updates without touching compressed data. More complex but O(1) for all operations.

### Format notes (from source)

**v3** — no compression. Row groups are raw bytes. Append and physical delete are just byte writes/rewrites; no compression cost at all.

**v4** (`vtr_codec.h`) — per-column per-rowgroup encoding + custom LZ77 (`LZ_VTR`, ~120 lines, zero external deps). Three encoding passes before compression:
- `PLAIN` — raw bytes (doubles, bools)
- `DICTIONARY` — string columns with < 50% unique values
- `DELTA` — monotonically increasing int64 columns

Because each column-chunk is compressed independently, the diff boundary is one column × one row group:
- **Append**: new row group written at end — existing compressed chunks untouched, byte-identical
- **Logical delete** (tombstone side file): zero recompression
- **Physical delete**: decompress + filter + re-encode + re-compress the affected row group only — cost is `O(row_group_size)`, not `O(file_size)`

The format architecture already supports all three operations. Only the API is missing.
