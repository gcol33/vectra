# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is vectra

An R package implementing a columnar query engine for larger-than-RAM data. It provides dplyr-like verbs (filter, select, mutate, group_by, summarise, joins, window functions) backed by a pure C11 pull-based execution engine and a custom on-disk format (`.vtr`). All operations are lazy until `collect()` materializes results.

## Build & Test Commands

```bash
# Document (regenerate NAMESPACE + man pages)
"/mnt/c/Program Files/R/R-4.5.2/bin/Rscript.exe" -e 'devtools::document()'

# Full check (compiles C, runs tests, checks NAMESPACE)
"/mnt/c/Program Files/R/R-4.5.2/bin/Rscript.exe" -e 'devtools::check(args = "--no-manual")'

# Run all tests
"/mnt/c/Program Files/R/R-4.5.2/bin/Rscript.exe" -e 'devtools::test()'

# Run a single test file
"/mnt/c/Program Files/R/R-4.5.2/bin/Rscript.exe" -e 'devtools::test(filter = "vtr1")'

# Load for interactive testing
"/mnt/c/Program Files/R/R-4.5.2/bin/Rscript.exe" -e 'devtools::load_all(); tbl(f) |> collect()'
```

## Architecture

### Execution model: lazy pull-based pipeline

All user-facing verbs (`filter`, `select`, `mutate`, etc.) build a tree of `vectra_node` objects in R. Each node wraps a C-level `VecNode` external pointer. No data moves until `collect()` calls `C_collect`, which pulls `VecBatch` structs through the node tree one row group at a time.

### On-disk format (.vtr / vtr1)

Custom binary columnar format defined in `src/vtr1.h`. A file contains: header (version + schema + row group count), row group index, then column data per row group. Columns are stored as typed arrays with validity bitmaps for NA support. `write_vtr()` writes from R data.frames; `tbl()` opens for lazy reading.

### C engine (`src/`)

Pure C11, no external dependencies. Key types in `src/types.h`:
- `VecType`: int64, double, bool, string
- `VecArray`: single column with validity bitmap and type-punned data union
- `VecBatch`: row group (n columns + optional selection vector for zero-copy filtering)
- `VecNode`: pull-based plan node with `next_batch()` and `free_node()` vtable

Plan nodes (each in its own `.h`/`.c` pair):
- `scan` (file reader) -> `filter` -> `project` (select/mutate/rename) -> `sort` -> `limit`
- `group_agg` (hash-based grouping + aggregation: n, sum, mean, min, max)
- `join` (hash join: left, inner, right, full, semi, anti)
- `window` (row_number, rank, dense_rank, lag, lead, cumsum, cummean, cummin, cummax)
- `concat` (union / bind_rows)

R-to-C bridge: `src/r_bridge.c` implements all `C_*` entry points registered in `src/init.c`.

### R layer (`R/`)

- `expr.R`: NSE expression serializer. Captures R expressions (arithmetic, comparison, boolean, is.na) into nested lists that the C bridge interprets. This is the translation layer between dplyr-style expressions and the C engine.
- `verbs.R`: All dplyr verb implementations as S3 methods on `vectra_node`. Verbs build new nodes via `.Call()` to C entry points.
- `joins.R`: Join verbs + `parse_join_keys()` for `by` argument handling.
- `across.R`: `across()` expansion for mutate/summarise.
- `windows.R`: Window function detection and node creation.
- `explain.R`: Prints the query plan tree for debugging.

### Column type mapping

R integers are stored as `VEC_INT64` internally and returned as `double` by default (no native int64 in base R). `bit64::integer64` support is optional via Suggests.

## Key Design Decisions

- **No Rcpp**: Direct `.Call()` interface to C via registered routines. No C++ anywhere.
- **Selection vectors**: `VecBatch.sel` enables zero-copy filtering (filter node sets a selection vector rather than copying rows).
- **String ownership**: `VecArray.owns_data` flag distinguishes owned vs borrowed string buffers to avoid double-free in hash join arenas.
- **Grouping is metadata-only**: `group_by()` just attaches `.groups` to the R list; the C-level `group_agg` node receives key names when `summarise()` is called.
