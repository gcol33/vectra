# vectra 0.6.2

## CRAN archive-issue fixes

Resolves the three findings the auto-check email surfaced for the
2026-05-06 archived 0.5.1 release.

* DESCRIPTION: replaced "gridded" (flagged as a possibly-misspelled word
  in the CRAN incoming pretest) with "raster".
* gcc-ASAN heap-buffer-overflow in the LZ decode path
  (`tdc/src/api/decode_impl.c`, surfaced through `read_rg_tdc_with_fp` in
  `vtr1_tdc.c`): the consolidated decode pipeline now always allocates
  scratch buffers with a +16-byte wildcopy slack, so `tdc_match_copy`'s
  SIMD overshoot stays within the allocation. The `decode_ex.c` variant
  that was missing this slack on 0.5.1 is gone (folded into the shared
  `driver_decode_block_impl`). The ASAN-under-vignettes regression check
  is now part of the GitHub Actions sanitizer workflow so a future
  drift would be caught locally instead of at CRAN's BDR memcheck.
* rchk PROTECT findings in `src/r_bridge.c`, `src/r_bridge_io.c`,
  `src/vtr1_tdc.c`, and `src/collect.c`: every `Rf_getAttrib` /
  `Rf_mkString` result that crossed an allocating call (`R_alloc`,
  `Rf_warning`, `Rf_setAttrib`, `Rf_asReal`, `Rf_asInteger`,
  `parse_*`) is now `PROTECT`ed and balanced with a matching
  `UNPROTECT`. Touches `apply_annotation`, `C_write_vtr`,
  `C_write_vtr_tdc`, `parse_quantize`, and `parse_spatial`.

# vectra 0.6.1

## Fixes

* `src/vec_omp.h` and call sites: stop including `<omp.h>` and forward-declare
  the three OpenMP runtime functions vectra calls (`omp_get_max_threads`,
  `omp_get_thread_num`, `omp_in_parallel`). clang 21's bundled omp.h wrapper
  contains an unbalanced `#pragma omp end declare variant` that breaks
  compilation of `block.c` (and any other vectra TU that includes the
  wrapper) under r-devel-linux-x86_64-debian-clang. The bug is in the wrapper
  itself, so an `#ifdef _OPENMP` guard around `#include <omp.h>` is not
  enough — when `-fopenmp` is on the compile line, `_OPENMP` is defined and
  the broken wrapper is pulled in. Skipping the wrapper avoids the bug; the
  `#pragma omp ...` directives elsewhere in `src/` are still recognised and
  the runtime symbols resolve at link time via `libomp`. Fixes the
  compilation error that caused vectra 0.5.1 to be archived from CRAN.

# vectra 0.6.0

## Raster format (`.vec`)

A new tiled raster format and accompanying API for larger-than-RAM gridded
data. Each tile is encoded as a self-describing tdc block (PRED_2D +
BYTE_SHUFFLE + LZ); decoding is parallel across tiles.

* `vec_write_raster(x, path, ...)`: write a numeric matrix or 3D
  `(rows, cols, bands)` array to `.vec`. Storage dtypes: `f64`, `f32`,
  `i8`/`u8`, `i16`/`u16`, `i32`/`u32`, `i64`/`u64`. `compression` controls
  per-tile codec probing — `"fast"`, `"balanced"`, or `"max"` (six-spec probe
  per tile). Decode cost is unchanged across levels because each tile records
  its own codec spec.
* `vec_open_raster(path)` / `vec_close_raster(r)`: lazy open returning a
  metadata + handle list (`vectra_raster`). The handle is auto-finalized on
  garbage collection.
* `vec_read_window(r, band, level, cols, rows)`: decode a window of a chosen
  band, with overview-level support. Pixels outside the raster come back as
  `NA`. Tile decode is parallelized across worker threads (Phase 5a).
* `vec_extract_points(r, x, y)`: sample band values at `(x, y)` points.
* `vec_build_overviews(path, levels, resampling)`: append `n_levels - 1`
  reduced-resolution copies in place. Resampling kernels: `"nearest"`,
  `"average"`, `"bilinear"`, `"mode"`, `"gauss"`. The file's `n_levels` is
  updated atomically.
* `vec_to_tiff(path, output, compression)`: export `.vec` level-0 pixels to
  GeoTIFF. Compression is `"none"`, `"deflate"`, or `"lzw"`; LZW also applies
  horizontal differencing (Predictor 2) for integer pixel types, matching the
  layout libtiff/GDAL produce by default. Inherits dtype, geotransform,
  EPSG, and nodata from the source.

## Time cubes

* `vec_write_time_cube(x, times, path, layout, ...)`: write a 4D
  `(rows, cols, bands, time)` array. Two layouts:
  - `"image"` (default): one tile per `(band, time, ty, tx)` — optimal for
    "give me one full image at time T" reads.
  - `"pixel"`: one tile per `(band, ty, tx)` holding the full time stack as
    `[tw*th, n_time]` — optimal for "give me the time series at pixel
    `(x, y)`" reads.
* `vec_read_pixel_series(r, x, y, band)`: full time series at a single
  pixel as a numeric vector. On pixel-major files this is one tile decode;
  on image-major files the reader scans the index for distinct time stamps
  and decodes one tile per stamp.
* `vec_read_time_slice(r, time, band, level, cols, rows)`: read a single
  time slice as a matrix.
* `vec_raster_times(r, band, level)`: distinct time stamps, in ascending
  order.
* `vec_raster_layout(r)`: query whether an open raster is `"image"` or
  `"pixel"` layout.
* `print.vectra_raster()`: prints dimensions, dtype, geotransform, EPSG,
  nodata, and band names.

## GeoTIFF reader and writer

* Reader: tiled and Cloud-Optimized GeoTIFF (COG) inputs go through the same
  block abstraction as strip TIFFs (strips collapse to `n_blocks_x = 1`).
  Edge-block padding is handled in `block_stored_rows()`.
* `tiff_band_names()`: parse `<Item role="description">` entries from
  `GDAL_METADATA` (tag 42112). Pure-R scanner, no `xml2` dependency.
* `tiff_crs(path)`: read the EPSG code, geographic-vs-projected flag, and
  citation string from the GeoKey directory (tags 34735/34737).
* `write_tiff()` gains `tiled`, `tile_size`, `bigtiff`, and `crs` arguments.
  - `tiled = TRUE` emits TIFF tags 322/323/324/325 in place of strip tags.
    `tile_size` accepts a single integer (square) or a length-2 `c(w, h)`;
    both dimensions must be positive multiples of 16. Default 256. Tiled
    output is the layout required for Cloud-Optimized GeoTIFF.
  - `bigtiff = "auto"` (default) auto-promotes to BigTIFF (magic `0x002B`,
    64-bit offsets) when the expected raw payload exceeds the classic-TIFF
    4 GB ceiling; `TRUE` forces BigTIFF; `FALSE` forces classic TIFF. Tiled
    BigTIFF is not yet supported.
  - `crs` accepts an integer EPSG code, an `"EPSG:xxxx"` string, or a list
    with `$epsg`, `$geographic`, and optional `$citation`. Outputs round-trip
    through `terra::rast()` for 4326, 3857, and 31287.

## Fixes

* `collect()` / `block_array_gather`: empty-string slots now shortcut to
  `R_BlankString`. Previously the gather paths called `Rf_mkCharLenCE(NULL,
  0, ...)` and the dedup cache called `memcmp(NULL, ...)` when a batch
  happened to contain only empty/`NA` strings, tripping UBSAN's nonnull
  check even though the length was zero.

## Internal

* C-side `*_push` helpers (`vec_buf_push`, `vec_array_push`, ...)
  consolidated into a single `vec_grow_to` growth primitive.

# vectra 0.5.1

## CRAN resubmission fixes (0.5.0 incoming pretest feedback)

* `configure` / `configure.win`: rewritten as POSIX `/bin/sh` (previously
  `#!/usr/bin/env bash` with `set -o pipefail` and `[[ ... ]]`). Bash is
  not guaranteed on all CRAN build hosts.
* `src/window.c`: the OpenMP task-parallel merge sort helper was defined
  unconditionally but called only from `#ifdef _OPENMP` branches, producing
  a clang `-Wunneeded-internal-declaration` warning under Debian's
  no-OpenMP build. The definition now shares the guard.
* Vendored `tdc`: all `fprintf(stderr, ...)` debug/timing prints are routed
  through a `TDC_LOG(...)` macro that is a no-op unless
  `TDC_ENABLE_STDERR_LOG` is defined at build time, so the released `.so`
  contains no `stderr` / `fprintf` symbols. Addresses the WRE §1.6.4 policy
  forbidding compiled code from writing to stdout/stderr.

## Fixes

* `collect()`: fix use-after-free in the cross-batch CHARSXP dedup cache. Each
  slot stored a raw pointer into the decoder's heap buffer, which is freed
  when the batch is consumed; the next batch's hash-collision `memcmp` then
  dereferenced freed memory. Manifested as segfaults on the second consecutive
  `collect()` of a large multi-rowgroup string-heavy `.vtr` (register,
  backbones), more likely under the parallel reader where batches accumulate
  before the serial consumer loop. Now verifies cache hits against
  `CHAR(sexp)`, which points into the still-alive interned CHARSXP body.

# vectra 0.5.0

## Compression backend rewire

* Replaced the bespoke v4 codec with `tdc`, a standalone typed-dimensional
  compression library vendored into `src/tdc/`. Encode and decode go through
  a self-describing block record (model + transform chain + entropy) rather
  than per-column tag constants. Deleted `vtr_codec.c`, `vtr_encodings.c`,
  `vtr_compress.c`, `vtr1.c`, and `vtr_codec_internal.h`.
* The `.vtr` on-disk format is a deliberate breaking change: pre-0.5 files
  are not readable. `write_vtr()` and `append_vtr()` write the new container;
  `tbl()` reads only the new container.
* Per-row-group column statistics (min/max) are carried in the container
  index so the scan layer can still prune unreachable row groups.
* Parallel row-group reads are preserved.
* Custom vendoring via `tools/vendor_tdc.sh` and `configure` / `configure.win`
  pull the latest upstream `tdc` on every install when the source checkout
  is present; the pre-vendored copy is used otherwise.

## Known regression

* The v4 dict-defer CHARSXP fast path is gone — duplicate strings now hit R's
  CHARSXP hash per row. Will be re-implemented on top of `tdc`'s
  dictionary-encoded varlen output when it becomes a hot spot.

## Fixes

* `man/write_vtr.Rd`: replaced a literal percent sign in the `compress`
  argument description that produced malformed Rd output on build.
* Windows: `write_vtr()`, `append_vtr()` and `delete_vtr()` now use
  `MoveFileEx` with a short retry loop for the final temp-to-target swap.
  Previously, a preceding `tbl()` read could leave the target file mmap'd
  pending GC, and the swap would fail with a sharing violation.

# vectra 0.4.1

## Star schema and lookup

* New `vtr_schema()`, `link()`, and `lookup()` functions for star-schema
  workflows. Register a fact table with named dimension links once, then
  pull columns from any dimension without writing explicit joins. Only
  referenced dimensions are scanned.
* `lookup()` reports unmatched keys per dimension by default, catching
  referential integrity issues before they propagate NAs silently.
* Supports both `"left"` (default) and `"inner"` join modes, named keys
  for differing column names, and reusable schema objects across multiple
  queries.

# vectra 0.3.2

* Fix misaligned `int64_t` memory access in `vtr_codec.c` (UBSAN).
  Dictionary encoding wrote and read 8-byte offsets through an unaligned
  pointer; delta decoding had the same issue. All fixed with `memcpy`.

# vectra 0.3.1

* CRAN submission fixes: title case, quoted technical terms in DESCRIPTION,
  corrected documentation URLs.

# vectra 0.3.0

## File operations

* `append_vtr(df, path)`: append a data.frame as a new row group to an
  existing `.vtr` file. Existing row groups are never rewritten.
* `delete_vtr(path, row_ids)`: logically delete rows by 0-based physical
  index. Writes a tombstone side file (`<path>.del`); the `.vtr` file is
  never modified. Deletions are cumulative and excluded automatically on the
  next `tbl()` call.
* `diff_vtr(old_path, new_path, key_col)`: key-based logical diff between
  two `.vtr` files. Returns a list with `added` (a lazy `vectra_node`) and
  `deleted` (a vector of key values). Implemented as a single-pass C streaming
  engine with O(n_unique_keys) memory.

## Expressions

* `tolower()`, `toupper()`, `trimws()`: case conversion and whitespace
  trimming for string columns in `filter()` and `mutate()`.
* `levenshtein(x, y)` / `levenshtein_norm(x, y)`: Levenshtein edit distance
  and normalised variant (0–1). Supports column-vs-column and column-vs-literal
  comparisons. Optional `max_dist` argument for early termination.
* `dl_dist(x, y)` / `dl_dist_norm(x, y)`: Damerau-Levenshtein distance
  (counts transpositions as cost 1) and normalised variant.
* `jaro_winkler(x, y)`: Jaro-Winkler similarity (0–1, higher = more similar).
  All string-similarity functions propagate `NA` and work in `filter()` and
  `mutate()`.
* `resolve(fk, pk, value)`: scalar self-join — looks up `value` where
  `pk == fk` within the same batch. Useful for denormalising parent-child
  tables without a join.
* `propagate(parent_id, id, seed)`: tree-traversal aggregation — propagates
  non-NA `seed` values down a parent-child hierarchy until all reachable nodes
  are filled. Converges in O(depth) passes.

## Format

* `.vtr` format version 4 with a two-layer codec (no external dependencies):
  - Encoding: `PLAIN` (default), `DICTIONARY` (string columns with < 50%
    unique values), `DELTA` (monotonically increasing `int64` columns).
  - Compression: custom LZ77 byte compressor (`LZ_VTR`, ~120 lines of C).
    Applied after encoding; skipped for buffers < 64 bytes or when
    compression does not reduce size.
  Files written with v4 are typically 30–60% smaller than v3. `tbl()` reads
  v1–v4 files; `write_vtr()` always writes v4.

# vectra 0.2.2

## Query optimizer

* Column pruning: scan nodes only read columns needed by the query plan.
* Predicate pushdown: filter predicates are attached to scan nodes and use
  `.vtr` v3 per-rowgroup min/max statistics to skip entire row groups.

## Engine

* `.vtr` format version 3 with per-column per-rowgroup statistics (min/max).
* O(n log n) `rank()` and `dense_rank()` (replaces O(n²) comparison-based).
* Nested expressions in `summarise()`: `summarise(m = mean(x + y))` auto-inserts
  a hidden mutate.

## Expressions

* `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`: date/time
  component extraction for Date and POSIXct columns.
* `as.Date()` and `as.POSIXct()` literals in filter expressions (e.g.
  `filter(date > as.Date("2020-01-01"))`).
* `as.Date(string_col)`: convert ISO-format date strings to Date values.
* `nchar()`: returns string length as integer.
* `substr(x, start, stop)`: substring extraction (1-based, like R).
* `grepl(pattern, x)`: fixed string matching (no regex).
* `paste0(a, b)`: two-argument string concatenation.
* `gsub(pattern, replacement, x)` / `sub()`: fixed-string replacement.
* `startsWith()` / `endsWith()`: string prefix/suffix matching.
* `pmin()` / `pmax()`: element-wise minimum/maximum.
* `log2()`, `log10()`, `sign()`, `trunc()`: additional math functions.

## Aggregation

* `sd()` and `var()`: sample standard deviation and variance via Welford's
  online algorithm. Returns NA for groups with fewer than 2 values (R semantics).
* `first()` and `last()`: first and last non-NA value per group. Both support
  `na.rm = TRUE`.

## Verbs

* `slice_min()` and `slice_max()` gain a working `with_ties` parameter
  (default `TRUE`). Ties at the boundary are now included by default; use
  `with_ties = FALSE` for exactly `n` rows.
* `count()` and `tally()` gain a working `sort` parameter. `sort = TRUE`
  returns results in descending order of the count column.
* `transmute()` and `reframe()` now support `across()`.
* `distinct(.keep_all = TRUE)` with a column subset now emits a message when
  falling back to R.

## Utilities

* `glimpse()`: preview column names, types, and first few values without
  collecting the full result.
* `collect()` now works on data.frames (no-op), so `slice_min(...) |> collect()`
  works regardless of the `with_ties` path.

## Documentation

* New quickstart vignette: `vignette("quickstart")`.
* `@details` sections added to `filter()`, `mutate()`, `summarise()`,
  `arrange()`, `distinct()`, `count()`, and join functions.

# vectra 0.2.1

## Engine

* External merge sort with 1 GB memory budget and automatic spill-to-disk.
* Sort-based `group_by() |> summarise()` path for spill-safe aggregation.
* Chunked FULL join finalize (65,536 rows per batch).
* Automatic type coercion (`int64 <-> double`) in join keys and `bind_rows()`.
* `rank()` and `dense_rank()` window functions.

## Type system

* `.vtr` format version 2 with per-column annotations.
* Date, POSIXct, and factor columns roundtrip through `write_vtr()` / `collect()`.
* `where()` predicates work in `select()`, `rename()`, `relocate()`, and `across()`.

## Infrastructure

* Engine reference vignette (`vignette("engine")`).
* 17-scenario benchmark suite with baseline snapshots and regression thresholds.
* ASAN/UBSAN CI job on Linux.
* Benchmark smoke job on PRs.

# vectra 0.1.0

* Initial release.
* Custom columnar on-disk format (`.vtr`) with multi-row-group support.
* dplyr-compatible verbs: `filter()`, `select()`, `mutate()`, `transmute()`,
  `rename()`, `relocate()`, `group_by()`, `summarise()`, `count()`, `tally()`,
  `distinct()`, `reframe()`, `arrange()`, `slice_head()`, `slice_tail()`,
  `slice_min()`, `slice_max()`, `pull()`.
* Hash joins: `left_join()`, `inner_join()`, `right_join()`, `full_join()`,
  `semi_join()`, `anti_join()`.
* `bind_rows()` and `bind_cols()` for combining queries.
* Window functions: `row_number()`, `lag()`, `lead()`, `cumsum()`, `cummean()`,
  `cummin()`, `cummax()`.
* `across()` support in `mutate()` and `summarise()`.
* `explain()` for inspecting the execution plan.
* `tidyselect` integration for column selection helpers.
* Data sources: `.vtr`, CSV, SQLite, GeoTIFF.
* Data sinks: `write_csv()`, `write_sqlite()`, `write_tiff()`.
