# vectra 0.9.0

## Raster and vector toolbox

* `polygonize(raster)` vectorises a raster into polygon features, the inverse of
  `rasterize()`: cells are read one tile-row strip at a time and (by default)
  dissolved by value into one polygon per value through `spatial_dissolve()`.
* `contours(raster, levels)` traces iso-lines with marching squares over a
  haloed strip pass, then joins each level's segments into continuous lines.
* `mask(raster, polygon)` clips a raster to an `sf` polygon layer one strip at a
  time, keeping the pixels whose centre falls inside (or, with `inverse = TRUE`,
  outside) it. It is the raster counterpart of `spatial_clip()`.
* `rast_calc(rasters, expr)` evaluates a cellwise expression across aligned
  rasters (map algebra): band indices like `(nir - red) / (nir + red)`,
  reclassification, and arithmetic across layers, streamed strip by strip.
* `mosaic(rasters, fun)` merges rasters sharing a resolution and cell grid onto
  their union, resolving overlap with `first` / `last` / `mean` / `sum` / `min`
  / `max`, one output strip at a time.
* `proximity(raster, target)` computes the exact Euclidean distance from every
  cell to the nearest feature (non-NA, or matching `target`) in CRS units, via
  the separable Felzenszwalb-Huttenlocher distance transform: a row pass, an
  out-of-core transpose, a column pass, and a transpose back, each over tile-row
  strips so the whole grid is never resident. Squared distances scale by the x
  and y resolution, so the result is exact on anisotropic cells.

## Documentation

* New `vignette("spatial")` walks the out-of-core GIS toolbox as one workflow,
  with inline canvas animations for the raster-to-points bridge, select by
  location, rasterization, and the cost-model tiers.
* The quickstart vignette leads with animated views of the streaming memory
  envelope, what has to fit in RAM, and the lazy pull-based plan, and its
  on-disk-format description now matches the tdc codec.

## Two-sided streamed spatial join

* `spatial_join(x, y, partition = grid(cellsize))` joins two larger-than-RAM
  layers by binning both to a uniform spatial grid and joining one shard at a
  time, for the case where neither side fits in memory as a resident `sf`
  object. `y` becomes a streamed `vectra_node`; each left feature is assigned to
  the single grid cell of its reference point while each right feature is
  replicated to every cell its bounding box overlaps, so a left row is emitted
  exactly once and the result equals the resident join. This is exact for point
  left geometries (the dominant case -- tagging a huge point set with the
  polygon it falls in). `grid(cellsize, origin)` defines the partition grid. The
  partition path serves the topological predicates (intersects, within,
  contains, ...) and `sf::st_nearest_feature`, for which each left feature
  searches its own cell and the eight around it (the nearest is found when it
  lies within one cell of the left reference cell).

## Streamed warp (resample / reproject)

* `warp(raster, template, method)` resamples or reprojects a `.vec` raster onto
  a target grid, walking the *output* one tile-row strip at a time. Each strip's
  target pixel centres are projected into the source CRS (via PROJ through `sf`
  only when the two CRSs differ), mapped through the source geotransform, and
  sampled from the bounded source window they fall in -- so the whole output grid
  is never resident and the source is read in windows rather than held whole.
  `method` is `"near"`, `"bilinear"`, or `"cubic"` (Catmull-Rom), following the
  GDAL / `terra::project()` convention; kernels that reach off the source extent
  or touch nodata return `NA`. `template` borrows a grid from another raster or
  is given as `list(crs =, extent =, res =, dims =)`. The C sampler keeps the
  interpolation native; projection stays in PROJ.

## Streamed focal and terrain

* `focal(raster, w, fun)` applies a moving window to a `.vec` raster, reading
  the input one tile-row strip at a time -- each strip expanded by the kernel
  radius (a halo read) so window neighbours are available without ever holding
  the whole grid resident. When `path` is given the output is streamed straight
  back to a new `.vec` one tile-row at a time, so neither the input nor the
  output band is ever fully in memory: the raster op that runs out of core where
  an in-memory engine needs the whole raster at once. The window is a weight
  matrix (or a single odd integer); `fun` is one of `"sum"`, `"mean"`, `"min"`,
  `"max"`, `"sd"`, `"median"`, computed in C, with `na.rm` matching the resident
  behaviour at edges.
* `terrain(raster, v)` derives DEM products with Horn's 3x3 method on the same
  haloed strip pass: `"slope"`, `"aspect"`, `"hillshade"`, `"TPI"`,
  `"roughness"`, `"TRI"`. The return follows the input -- one matrix for a single
  `v`, a named list (or a multi-band `.vec`) for several -- and matches
  `terra::terrain()` / `terra::shade()`.

## Streamed dissolve

* `spatial_dissolve(x, by, .fun)` unions the geometries within each `by` group
  into a single feature (the GIS "Dissolve" tool), optionally summarising
  attributes through a named list of functions. Dissolve needs every geometry of
  a group together, so it rides the partition tier: `x` is spilled once and
  routed into one shard per group in a single bounded pass, then each shard is
  unioned with `sf`. With no `by` the whole layer dissolves into one feature.

## Streamed zonal statistics

* `zonal(raster, zones, fun)` summarises a raster within zones one tile-row
  strip at a time, so the whole grid never has to be resident. Zones come from a
  second raster aligned to the value grid (the `terra::zonal()` pattern) or from
  an `sf` polygon layer (each pixel assigned the polygon its centre falls in).
  The per-zone moments are folded in memory as strips arrive -- peak memory is
  one strip plus the small per-zone table -- and `fun` may name several of
  `"mean"`, `"sum"`, `"count"`, `"min"`, `"max"`, `"sd"` at once. Raster zones
  are `sf`-free; `sd` is derived from the streamed moments with no second pass.

## Streamed vector-to-raster

* `rasterize(x, template, field, fun)` folds a larger-than-RAM point stream into
  a fixed raster grid one batch at a time. The grid is held resident while the
  points flow past, so peak memory is the grid plus one batch -- the streaming
  counterpart to `terra::rasterize()` on a point set that has to fit in RAM. The
  per-cell reduction (`"count"`, `"sum"`, `"mean"`, `"min"`, `"max"`) is
  accumulated in C. Points arrive either as two coordinate columns (the default,
  `sf`-free path) or from a hex-WKB point-geometry column. The result is an
  in-memory georeferenced matrix, or a `.vec` raster when `path` is given.

## Streamed select-by-location and clip/erase

* `spatial_filter(x, y, predicate)` keeps the rows of a streamed layer `x` whose
  geometry satisfies an `sf` binary predicate against a small resident layer `y`
  (select by location), filtering the billion-row stream one batch at a time
  while `y` stays in memory. Rows are filtered, never duplicated, and the output
  carries `x`'s schema unchanged; `negate = TRUE` keeps the non-matching rows
  (select by location, inverted).

* `spatial_clip(x, mask, erase)` cuts a streamed layer's geometry against a
  small resident `mask`: the intersection by default (the GIS "Clip" tool), or
  the difference with `erase = TRUE` (the "Erase" tool). The mask is dissolved
  once and held resident while the stream flows past one batch at a time.

* The run-file spill machinery shared by the streamed spatial verbs
  (`spatial_map`/`join`/`filter`/`clip`/`overlay`) is now a single internal
  accumulator, so all of them flush, finalize, and clean up identically.

# vectra 0.8.2

## Bug fixes

* `ifelse()` (and `if_else()`) now returns the correct type when its two
  branches differ. Previously `ifelse(int64_col, x, y)` with a `double` or `NA`
  other branch labelled the result column int64 while the evaluator produced
  doubles, so the kept int64 values came back as ~4.6e18 garbage (and triggered
  a spurious "int64 value exceeds 2^53" warning). The result column now adopts
  the common type of the two branches, matching the evaluator. In particular
  `ifelse(year > 0, year, NA)` is a clean way to blank out sentinel years.

# vectra 0.8.1

## Polygon self-overlay

* `spatial_overlay(x)` splits a polygon `sf` layer along all its own overlaps
  into disjoint pieces (the "Union (single layer)" overlay), returning a lazy
  node with one row per piece per covering polygon. Resolve the duplicates with
  a grouped `slice_min()`/`slice_max()` -- e.g. earliest designation year wins,
  `group_by(piece_id) |> slice_min(year)`. The overlay runs in C on the GEOS C
  API (via `libgeos`). Each feature is parsed once, in parallel -- repaired and
  snapped to a fixed-precision grid -- then features are grouped into connected
  components from their bounding boxes. Each component is one overlay job whose
  boundary linework is noded once and polygonised into faces (a single noding
  pass, so cost tracks the number of pieces, not how deeply polygons overlap);
  the few components too large for the memory budget are tiled over their own
  extent and clipped, so no single noding pass is ever large. Jobs run one per
  OpenMP thread (`threads`) and stream to a `.vtr` in batches sized to a
  `mem_limit` budget, so peak memory stays bounded regardless of layer size. The
  snapping grid is derived from the data's coordinate magnitude and checked
  against a coverage invariant (the piece areas covering an input sum to its
  area), so pieces come out disjoint and their areas reconstruct the union.
  Scales to layers a single `sf::st_intersection()` cannot hold at once (a 470k
  marine-protection layer overlays in bounded memory where the in-memory call
  exhausts RAM).

# vectra 0.8.0

## Group-aware slicing

* `slice_min()` and `slice_max()` now respect `group_by()`: they keep the n
  smallest/largest rows *within each group* and return the whole winning row
  (every column, including geometry carried as a string), rather than a global
  top-n. `with_ties = FALSE` returns exactly n per group via a deterministic
  ordered `row_number()`; `with_ties = TRUE` keeps rows tied at the nth value.
  Previously a grouped `slice_min()`/`slice_max()` silently ignored the grouping
  and returned a single global result.
* `row_number()` accepts an order column: `row_number(col)` and
  `row_number(desc(col))` assign a deterministic 1..n within each partition,
  ordered by the column (the unordered `row_number()` is unchanged).
  `rank(desc(col))` is also supported.

## Streamed spatial operations

* `spatial_map(x, fn)` streams a lazy query through an `sf` transform (buffer,
  centroid, CRS transform, simplify, ...) one batch at a time and returns a new
  lazy node, so a per-feature geometry operation runs on a table larger than
  RAM at one-batch peak memory.
* `spatial_join(x, y, join)` joins a streamed left side `x` against a small
  resident `sf` object `y` with an `sf` binary predicate (`st_intersects` by
  default): the spatial analogue of a hash join with the small side resident.
  The dominant use is tagging a huge point set with the polygon it falls in.
  Both-sides-huge joins compose with `offload(by = ...)`: partition on a spatial
  grid key, join each shard, recombine.
* `collect_sf(x)` materializes a spatial query as an `sf` object.
* Geometry rides through the engine as hex-encoded WKB in an ordinary string
  column (no new column type), losslessly round-tripped; the CRS is carried on
  the node. Topology stays with `sf`/GEOS — `sf` is an optional dependency
  (Suggests).

# vectra 0.7.1

* Cap the OpenMP team to two threads under `R CMD check`. When CRAN's
  `_R_CHECK_LIMIT_CORES_` is set, the package now lowers its default team size
  to two so the parallel string, fuzzy-join, sort, and window kernels stay
  within the check farm's two-core limit. The fuzzy-join match phase also
  clamps its requested thread count to the available maximum, matching the
  blocked fuzzy-lookup path. Outside a check the package still uses every
  available core.

# vectra 0.7.0

## Streaming consumption

* `collect_chunked(x, f, .init)` folds a function over a query one batch at a
  time. The engine pulls a single batch into R, applies `f(acc, chunk)`, frees
  the batch, and moves on, so a result larger than RAM can be reduced to a
  small summary (a running count, per-group sufficient statistics, the
  cross-products behind a linear fit) in one bounded-memory pass.
* `chunk_feeder(.source)` turns a query into a resettable generator following
  the `data(reset)` protocol that `biglm::bigglm()` expects, so a generalized
  linear model can be fitted out-of-core: each iteratively reweighted pass
  streams through the engine without ever holding the full design matrix.
  `.source` is a factory returning a fresh node, replayed on every reset.
* New C pull interface (`C_node_optimize`, `C_node_next_batch`) backs both
  verbs; per-batch conversion reuses the existing column converter, so the
  chunked and materializing paths share one code path.

## Offloading and out-of-core fits

* `offload()` is one verb with two return shapes. `offload(x)` materializes a
  query once to a `.vtr` and returns a node that streams from that file: it
  holds the same rows as `x` (an identity on values) and changes only the cost
  profile, since replaying it is a disk scan instead of a re-run of the upstream
  pipeline. `chunk_feeder()` accepts an offloaded node directly, so an iterative
  consumer such as `biglm::bigglm()` reads the prepared columns from disk on
  every reweighted pass rather than rebuilding them each time.
* `offload(x, by = ...)` splits a query into disjoint shards in a single
  streaming pass, one per key value (`method = "level"`), per value range
  (`"range"`), or per hash bucket (`"hash"`); `"auto"` picks level for a
  discrete key and range for a numeric one. The result is list-like: `length()`,
  `names()` (the keys), `p[["key"]]`, and `lapply(p, ...)` all work, turning a
  model that couples within a group into independent per-shard fits. The union
  of the shards reproduces the input; row totals are checked.
* `group_map()` and `group_modify()` run a function on each shard of a
  partition. `group_map()` reads each shard into a data.frame, hands it to the
  function with its key, and returns the results keyed by shard (one fit per
  group). `group_modify()` binds per-shard data.frames into one table and
  restores the key as a column. A purrr-style `~` formula works for either.
* `collect_chunked()` is now a generic and gains a `combine` argument: supplying
  it declares the reduction a monoid (with `.init` as identity), which lets the
  fold run over the shards of a partition and merge the partial results. A
  `commutative` flag declares the merge order-free.
* Offloaded streams carry a cost grade (passes over the data, peak memory, I/O
  class), shown by `print()` and `explain()` -- the label a plan reads to choose
  between a one-pass fold, an external sort (`arrange()`), and a partition.

# vectra 0.6.3

## Fixes

* `summarise()` / `summarize()` now accept namespace-qualified
  aggregation calls (`vectra::n()`, `vectra::sum(x)`,
  `vectra:::mean(x)`). Previously `parse_agg_expr` ran
  `as.character()` on the call head and dispatched on its result; for
  a `pkg::fn` call that yielded the length-3 vector
  `c("::", "pkg", "fn")`, and the subsequent `if (!fn %in% valid_aggs)`
  triggered "the condition has length > 1" under R >= 4.2. The parser
  now unwraps `::` / `:::` and uses the bare function name.

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
