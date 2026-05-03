# Vectra Raster Plan

**Status**: design proposal, not implemented.
**Audience**: vectra C/R devs.
**Goal**: dual-format raster support — `.vec` as the canonical *read* format (insanely compressed, insanely fast random-access), GeoTIFF as the canonical *export* format (interop with terra/GDAL/QGIS).

This is taxify-style: ingest once into `.vec`, query forever; export to TIFF only when handing data to a foreign tool.

---

## Design principles (non-negotiable)

1. **Maximum compression.** tdc beats LZW/DEFLATE on tabular data; we want the same advantage for raster. Target ≥ 30 % smaller than LZW-Predictor-2 GeoTIFF on dense float32 climate rasters; ≥ 50 % smaller on sparse/categorical.
2. **Maximum speed.** Random-access tile reads in O(log n_tiles) seek + one decode. SIMD decoders. mmap by default. Zero-copy where the layout permits.
3. **Single source of truth.** Same chunked spatial layout for memory, disk, and (eventually) network range-reads. No "in-memory format vs on-disk format" divergence.
4. **No premature abstraction.** Raster is a *first-class table schema* in vectra, not a parallel codebase. Reuse tdc, the existing IO layer, the existing query engine. Add only what raster genuinely needs.
5. **Cloud-native ready.** Layout must permit HTTP range-read access from S3/GCS without a full download. We don't have to ship the network reader in v1, but the file layout has to make it trivial later.
6. **Lossless by default, lossy opt-in.** Float quantization, JPEG2000, etc. are explicit opt-ins per band, never silent.

---

## Schema: raster as a vectra table

A raster is a table with these columns (one row per **tile**, not per pixel):

| column         | type            | notes                                                   |
|----------------|-----------------|---------------------------------------------------------|
| `tile_x`       | int32           | tile column index                                        |
| `tile_y`       | int32           | tile row index                                           |
| `level`        | uint8           | overview level (0 = full res, 1 = ½, 2 = ¼ …)            |
| `band`         | uint16          | band index (0-based)                                     |
| `time`         | int64 (nullable)| epoch ms or step index for time-series cubes             |
| `payload`      | blob (tdc)      | compressed pixel block                                   |
| `payload_codec`| uint8           | which sub-codec inside the blob (see Compression below)  |
| `min`, `max`   | matches sample  | per-tile statistics, kept in-line for predicate pushdown |
| `n_valid`      | int32           | non-NoData pixel count (skip empty tiles fast)           |
| `checksum`     | uint64 (xxh3)   | corruption detection                                     |

The **table** carries header metadata (CRS, geotransform, tile size, sample format, NoData, band names) in a sidecar struct stored as a typed column-group header (same place we already keep schema info in `.vec`).

Why one row per tile, not per pixel:
- Pixel-per-row is fine for sparse point clouds; for dense rasters it explodes row count (a 50 000² raster = 2.5 G rows) and destroys tdc's ability to exploit 2-D spatial correlation.
- Tile-per-row keeps row counts small (~100 k–1 M), each row's payload is independently decodable, and the engine's existing predicate pushdown on `tile_x`/`tile_y`/`min`/`max` gives you spatial filtering for free.

---

## Compression: layered, per-tile

Each tile's `payload` is a self-describing blob. Decoder reads `payload_codec` then dispatches:

### Sub-codecs (in order of typical effectiveness)

1. **`CONST`** — tile is all one value (very common for NoData regions, ocean masks, padding). 1 byte payload. Can compress an empty raster to ~32 bytes per band.
2. **`RLE`** — categorical / land-cover / classification rasters. tdc already has RLE infrastructure.
3. **`BITPACK + DELTA`** — small-integer rasters (e.g. counts, indices). Reuse tdc bitpack.
4. **`PFOR + ZSTD`** — generic integer fallback.
5. **`SPLITBYTE + ZSTD`** — float32/float64 default. Split mantissa/exponent/sign into separate byte streams (huge entropy gain), then ZSTD each. Target: 1.5–2× better than DEFLATE-Predictor-2.
6. **`ZFP` (lossless)** — opt-in for float; better on smooth rasters (DEMs, climate fields).
7. **`ZFP` (fixed-rate / fixed-precision)** — opt-in lossy for archive cases.
8. **`JPEG2000`** — opt-in lossy for visible-spectrum imagery.

### Pre-transform (applied before sub-codec)

- **Predictor**: horizontal (P2) and 2-D Paeth (P3). Same as PNG/TIFF.
- **Mask separation**: NoData mask becomes its own bitset stream, payload only stores valid pixels packed.
- **Float quantization** (opt-in): scale → int → BITPACK. For climate variables where 4 decimal places is enough, this is a 2–4× win over float compression.

### Codec selection

At write time, the encoder **probes** each tile against a small candidate set (CONST, RLE, BITPACK, SPLITBYTE+ZSTD) and picks the smallest. Probing cost is negligible vs. ZSTD itself. Hint: `compression = "fast"` skips probing and uses SPLITBYTE+ZSTD level 3; `compression = "max"` probes + ZSTD level 19.

---

## Speed: making it actually fast

### Memory layout
- **mmap by default.** Tile offsets are int64 in the table; decoder reads tile bytes via mmap'd file slice → decode into caller's output buffer.
- **Tile size**: default 512×512. Large enough to amortize header overhead, small enough that one tile per thread keeps cores busy.
- **Block size inside tile**: 8×8 or 16×16 sub-blocks for SIMD-friendly Paeth/predictor passes.

### Parallelism
- **Tile-parallel decode.** Reuse vectra's existing thread pool. Reading an N-tile window decodes N tiles in parallel, one per worker.
- **Per-band parallel** for multi-band reads.
- Decode-into-user-buffer (no intermediate allocation) when the caller pre-allocates an `array(dim=c(rows, cols, bands))`.

### SIMD
- Predictor inverse (P2/P3): AVX2 / NEON path for int16/int32/float32.
- Bitpack unpack: existing tdc SIMD paths.
- ZSTD: link against the vendored ZSTD with `-DZSTD_MULTITHREAD` and use the streaming API per tile.

### I/O
- Tile offset table stored **contiguously** at a known location in the file → one read fetches all offsets, then random-access by index.
- For sequential reads (whole-raster scan), prefetch next-tile bytes via `madvise(MADV_WILLNEED)` while decoding current tile.

### Cold-cache target
- 10 000 × 10 000 float32 single-band raster, SPLITBYTE+ZSTD level 9, full read on the i9-14900K: **< 200 ms cold, < 50 ms warm**. (Reference: tiled LZW GeoTIFF same size via terra is ~800 ms cold.)

---

## Overviews / pyramids built-in

Overviews are not a separate file or a separate codepath — they are additional rows in the same table with `level > 0`.

- Generated lazily on write or eagerly via `vec_build_overviews(path, levels = 5, resampling = "average")`.
- Resampling kernels: `nearest`, `average`, `bilinear`, `mode` (categorical), `gauss`.
- Reader's `read_window(extent, target_resolution)` picks the smallest level that covers the request at ≥ target resolution → reads only the tiles that overlap → resamples only the edges. Same algorithm GDAL uses for overview-aware reads.

---

## Spatial predicate pushdown

The query engine already pushes predicates into the table scan. For raster:

```r
vec_open("dem.vec") |>
  filter(extent_overlaps(c(xmin, ymin, xmax, ymax))) |>
  filter(level == 0) |>
  filter(min < 100)   # find tiles with any low-elevation pixel
```

`extent_overlaps` becomes a range scan on `tile_x`/`tile_y`. `min`/`max` columns give you spatial-statistical filtering without decoding a single pixel. This is the killer feature vs GeoTIFF — terra has to decode every tile to ask "where are pixels below 100?".

---

## Time-series / data cubes

Stacking N time slices of the same raster: just add rows with `time` set. Same file. Same table. Same scan engine.

- Temporal predicate pushdown is a column filter on `time`: `filter(between(time, t0, t1))`.
- Per-pixel time-series extraction: for a target (x, y), find the tile containing it, decode, return that pixel across the time range. Becomes O(time slices) tile decodes — already faster than terra's stack-and-extract for any non-trivial cube.
- Optional **transpose layout** (opt-in at write time): instead of `tile × time`, store `pixel-block × time` so pixel-time-series reads decode one block instead of N. Two physical layouts, same logical schema.

---

## TIFF export — tier-1 features needed

These are the gaps we already identified; restating in priority order for the dev who picks this up:

1. **LZW write** (compression code 5, optional Predictor 2). Patent expired 2004, no licensing concerns.
2. **Tiled write** (tags 322/323/324/325). Reuse the block abstraction the reader already has — writing is the inverse.
3. **NoData write** (tag 42113 `GDAL_NODATA` as ASCII). Read it back too.
4. **Multi-band chunky write** verification — write a 4-band raster, terra reads all 4 bands correctly.
5. **BigTIFF write** (magic `0x002B`, 64-bit offsets) — flip switch when `n_pixels * bytes_per_sample > 4 GiB - header_budget`.
6. **Overviews write** — dump `level > 0` rows from the `.vec` source as additional IFDs (`SubfileType = 1`).
7. **ZSTD write** (compression code 50000). Same vendored ZSTD as `.vec` uses.

Out of scope for v1 export: JPEG-in-TIFF, mask bands, color tables.

---

## TIFF read — what we already have / what to add

Already done:
- NONE, DEFLATE
- Strip and tiled
- GeoKey directory read
- GDAL_METADATA band names
- Point extraction

Add only what's needed for `.vec` ingestion of foreign GeoTIFFs (i.e. `vec_ingest_tiff(path)`):
- LZW read + Predictor 2.
- NoData read (tag 42113).
- BigTIFF read (offset width switch only).

Defer indefinitely (let users go through terra to ingest):
- JPEG/JPEG2000/WEBP read.
- Color tables, mask bands, multi-IFD overview-aware read.

---

## API sketch (R side)

```r
# Write a raster (one band, from a matrix with attached extent + crs)
vec_write_raster(
  m,
  path        = "dem.vec",
  extent      = c(xmin, ymin, xmax, ymax),
  crs         = "EPSG:31287",
  tile_size   = 512,
  compression = "max",          # "fast" | "balanced" | "max"
  overviews   = 5,
  nodata      = -9999
)

# Open lazily — no decode yet
r <- vec_open_raster("dem.vec")

# Window read: extent + target resolution → picks the right overview level
vals <- vec_read_window(r, extent = c(...), target_res = 100)

# Predicate-pushdown scan
r |>
  vec_filter(level == 0, min < 100) |>
  vec_collect_tiles()    # returns list of decoded tile arrays + their extents

# Point extraction (already exists for TIFF; mirror for .vec)
vec_extract_points(r, xy)

# Export
vec_to_tiff(r, "out.tif", compression = "lzw", tiled = TRUE, bigtiff = "if_needed")
```

C-side entry points mirror the R API: `vec_raster_open`, `vec_raster_read_window`, `vec_raster_extract_points`, `vec_raster_to_tiff`.

---

## Implementation phases

**Phase 1 — schema + write + read (no compression sophistication yet)**
- Schema struct, header serialization, tile offset table.
- Writer for SPLITBYTE+ZSTD only (good baseline).
- Reader: open, window read, point extract.
- R API: `vec_write_raster`, `vec_open_raster`, `vec_read_window`, `vec_extract_points`.
- Round-trip test against terra.

**Phase 2 — codec menu + probing**
- Add CONST, RLE, BITPACK+DELTA, PFOR+ZSTD.
- Probing encoder.
- `compression = "fast"|"balanced"|"max"` knob.

**Phase 3 — overviews + spatial predicate pushdown**
- `vec_build_overviews`, resampling kernels.
- `extent_overlaps` predicate.
- `min`/`max` pushdown.

**Phase 4 — TIFF export tier-1 (LZW + tiled + NoData + BigTIFF + multi-band)**
- Pure-C LZW encoder (~150 lines).
- Tiled writer (mirror reader's block layout).
- NoData tag write.
- 4-band write verification.
- BigTIFF switch.

**Phase 5 — performance pass**
- SIMD predictor inverse.
- mmap + madvise.
- Parallel tile decode.
- Cold-cache benchmark vs terra.

**Phase 6 — time cubes + transpose layout**
- `time` column.
- Temporal predicate pushdown.
- Optional transpose layout for pixel-time-series workloads.

**Phase 7 — lossy / advanced (only if needed)**
- ZFP integration.
- JPEG2000 for imagery.
- Cloud range reads (HTTP/S3).

---

## What this is NOT

- Not a reprojection engine. Use terra/sf/GDAL upstream.
- Not a replacement for GDAL's full driver matrix. Foreign formats go through terra.
- Not a replacement for terra's analysis API (`focal`, `app`, `zonal`, raster algebra). vectra does storage + scan + extraction. Analysis composes downstream.

The bet: if storage + scan + extract is 5–10× faster and 2–3× smaller than terra-on-GeoTIFF, that's enough for it to become the default intermediate format for any serious raster pipeline (siteify, taxify, RESOLVE, etc.). Foreign interop happens at the edges via TIFF export.

---

## Open questions for the implementer

1. **tdc reuse vs raster-specific codecs**: how much of the SPLITBYTE+ZSTD pipeline is already in tdc, vs. needs to be added? If tdc already does mantissa/exponent splitting, we get this for free.
2. **Tile size default**: 512 is a guess. Benchmark 256 / 512 / 1024 on the i9 + Mac M4 Pro.
3. **Overview storage**: same file vs. sidecar `.vec.ovr`? Same-file is simpler; sidecar is closer to GDAL's habit and lets you regenerate without rewriting.
4. **Schema versioning**: how does an old reader handle a `.vec` raster written by a newer encoder with new sub-codecs? Reserve a `feature_flags` bitset in the header so old readers can refuse cleanly.
5. **NoData representation**: typed NA vs. sentinel value vs. separate mask bitset? Recommend separate mask bitset stored alongside payload — clean, codec-independent, lets the value channel compress without sentinel pollution.
