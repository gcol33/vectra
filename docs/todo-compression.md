# VTR Compression Redesign

## Why

VTR compression is broken for numeric data. The current LZ_VTR (custom
LZ77, 256-byte window) finds zero patterns in raw float64 bytes —
“compressed” files are identical in size to raw. The CLAUDE.md
incorrectly claims zstd is linked; only zlib is linked (TIFF deflate +
CSV gzip). We need compression that actually works on columnar
float64/int64 data.

Goal: match or beat zstd on all benchmarks (ratio and speed), with code
we fully own.

## Architecture

The compression system is a sealed pipeline with three layers. Each
layer has a clear contract. Low-level ugliness stays contained — the
rest of vectra never sees it.

    Public API           write_vtr(x, path, compress = "fast")
                               |
    Orchestration        compress_frame(batch, schema, level)
                               |
                         +-----+------+
                         |            |
    Mid-level        compress_block   emit_header
                         |
                     +---+---+
                     |       |
    Low-level    byte_shuffle  lz_compress
                 (sealed)      (sealed)

### Layer boundaries

**Public API** — R functions. `compress` parameter: `"none"`, `"fast"`,
`"ratio"`. That’s it.

**Orchestration** — `vtr_encode_column_ex(col, n_rows, level)`. Decides
encoding (PLAIN/DICTIONARY/DELTA), decides whether to shuffle, picks
compressor. Reads like the conceptual algorithm. No bit manipulation, no
pointer arithmetic.

**Mid-level** — `compress_block(data, size, level)`,
`byte_shuffle(dst, src, n_elems, elem_size)`. Clear inputs, clear
outputs. Each is a self-contained transform.

**Low-level kernels** — match finding, hash chains, literal run
encoding. Dense, optimized, ugly where needed. But sealed: the rest of
the codebase never calls into these. They exist behind `lz_compress()`
and `lz_decompress()` and nowhere else.

The wrapper is not convenience — it protects the rest of the code from
the implementation style of the hot path.

### State management

All compressor state lives in stack-allocated structs with well-defined
lifetimes:

``` c
typedef struct {
    const uint8_t *src;
    uint32_t src_size;
    uint8_t *dst;
    uint32_t dst_size;
    uint32_t dst_pos;
    uint16_t htab[65536];     /* hash table — lives here, not global */
    uint16_t chains[4][65536]; /* hash chains — here too */
} LZState;
```

No thread-local globals. No leaked assumptions. The struct is created,
used, and destroyed within `lz_compress()`. The caller never touches it.

## Pipeline: byte-shuffle + compressor

### Byte-shuffle

Transposes N elements of E bytes so same-significance bytes are grouped:

    Before: [e0:b0 b1 b2 b3 b4 b5 b6 b7] [e1:b0 b1 b2 b3 b4 b5 b6 b7] ...
    After:  [e0:b0 e1:b0 e2:b0 ...] [e0:b1 e1:b1 e2:b1 ...] ...

After shuffle, exponent bytes of float64 are adjacent (nearly identical
for same-magnitude data), high mantissa bytes cluster. Any LZ compressor
finds massive redundancy.

Element size is type-aware (no extra disk bytes — reader knows from
schema):

| Column type + encoding | elem_size      | Shuffle? |
|------------------------|----------------|----------|
| PLAIN + INT64/DOUBLE   | 8              | yes      |
| PLAIN + BOOL           | 1              | no       |
| PLAIN + STRING         | variable       | no       |
| DELTA + INT64          | 8              | yes      |
| DICTIONARY + STRING    | variable (RLE) | no       |

~30 lines of C. Inverse is symmetric.

### LZ compressor (from scratch, fully owned)

Two tiers:

| Level | Tag | Pipeline | Target |
|----|----|----|----|
| `"fast"` | `VTR_COMP_SHUFFLE_LZ (0x04)` | shuffle + LZ | match zstd level 1 speed, ≥90% of its ratio |
| `"ratio"` | `VTR_COMP_SHUFFLE_DEFLATE (0x05)` | shuffle + zlib deflate | match zstd level 6 ratio |

### LZ vs current LZ_VTR

| Parameter   | LZ_VTR (current) | LZ (new)       |
|-------------|------------------|----------------|
| Max offset  | 256 (8-bit)      | 65536 (16-bit) |
| Max match   | 130              | 258            |
| Hash        | 3-byte, 14-bit   | 4-byte, 16-bit |
| Hash chains | single entry     | 4-deep         |
| Match token | 2 bytes          | 3 bytes        |

Wire format: `[0xxxxxxx]` = literal (1-128 bytes), `[1xxxxxxx yy zz]` =
match (len 3-130, offset 1-65536).

The 64K window is critical: after byte-shuffle, patterns in
same-significance lanes repeat at stride N (row group size), which can
be thousands of bytes apart.

### Why this can beat zstd on columnar float data

zstd is a general-purpose compressor. It doesn’t know it’s looking at
columnar float64. We do. The byte-shuffle exploits columnar structure
that zstd’s match finder has to discover blindly. On shuffled data, even
a simple LZ77 with adequate window size captures most of the redundancy
— the entropy is in the structure, and shuffle removes it before
compression starts.

The benchmark target: feed the same raw column bytes to both our
pipeline (shuffle + LZ) and zstd. We should match or beat on ratio
because shuffle gives us a structural advantage zstd doesn’t have. We
should match on speed because our inner loop is simpler (no FSE, no
Huffman — just LZ with large window on already-structured data).

## Format

Stay at v4. New compression tags (0x04, 0x05) use existing chunk header:
`encoding(1) + compression(1) + data_size(4) + uncompressed_size(4)`.
Old LZ_VTR files still decode. Old readers error cleanly on unknown
tags.

## Files to modify

| File | What |
|----|----|
| `src/vtr_codec.h` | New constants `VTR_COMP_SHUFFLE_LZ`, `VTR_COMP_SHUFFLE_DEFLATE`. New `vtr_encode_column_ex()` signature. |
| `src/vtr_codec.c` | `byte_shuffle()` / `byte_unshuffle()` (~30 LOC). `lz_compress()` / `lz_decompress()` (~140 LOC). Wire into encode/decode pipeline. `#include <zlib.h>` for RATIO path. |
| `src/vtr1.c` | Fast-path decode for new tags in `vtr1_read_rowgroup` + parallel reader. Pass comp_level to encode. `#include <zlib.h>`. |
| `src/vtr1.h` | Add comp_level to `vtr1_write_rowgroup` or context struct. |
| `src/vtr_write.c` / `.h` | Thread comp_level through write pipeline. |
| `src/r_bridge_io.c` | Parse `compress` arg from R, map string to int level. |
| `R/write.R` | Add `compress = "fast"` param to [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md). |

~300 lines of new/modified C.

## Verification

1.  **Round-trip correctness**:
    `all.equal(original, tbl(compressed) |> collect())` for all column
    types
2.  **Backward compat**: old LZ_VTR files still read
3.  **Benchmark vs zstd**: link zstd in bench script only (not runtime
    dep), compare ratio + decode/encode speed on 2000x2000x5 float64
    raster
4.  **File sizes**: raw VTR vs fast vs ratio vs terra DEFLATE vs zstd
5.  **R CMD check**: clean pass
6.  **String columns**: verify DICTIONARY encoding still compresses well
    with LZ_VTR path (no regression)

## Implementation order

1.  Byte-shuffle + unshuffle (self-contained, testable in isolation)
2.  LZ compressor + decompressor (self-contained, testable in isolation)
3.  Wire into `vtr_encode_column_ex` / `vtr_decode_column`
4.  Thread comp_level through write path (vtr1.c → vtr_write.c →
    r_bridge_io.c → R)
5.  Add fast-path decode in read path
6.  Benchmark + tune
7.  Fix CLAUDE.md (remove zstd claims, document actual compression)

------------------------------------------------------------------------

# Phase 2: Narrow Integer Types, Lossy Quantization, and Raster Compression

Phase 1 (above) delivered byte-shuffle + LZ/deflate on raw columnar
bytes. Phase 2 attacks the data itself: remove unnecessary precision,
exploit spatial structure, and compress the residuals.

## Why

Float64 carries 15–17 significant digits. Temperature data needs 3. That
means ~80% of the mantissa bits are noise that no compressor can reduce.
Byte-shuffle groups the noisy bytes together, which helps, but the noise
is still there consuming space. The fix is to remove the noise before
compression — store only the precision the data actually carries.

Beyond precision, spatially smooth rasters have massive redundancy:
neighboring cells differ by tiny amounts. Exploiting that structure
before handing data to the compressor can reduce entropy by an order of
magnitude.

## Overview

Three new capabilities, each building on the previous:

    Phase 2a: Narrow integer types (int8, int16, int32)
    Phase 2b: Lossy quantization encoding (float → narrow int with scale)
    Phase 2c: Raster-aware spatial encoding (approximation + correction)

The full pipeline for a temperature raster:

    float64 values
      → quantize to int16 (scale=1000, ±20°C → ±20000, fits int16)    [2b: lossy]
      → spatial predictor removes smooth trend                          [2c: raster]
      → DIFF encoding on residuals                                      [2c: general]
      → byte-shuffle + LZ/deflate                                      [phase 1]

Each layer is independent and optional. A user who wants lossless
float64 skips 2b/2c and gets the existing Phase 1 pipeline unchanged.

------------------------------------------------------------------------

## Phase 2a: Narrow Integer Types

### Motivation

VecType currently has four types: int64, double, bool, string. All
integers are 8 bytes. For data that fits in \[-128, 127\] or \[-32768,
32767\], this wastes 6–7 bytes per value. Narrow types give:

- 2–8× raw size reduction before any compression
- Better byte-shuffle effectiveness (shorter shuffle strides)
- Foundation for quantization encoding (float → int16 needs an int16
  type)

### Type system changes

Add three new VecType variants:

``` c
typedef enum {
    VEC_INT8   = 4,   /* signed 8-bit  [-128, 127] */
    VEC_INT16  = 5,   /* signed 16-bit [-32768, 32767] */
    VEC_INT32  = 6,   /* signed 32-bit [-2^31, 2^31-1] */
    VEC_INT64  = 0,   /* existing */
    VEC_DOUBLE = 1,   /* existing */
    VEC_BOOL   = 2,   /* existing */
    VEC_STRING = 3    /* existing */
} VecType;
```

Existing tag values (0–3) are unchanged for backward compatibility. New
tags 4–6 are only written by v5+ files.

### Storage layout

Each narrow type gets its own buffer pointer in the VecArray union:

``` c
union {
    int64_t  *i64;
    int32_t  *i32;
    int16_t  *i16;
    int8_t   *i8;
    double   *dbl;
    uint8_t  *bln;
    struct { ... } str;
} buf;
```

### R bridge mapping

R has no native int8/int16/int32. Mapping options:

| VecType | R representation | Notes |
|----|----|----|
| INT8 | integer (32-bit) | Widen on read, narrow on write if `type=` specified |
| INT16 | integer (32-bit) | Same |
| INT32 | integer (32-bit) | Direct — R integer IS 32-bit signed |
| INT64 | double (default) or bit64::integer64 | Existing behavior |

On write, the user specifies target types via a `col_types` parameter or
a schema:

``` r

write_vtr(df, "out.vtr", col_types = c(temp = "int16", band = "int8"))
```

Values that overflow the target range produce a warning and are clamped
or set to NA.

### Byte-shuffle element sizes

| Type   | elem_size | Shuffle?      |
|--------|-----------|---------------|
| INT8   | 1         | no (identity) |
| INT16  | 2         | yes           |
| INT32  | 4         | yes           |
| INT64  | 8         | yes           |
| DOUBLE | 8         | yes           |

### Expression evaluation

Arithmetic/comparison on narrow ints promotes to int64 internally (same
as C integer promotion). No narrow-int arithmetic kernels — the cost is
in I/O, not compute. This keeps the expression evaluator simple: read
narrow → widen to int64 → evaluate → optionally narrow on write.

### Format version

Bump to v5. v5 reader handles int8/16/32 column types. v4 reader rejects
unknown type tags cleanly (already does this). v5 writer still produces
v4 files if no narrow types are used.

### Files to modify

| File | What |
|----|----|
| `src/types.h` | Add VEC_INT8/16/32 to enum, update vec_type_name(), add elem_size helper |
| `src/types.h` | Add i8/i16/i32 to VecArray union |
| `src/vtr1.h` / `src/vtr1.c` | Read/write narrow types in row groups, bump version |
| `src/vtr_codec.h` / `src/vtr_codec.c` | Shuffle elem sizes for 2/4-byte types, encode/decode narrow columns |
| `src/r_bridge.c` / `src/r_bridge_io.c` | R integer ↔︎ narrow int conversion, col_types param |
| `src/expr.c` | Widen narrow ints to int64 before evaluation |
| `src/scan.c` | Zone maps for narrow types |
| `src/sort.c` | Comparison for narrow types |
| `R/write.R` | `col_types` parameter |

### Verification

- Round-trip: write int16, read back, compare (widened to R integer)
- Overflow: values outside range produce warning + NA
- Existing int64/double paths unchanged
- Expression evaluation: filter/mutate on int16 columns works correctly
- R CMD check clean

------------------------------------------------------------------------

## Phase 2b: Lossy Quantization Encoding

### Motivation

Lossy compression is a legitimate storage-layer feature for numeric
data. The user trades precision for space, with explicit control over
the trade-off. This is what HDF5’s scale-offset filter does, what
NetCDF’s lossy compression does, and what every image format does.

The key insight: for many scientific datasets, the measurement precision
is far less than float64. Temperature to 0.001°C needs ~15 bits, not 52
mantissa bits. Quantizing to int16 removes 6 bytes of noise per value.

### Encoding specification

New encoding tag:

``` c
#define VTR_ENC_QUANTIZE 0x03  /* lossy: float → scaled integer */
```

On-disk metadata per column (stored in the column chunk header):

    quantize_scale:  float64 (8 bytes)  — multiplier used during encoding
    quantize_offset: float64 (8 bytes)  — offset (for non-centered ranges)
    target_type:     uint8   (1 byte)   — VEC_INT8/16/32 tag

Encoding formula:

    stored_value = round((float_value - offset) * scale)

Decoding formula:

    float_value = (stored_value / scale) + offset

The offset centers the range to maximize use of the target integer
range. For temperature in \[-20, 20\]:

    offset = 0.0 (symmetric around zero)
    scale  = 1000.0 (0.001 precision)
    range  = [-20000, 20000] → fits int16 [-32768, 32767]

### User API

``` r

# Explicit quantization
write_vtr(df, "out.vtr", 
          quantize = list(temp = c(scale = 1000, type = "int16")))

# Convenience: auto-compute scale from target precision
write_vtr(df, "out.vtr",
          quantize = list(temp = c(precision = 0.001, type = "int16")))

# Multiple columns
write_vtr(df, "out.vtr",
          quantize = list(
            temp     = c(precision = 0.001, type = "int16"),
            pressure = c(precision = 0.1,   type = "int32"),
            humidity = c(precision = 1,     type = "int8")
          ))
```

When `precision` is given instead of `scale`: `scale = 1 / precision`.
The offset is auto-computed as the midpoint of the column’s range (or 0
if the range is symmetric).

### Interaction with downstream encodings

After quantization, the column IS a narrow integer column. All
downstream encodings (DIFF, byte-shuffle, LZ/deflate) apply to it
naturally. The quantize step just sits at the front of the pipeline:

    float64 → QUANTIZE → int16 → [DIFF] → byte-shuffle → LZ/deflate

### Lossiness contract

The file metadata records that quantization was applied, the scale,
offset, and target type. On read:

- Default: reconstruct to float64 (inverse formula). The user gets
  floats back, but only to the stored precision.
- Optional: read raw integers (`raw = TRUE`) for downstream processing
  that doesn’t need the float conversion.

The explain() output shows the quantization parameters so the user
always knows what happened to their data.

### Files to modify

| File | What |
|----|----|
| `src/vtr_codec.h` | VTR_ENC_QUANTIZE tag, quantize metadata in VtrEncodedCol |
| `src/vtr_codec.c` | Quantize encode/decode: float→int with scale/offset, inverse |
| `src/vtr1.c` | Read/write quantize metadata in column chunk header |
| `src/r_bridge_io.c` | Parse `quantize` list from R |
| `R/write.R` | `quantize` parameter |

### Verification

- Round-trip precision: `max(abs(original - reconstructed)) <= 1/scale`
- Overflow detection: values outside target int range → warning + NA
- Interop with DIFF encoding and compression
- explain() shows quantize params
- R CMD check clean

------------------------------------------------------------------------

## Phase 2c: Raster-Aware Spatial Encoding

### Motivation

Raster data is 2D (or 3D with bands). Neighboring cells are correlated.
After quantization, a temperature raster might look like:

    1234 1235 1235 1236 1237 1236 ...

Two techniques exploit this: general 1D differencing (works on any
column) and 2D spatial prediction (raster-specific, higher compression).

### Part 1: DIFF encoding (general, 1D)

Generalize the existing DELTA encoding (monotonic int64 only) to signed
differencing on any integer type:

``` c
#define VTR_ENC_DIFF 0x04  /* store first value + signed differences */
```

Encoding:

    output[0] = input[0]              (first value, full width)
    output[i] = input[i] - input[i-1] (signed difference)

For slowly varying data, differences are small integers near zero. After
byte-shuffle, the high bytes are mostly 0x00 or 0xFF, which LZ crushes.

Works on all integer types (int8/16/32/64) and float64 (IEEE subtraction
— less effective but still helps for similar-magnitude values). The
encoding tag on disk tells the decoder to reverse the differencing.

DIFF vs DELTA: - DELTA requires monotonically increasing, stores as
unsigned. Existing behavior unchanged. - DIFF works on any sequence,
stores signed differences. New encoding.

Auto-selection heuristic in the encoder: if a column is monotonically
increasing → DELTA (existing). If a column has low variance in
consecutive differences → DIFF. Otherwise → PLAIN.

### Part 2: Spatial predictor (raster-specific, 2D)

For raster data where we know the grid dimensions (nx, ny), we can
predict each cell from its neighbors and store only the residual.

``` c
#define VTR_ENC_SPATIAL 0x05  /* 2D predictor + residuals */
```

#### Predictor options (stored in 1-byte predictor tag):

| Tag | Predictor | Formula                      | Best for             |
|-----|-----------|------------------------------|----------------------|
| 0   | Left      | `pred = val[row][col-1]`     | Smooth along rows    |
| 1   | Up        | `pred = val[row-1][col]`     | Smooth along columns |
| 2   | Average   | `pred = (left + up) / 2`     | Generally smooth     |
| 3   | Paeth     | PNG-style Paeth predictor    | Edges and gradients  |
| 4   | Plane     | Per-tile least-squares plane | Large-scale trends   |

The encoder tries predictors 0–3 on a sample of the row group, picks the
one with smallest sum of absolute residuals. Predictor 4 (plane) is used
when the tile-based approach is enabled.

#### Plane predictor detail (predictor 4)

Divide the raster into tiles (e.g. 32×32 or 64×64). For each tile:

1.  Fit a plane: `pred(x,y) = a + b*x + c*y` via least-squares (6 sums,
    closed-form solution — no iteration)
2.  Compute residual: `residual = quantized_value - round(pred)`
3.  Store: 3 coefficients per tile (as int32) + residual array

The coefficients are tiny (3 × 4 bytes per tile = 12 bytes for a
32×32=1024 cell tile — negligible overhead). The residuals are small
integers near zero.

For a temperature raster with a north-south gradient of 0.5°C over 2000
cells: without the plane, values span \[-20000, 20000\]. With the plane
removed, residuals might span \[-50, 50\]. That’s a ~400× reduction in
value range before compression even starts.

#### On-disk format for spatial encoding

Column chunk header extension:

    predictor_tag: uint8     — which predictor (0–4)
    nx:            uint32    — raster width (needed to reconstruct 2D indexing)
    ny:            uint32    — raster height
    tile_size:     uint16    — tile size (only for predictor 4)
    n_tiles:       uint32    — number of tiles (only for predictor 4)
    coefficients:  int32[]   — 3 per tile (only for predictor 4)
    residuals:     int_N[]   — same type as input column, after prediction

The residuals then go through the standard compression pipeline
(byte-shuffle + LZ/deflate).

#### Where the grid dimensions come from

The tiff backend already knows nx, ny (from the TIFF header). For
general .vtr files, the user provides dimensions:

``` r

# Tiff path: dimensions are automatic
write_tiff(raster_node, "out.tif", spatial_compress = TRUE)

# VTR path: user provides dimensions
write_vtr(raster_node, "out.vtr", 
          spatial = list(nx = 2000, ny = 2000))
```

If spatial dimensions are not provided, spatial encoding is not
attempted (falls back to DIFF or PLAIN).

### Full raster pipeline

    float64 temperature raster (2000 × 2000 × 5 bands)
      → QUANTIZE: float64 → int16, scale=1000        [removes 6 bytes/value of noise]
      → SPATIAL: plane predictor, 32×32 tiles         [removes smooth trend]
      → residuals are tiny int16 values near zero
      → byte-shuffle: group high/low bytes            [high bytes all ~0x00]
      → LZ or deflate                                [crushes the zero-runs]

Expected compression vs Phase 1:

| Stage                                  | Bytes/value | Ratio vs raw float64 |
|----------------------------------------|-------------|----------------------|
| Raw float64                            | 8.0         | 1.00×                |
| Phase 1 (shuffle + LZ)                 | ~1.2–1.6    | ~0.15–0.20×          |
| Phase 1 (shuffle + deflate)            | ~0.6–0.8    | ~0.07–0.10×          |
| Quantize to int16 only                 | 2.0         | 0.25×                |
| Quantize + DIFF + shuffle + LZ         | ~0.2–0.4    | ~0.03–0.05×          |
| Quantize + spatial + shuffle + LZ      | ~0.1–0.2    | ~0.01–0.03×          |
| Quantize + spatial + shuffle + deflate | ~0.05–0.15  | ~0.007–0.02×         |

These are estimates based on typical temperature raster characteristics.
Actual ratios depend on spatial smoothness and the precision/range
trade-off.

### Files to modify

| File | What |
|----|----|
| `src/vtr_codec.h` | VTR_ENC_DIFF, VTR_ENC_SPATIAL tags, predictor constants |
| `src/vtr_codec.c` | DIFF encode/decode (~40 LOC). Spatial predictor encode/decode (~200 LOC). Plane fitting (~50 LOC, closed-form least squares). Auto-selection heuristic. |
| `src/vtr1.c` | Read/write spatial metadata in column chunk header |
| `src/tiff_write.c` | Pass nx/ny through to codec when spatial_compress enabled |
| `src/r_bridge_io.c` | Parse `spatial` list from R |
| `R/write.R` | `spatial` parameter |

### Verification

- DIFF round-trip: exact on all integer types
- DIFF on float64: bitwise exact (IEEE subtract/add)
- Spatial predictors 0–3: exact round-trip (integer arithmetic, no
  rounding)
- Plane predictor: exact round-trip (residual = value - round(pred),
  reconstruct = round(pred) + residual)
- Plane coefficients stored as int32 (fixed-point with known scale) — no
  float rounding in coefficient storage
- Grid dimensions mismatch detection (nx × ny ≠ n_rows → error)
- Falls back gracefully when spatial dims not provided
- Benchmark: 2000×2000×5 temperature raster, compare all pipeline
  combinations

------------------------------------------------------------------------

## Implementation Order (Phase 2)

All three sub-phases are sequential — each builds on the previous.

### Phase 2a: Narrow integer types ✓ DONE

1.  [x] Add VEC_INT8/16/32 to VecType enum and VecArray union
2.  [x] Read/write narrow types in vtr1.c (v5 format)
3.  [x] Byte-shuffle for 2-byte and 4-byte elements
4.  [x] R bridge: integer ↔︎ narrow int conversion + col_types parameter
5.  [x] Expression evaluation: widen to int64 on read
6.  [x] Zone maps, sort, scan for narrow types
7.  [x] Tests + R CMD check (838 tests pass, 0 -Wswitch warnings)

### Phase 2b: Lossy quantization encoding ✓ DONE

1.  [x] VTR_ENC_QUANTIZE in codec: float → scaled narrow int with stored
    scale/offset
2.  [x] Inverse decode: narrow int → float (transparent dequantize on
    read)
3.  [x] Wire quantize metadata into column chunk header (vtr1.c) — 17
    extra bytes (scale:f64, offset:f64, target_type:u8)
4.  [x] R API: `quantize` parameter in write_vtr() (both data.frame and
    node paths)
5.  explain() shows quantize params — deferred (quantize is write-time,
    not query-plan)
6.  [x] Tests: precision contract, overflow handling, NA handling,
    interop with Phase 1 compression (all 838 existing tests + 8
    quantize tests pass)

### Phase 2c: Spatial encoding

1.  VTR_ENC_DIFF: general signed differencing (~40 LOC)
2.  Auto-selection: DELTA vs DIFF vs PLAIN heuristic
3.  VTR_ENC_SPATIAL: predictor framework (left/up/average/Paeth)
4.  Plane predictor: tile-based least-squares + coefficient storage
5.  Wire spatial metadata into column chunk header
6.  R API: `spatial` parameter in write_vtr(), auto for tiff backend
7.  Benchmark suite: all pipeline combinations on temperature raster

### Benchmark targets

Compare against: - Phase 1 (current): shuffle + LZ/deflate on raw
float64 - terra DEFLATE (GeoTIFF baseline) - NetCDF-4 with shuffle +
deflate - Theoretical entropy bound (sum of log2 of unique residual
counts)

Target: ≥5× improvement over Phase 1 on smooth temperature rasters with
quantize + spatial + LZ.
