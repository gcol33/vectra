/*
 * src/model/plane2d.c
 *
 * TDC_MODEL_PLANE_2D — per-tile least-squares plane fit predictor for
 * RASTER_2D blocks.
 *
 * For each tile_size x tile_size tile of the raster, fit
 *
 *     pred(lx, ly) = a + b * lx + c * ly      (lx,ly = local tile coords)
 *
 * by solving the closed-form 3x3 normal equations
 *
 *     [ s_1   s_x   s_y  ] [a]   [s_v ]
 *     [ s_x   s_xx  s_xy ] [b] = [s_vx]
 *     [ s_y   s_xy  s_yy ] [c]   [s_vy]
 *
 * The (a, b, c) coefficients are stored as int32 with an implied 8-bit
 * fractional scale (round(coef * 256)) so the side-metadata footprint is
 * deterministic and small. The reconstructed predictor uses the SAME
 * fixed-point arithmetic, so encode/decode round-trip exactly under
 * modular wrap at the input dtype's width.
 *
 * Source: vectra/src/vtr_codec.c:1666-1786 (plane_encode, plane_decode).
 * The math kernel is preserved one-to-one. The wrapping is rewritten for
 * tdc:
 *   - tdc_buffer / realloc_fn allocation everywhere (no malloc, no
 *     longjmp on alloc failure)
 *   - typed dtype dispatch instead of vectra's int64-only path
 *   - side metadata is a delta-coded varint stream, not a flat i32 array
 *     (see plane2d_side_write for the 2D-predicted zigzag-LEB128 layout).
 *     For piecewise-planar inputs — the target shape of this model — the
 *     interior tiles predict exactly from their left neighbor, so every
 *     interior tile costs ~3 bytes (three single-byte zero deltas) instead
 *     of a flat 12 bytes. The previous flat i32 layout was the measured
 *     bottleneck behind SPEEDUP-TODO P0.1: on a 1024x1024 split-plane i32
 *     bench, residuals were already exactly zero but side_meta alone cost
 *     12 KB, dominating the compressed block.
 *   - bit-exact round trip via modular arithmetic at the input width,
 *     same convention as src/model/pred2d.c
 *
 * Accepted dtypes: i8, i16, i32, u8, u16, u32. (Same as PRED_2D and for
 * the same reason: 64-bit raster imagery is rare and the int64
 * accumulator inside the predictor cannot guard against overflow at
 * that width.) Floats are rejected — quantize first.
 *
 * Accepted layout: RASTER_2D only.
 *   ny = shape.dim[0]   (rows)
 *   nx = shape.dim[1]   (columns)
 *   idx = row * nx + col
 *
 * Validity bitmap: ignored, same convention as every other v0 model.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/model.h"
#include "tdc/codec.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"
#include "../core/log.h"
#include "../format/metadata_internal.h"

/* ----- SIMD capability detection ------------------------------------------ */
#if defined(__AVX2__) || (defined(_MSC_VER) && defined(__AVX2__))
#  include <immintrin.h>
#  define TDC_PLANE2D_HAVE_AVX2 1
#endif

#if defined(__SSE2__) || defined(_M_X64) || \
    (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  include <emmintrin.h>
#  define TDC_PLANE2D_HAVE_SSE2 1
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#  define TDC_PLANE2D_HAVE_NEON 1
#endif

/* ----- Zigzag LEB128 varint ---------------------------------------------- */
/*
 * Small inline codec for signed 32-bit integers. We emit the plane
 * coefficients (tile a, b, c) as 2D-predicted deltas; the deltas are
 * typically near zero on piecewise-planar input, so zigzag+LEB128 gives
 * one byte per coefficient for interior tiles and grows only where the
 * plane actually changes. Maximum encoded size for an int32 is 5 bytes.
 */
#define PLANE2D_VARINT_MAX_I32 5u

static inline uint32_t plane2d_zigzag_encode_i32(int32_t v) {
    return ((uint32_t)v << 1) ^ (uint32_t)(v >> 31);
}

static inline int32_t plane2d_zigzag_decode_u32(uint32_t v) {
    return (int32_t)((v >> 1) ^ (uint32_t)-(int32_t)(v & 1u));
}

/* Writes a zigzag-LEB128 varint for `v` to `p` (caller guarantees 5 bytes
 * of headroom). Returns the number of bytes written (1..5). */
static inline size_t plane2d_varint_write_i32(uint8_t *p, int32_t v) {
    uint32_t u = plane2d_zigzag_encode_i32(v);
    size_t n = 0;
    while (u >= 0x80u) {
        p[n++] = (uint8_t)((u & 0x7Fu) | 0x80u);
        u >>= 7;
    }
    p[n++] = (uint8_t)u;
    return n;
}

/* Reads a zigzag-LEB128 varint from r. On success, *out_val gets the
 * decoded int32 and the cursor advances. On overflow or EOF, r->ok is
 * cleared and *out_val is zero. Accepts up to 5 bytes per LEB128 spec
 * for a 32-bit payload. */
static inline void plane2d_varint_read_i32(tdc_meta_reader *r, int32_t *out_val) {
    uint32_t u = 0;
    unsigned shift = 0;
    *out_val = 0;
    for (unsigned i = 0; i < PLANE2D_VARINT_MAX_I32; ++i) {
        if (!r->ok || r->size - r->off < 1u) { r->ok = 0; return; }
        uint8_t b = r->base[r->off++];
        /* Guard against oversized final byte. At shift==28 only the low
         * 4 bits of the last byte carry payload — anything higher would
         * overflow uint32_t. */
        if (i == PLANE2D_VARINT_MAX_I32 - 1u && (b & 0xF0u) != 0u) {
            r->ok = 0; return;
        }
        u |= ((uint32_t)(b & 0x7Fu)) << shift;
        if ((b & 0x80u) == 0u) {
            *out_val = plane2d_zigzag_decode_u32(u);
            return;
        }
        shift += 7u;
    }
    /* Ran past the 5-byte budget without seeing a terminator. */
    r->ok = 0;
}

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ----- Acceptance bitmasks ------------------------------------------------ */

#define PLANE2D_ACCEPTED_DTYPES (         \
    TDC_DT_BIT(TDC_DT_I8)  |             \
    TDC_DT_BIT(TDC_DT_I16) |             \
    TDC_DT_BIT(TDC_DT_I32) |             \
    TDC_DT_BIT(TDC_DT_U8)  |             \
    TDC_DT_BIT(TDC_DT_U16) |             \
    TDC_DT_BIT(TDC_DT_U32))

#define PLANE2D_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_RASTER_2D)

#define PLANE2D_DEFAULT_TILE_SIZE 32u

/* Coefficients are stored as int32 with this fixed-point scale. The
 * inverse divides by COEFF_SCALE_F to recover the floating-point plane.
 * The 256x scale gives 8 bits of fractional precision, which is enough
 * for the predictor sub-pixel slope without overflowing int32 for any
 * realistic raster magnitude. */
#define PLANE2D_COEFF_SCALE_F 256.0

static int plane2d_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(PLANE2D_ACCEPTED_DTYPES, dt);
}

/* ----- Type-generic load / modular store --------------------------------- */
/*
 * Same convention as src/model/pred2d.c: load with sign- or zero-extend
 * into int64; store low-N bits via the unsigned counterpart so the wrap
 * is well-defined under C semantics.
 */
static inline int64_t plane2d_load(tdc_dtype dt, const uint8_t *base, int64_t i) {
    return tdc_model_load_int(dt, base, i);
}

static inline void plane2d_store(tdc_dtype dt, uint8_t *base, int64_t i, int64_t v) {
    tdc_model_store_int(dt, base, i, v);
}

/* ----- Tile geometry ------------------------------------------------------ */

typedef struct {
    uint32_t tiles_x;
    uint32_t tiles_y;
    uint32_t n_tiles;
} plane2d_tiling;

static plane2d_tiling plane2d_tile_count(int64_t nx, int64_t ny, uint32_t tile_size) {
    plane2d_tiling t;
    t.tiles_x = (uint32_t)(((uint64_t)nx + tile_size - 1u) / tile_size);
    t.tiles_y = (uint32_t)(((uint64_t)ny + tile_size - 1u) / tile_size);
    t.n_tiles = t.tiles_x * t.tiles_y;
    return t;
}

/* ----- Predictor evaluation ----------------------------------------------- */
/*
 * Reconstruct the int64 predictor value at local coordinates (lx, ly)
 * from the fixed-point plane coefficients. Computes
 *     round((a + b*lx + c*ly) / 256)
 * where (a, b, c) live in PLANE2D_COEFF_SCALE_F = 256-fixed-point.
 *
 * The reference implementation used double arithmetic + round() — three
 * int->double converts, two fmuls, an fadd, an fdiv, and a round() call
 * per pixel, which dominated decode at ~860 MB/s for i32 rasters. The
 * integer form below is exact (matches the encoder, which still uses
 * round() at fit time but only once per tile, not per pixel) and reduces
 * the per-pixel cost to two int64 multiply-adds and a shift.
 *
 * Round-to-nearest-half-away-from-zero matches C's round() semantics: for
 * a non-negative dividend, (sum + 128) / 256; for negative, the symmetric
 * branch flips signs. Using division by 256 (compiler folds to a shift on
 * 2's complement targets) avoids the implementation-defined behavior of
 * `>>` on signed values.
 */
static inline int64_t round_div256_i64(int64_t acc) {
    int64_t sign = acc >> 63;
    int64_t abs_acc = (acc ^ sign) - sign;
    int64_t q = (abs_acc + 128) >> 8;
    return (q ^ sign) - sign;
}

static inline int32_t round_div256_i32(int32_t acc) {
    int32_t sign = acc >> 31;
    int32_t abs_acc = (acc ^ sign) - sign;
    int32_t q = (abs_acc + 128) >> 8;
    return (q ^ sign) - sign;
}

static inline int64_t plane2d_eval(const int32_t *coeffs_for_tile,
                                   uint32_t lx, uint32_t ly) {
    int64_t a = (int64_t)coeffs_for_tile[0];
    int64_t b = (int64_t)coeffs_for_tile[1];
    int64_t c = (int64_t)coeffs_for_tile[2];
    int64_t sum = a + b * (int64_t)lx + c * (int64_t)ly;
    return round_div256_i64(sum);
}

/* ----- AVX2 tile kernels -------------------------------------------------- */
/*
 * 8-wide int32 round_div256 + store for the NORES and general tile paths.
 * The accumulator starts at (a + c*ly) for each row and steps by b per
 * pixel. The 8 lanes carry {acc, acc+b, acc+2b, ..., acc+7b} and step
 * by 8b each iteration.
 */
#ifdef TDC_PLANE2D_HAVE_AVX2

static inline __m256i avx2_round_div256_epi32(__m256i acc) {
    __m256i sign    = _mm256_srai_epi32(acc, 31);
    __m256i abs_acc = _mm256_sub_epi32(_mm256_xor_si256(acc, sign), sign);
    __m256i q       = _mm256_srli_epi32(
                          _mm256_add_epi32(abs_acc, _mm256_set1_epi32(128)),
                          8);
    return _mm256_sub_epi32(_mm256_xor_si256(q, sign), sign);
}

static void plane2d_nores_tile_avx2_u32(
        uint32_t *out, int64_t nx,
        uint32_t x0, uint32_t y0, uint32_t x1, uint32_t y1,
        int32_t a32, int32_t b32, int32_t c32)
{
    __m256i vb8  = _mm256_set1_epi32(b32 * 8);
    __m256i init = _mm256_set_epi32(
        b32 * 7, b32 * 6, b32 * 5, b32 * 4,
        b32 * 3, b32 * 2, b32 * 1, 0);

    for (uint32_t py = y0; py < y1; ++py) {
        uint32_t *row = out + (int64_t)py * nx;
        int32_t base = a32 + c32 * (int32_t)(py - y0);
        __m256i vacc = _mm256_add_epi32(_mm256_set1_epi32(base), init);
        uint32_t px = x0;
        for (; px + 8 <= x1; px += 8) {
            __m256i pred = avx2_round_div256_epi32(vacc);
            _mm256_storeu_si256((__m256i *)(row + px), pred);
            vacc = _mm256_add_epi32(vacc, vb8);
        }
        /* Scalar tail */
        if (px < x1) {
            int32_t acc = base + b32 * (int32_t)(px - x0);
            for (; px < x1; ++px) {
                row[px] = (uint32_t)round_div256_i32(acc);
                acc += b32;
            }
        }
    }
}

static void plane2d_nores_tile_avx2_u16(
        uint16_t *out, int64_t nx,
        uint32_t x0, uint32_t y0, uint32_t x1, uint32_t y1,
        int32_t a32, int32_t b32, int32_t c32)
{
    __m256i vb8  = _mm256_set1_epi32(b32 * 8);
    __m256i init = _mm256_set_epi32(
        b32 * 7, b32 * 6, b32 * 5, b32 * 4,
        b32 * 3, b32 * 2, b32 * 1, 0);

    for (uint32_t py = y0; py < y1; ++py) {
        uint16_t *row = out + (int64_t)py * nx;
        int32_t base = a32 + c32 * (int32_t)(py - y0);
        __m256i vacc = _mm256_add_epi32(_mm256_set1_epi32(base), init);
        uint32_t px = x0;
        for (; px + 8 <= x1; px += 8) {
            __m256i pred = avx2_round_div256_epi32(vacc);
            /* Pack 8×i32 → 8×i16: _mm256_packs_epi32 interleaves lanes,
             * so we need a permute to get the right order. */
            __m256i packed = _mm256_packs_epi32(pred, _mm256_setzero_si256());
            packed = _mm256_permute4x64_epi64(packed, 0xD8); /* 0,2,1,3 */
            _mm_storeu_si128((__m128i *)(row + px),
                             _mm256_castsi256_si128(packed));
            vacc = _mm256_add_epi32(vacc, vb8);
        }
        if (px < x1) {
            int32_t acc = base + b32 * (int32_t)(px - x0);
            for (; px < x1; ++px) {
                row[px] = (uint16_t)round_div256_i32(acc);
                acc += b32;
            }
        }
    }
}

static void plane2d_nores_tile_avx2_u8(
        uint8_t *out, int64_t nx,
        uint32_t x0, uint32_t y0, uint32_t x1, uint32_t y1,
        int32_t a32, int32_t b32, int32_t c32)
{
    __m256i vb8  = _mm256_set1_epi32(b32 * 8);
    __m256i init = _mm256_set_epi32(
        b32 * 7, b32 * 6, b32 * 5, b32 * 4,
        b32 * 3, b32 * 2, b32 * 1, 0);

    for (uint32_t py = y0; py < y1; ++py) {
        uint8_t *row = out + (int64_t)py * nx;
        int32_t base = a32 + c32 * (int32_t)(py - y0);
        __m256i vacc = _mm256_add_epi32(_mm256_set1_epi32(base), init);
        uint32_t px = x0;
        for (; px + 8 <= x1; px += 8) {
            __m256i pred = avx2_round_div256_epi32(vacc);
            /* i32 → i16 → u8 narrow: two-step pack. */
            __m256i p16 = _mm256_packs_epi32(pred, _mm256_setzero_si256());
            p16 = _mm256_permute4x64_epi64(p16, 0xD8);
            __m128i lo16 = _mm256_castsi256_si128(p16);
            __m128i p8 = _mm_packus_epi16(lo16, _mm_setzero_si128());
            /* Store lower 8 bytes. */
            _mm_storel_epi64((__m128i *)(row + px), p8);
            vacc = _mm256_add_epi32(vacc, vb8);
        }
        if (px < x1) {
            int32_t acc = base + b32 * (int32_t)(px - x0);
            for (; px < x1; ++px) {
                row[px] = (uint8_t)round_div256_i32(acc);
                acc += b32;
            }
        }
    }
}

/* General-case with residual (res != 0): AVX2 u32 path. */
static void plane2d_res_tile_avx2_u32(
        uint32_t *out, const uint32_t *res, int64_t nx,
        uint32_t x0, uint32_t y0, uint32_t x1, uint32_t y1,
        int32_t a32, int32_t b32, int32_t c32)
{
    __m256i vb8  = _mm256_set1_epi32(b32 * 8);
    __m256i init = _mm256_set_epi32(
        b32 * 7, b32 * 6, b32 * 5, b32 * 4,
        b32 * 3, b32 * 2, b32 * 1, 0);

    for (uint32_t py = y0; py < y1; ++py) {
        int64_t row_off = (int64_t)py * nx;
        uint32_t *orow = out + row_off;
        const uint32_t *rrow = res + row_off;
        int32_t base = a32 + c32 * (int32_t)(py - y0);
        __m256i vacc = _mm256_add_epi32(_mm256_set1_epi32(base), init);
        uint32_t px = x0;
        for (; px + 8 <= x1; px += 8) {
            __m256i pred = avx2_round_div256_epi32(vacc);
            __m256i rv   = _mm256_loadu_si256((const __m256i *)(rrow + px));
            _mm256_storeu_si256((__m256i *)(orow + px),
                                _mm256_add_epi32(rv, pred));
            vacc = _mm256_add_epi32(vacc, vb8);
        }
        if (px < x1) {
            int32_t acc = base + b32 * (int32_t)(px - x0);
            for (; px < x1; ++px) {
                int64_t idx = row_off + (int64_t)px;
                orow[px] = res[idx] + (uint32_t)round_div256_i32(acc);
                acc += b32;
            }
        }
    }
}

#endif /* TDC_PLANE2D_HAVE_AVX2 */

/* ----- Structural coefficient predictor ---------------------------------- */
/*
 * A tile's (a, b, c) are not independent of its neighbour: a neighbour
 * one tile to the right lives at x0 + tile_size, so for a globally-planar
 * region with slope b its constant term satisfies
 *
 *     a_right = a_left + b_left * tile_size
 *
 * and similarly moving one tile down by tile_size along y,
 *
 *     a_down  = a_top  + c_top  * tile_size.
 *
 * Both relations are exact in our int32 fixed-point (a_fp = 256*a,
 * b_fp * tile_size = 256*(b*tile_size) = the correct a_fp delta), so for
 * a single uniform plane the interior deltas are identically zero.
 *
 * This matters in practice: on the 1024x1024 split-plane i32 bench the
 * naive "copy previous tile" predictor still paid a 3-byte varint per
 * interior tile because `a` changed by a constant stride. The structural
 * predictor zeros it out and drops side_meta from ~5.1 KB to ~3.1 KB.
 *
 * `tile_size_step` is `tile_size` when the neighbour is a full tile
 * away. Partial trailing tiles at the right/bottom edge still use the
 * full tile_size for prediction even though the fit region is smaller,
 * because the fit reports coefficients in the same (a + b*lx + c*ly)
 * parameterization regardless of tile width — so the structural identity
 * still holds at the tile-origin offset.
 *
 * int64 intermediate guards against b*tile_size or c*tile_size
 * overflowing int32 on extreme slopes; the final cast to int32 wraps
 * modulo 2^32, matching the decoder's identical wrap. Encoder and
 * decoder share this helper verbatim so there is no drift.
 */
static inline void plane2d_predict_from_left(const int32_t prev[3],
                                              uint16_t      tile_size,
                                              int32_t       out[3]) {
    int64_t a = (int64_t)prev[0] + (int64_t)prev[1] * (int64_t)tile_size;
    out[0] = (int32_t)(uint32_t)(uint64_t)a;
    out[1] = prev[1];
    out[2] = prev[2];
}

static inline void plane2d_predict_from_top(const int32_t prev[3],
                                             uint16_t      tile_size,
                                             int32_t       out[3]) {
    int64_t a = (int64_t)prev[0] + (int64_t)prev[2] * (int64_t)tile_size;
    out[0] = (int32_t)(uint32_t)(uint64_t)a;
    out[1] = prev[1];
    out[2] = prev[2];
}

/* ----- Side metadata serialization ---------------------------------------- */
/*
 * Layout:
 *
 *     u16  tile_size
 *     u32  n_tiles
 *     varint-stream   2D-predicted zigzag-LEB128 deltas of (a, b, c),
 *                     n_tiles triples in row-major tile order.
 *
 * Prediction per tile (tx, ty):
 *
 *     tx > 0  -> predict from (tx-1, ty)     [left neighbour in same row]
 *     tx == 0, ty > 0 -> predict from (0, ty-1) [top of column 0]
 *     tx == 0, ty == 0 -> predict 0
 *
 * For piecewise-planar input (the model's target shape) interior tiles
 * all have identical coefficients to their left neighbour, so the stored
 * delta is (0, 0, 0) → three one-byte varints. The edge tiles where the
 * plane actually changes pay a few bytes of varint for the true delta.
 *
 * The previous flat `n_tiles * 3 * i32` layout paid 12 bytes per tile
 * regardless of content; on the 1024x1024 split-plane bench that cost
 * 12 KB of side_meta alone and was the dominant term in the final block
 * size. See SPEEDUP-TODO P0.1.
 */
/* Side-meta header:
 *   u16  tile_size
 *   u32  n_tiles
 *   u8   flags           (see PLANE2D_FLAG_*)
 *   u8   reserved        (MBZ; decoder rejects non-zero)
 *
 * Header bytes bumped from 6 → 8 in-place (no format version bump —
 * allowed by the prototype-phase policy in MEMORY.md). The flag byte
 * carries the zero-residual marker used by the Phase 1 decode fast
 * path. Old 6-byte blocks no longer decode. */
#define PLANE2D_META_HDR_BYTES 8u

/* Flag bit set by the encoder when every residual byte is zero. Tells
 * the decoder to skip the per-pixel residual read+add entirely and
 * write predictor values straight into the output. Saves one full
 * pass over the uncompressed output buffer on piecewise-planar inputs
 * where the plane fit is exact. */
#define PLANE2D_FLAG_NO_RESIDUAL 0x01u

static tdc_status plane2d_side_write(tdc_buffer *side_out,
                                     uint16_t        tile_size,
                                     uint32_t        tiles_x,
                                     uint32_t        tiles_y,
                                     uint8_t         flags,
                                     const int32_t  *coeffs) {
    uint32_t n_tiles = tiles_x * tiles_y;

    /* Worst-case reservation: header + 3 coefficients per tile *
     * 5 bytes each (int32 LEB128 upper bound). The final size_t is
     * set below after the stream is written. */
    size_t max_bytes = (size_t)PLANE2D_META_HDR_BYTES
                     + (size_t)n_tiles * 3u * (size_t)PLANE2D_VARINT_MAX_I32;
    tdc_status st = tdc_buf_reserve(side_out, max_bytes);
    if (st != TDC_OK) return st;

    uint8_t *base = side_out->data;
    tdc_le_store_u16(base + 0, tile_size);
    tdc_le_store_u32(base + 2, n_tiles);
    base[6] = flags;
    base[7] = 0u;           /* reserved */
    uint8_t *wp = base + PLANE2D_META_HDR_BYTES;

    /* We need the first tile in each row to fall back to the top-row
     * prediction, so cache the previous row's column-0 coefficients. */
    int32_t prev_row_first[3] = {0, 0, 0};
    int32_t prev_tile[3]      = {0, 0, 0};

    for (uint32_t ty = 0; ty < tiles_y; ++ty) {
        for (uint32_t tx = 0; tx < tiles_x; ++tx) {
            const int32_t *cf = coeffs + (size_t)(ty * tiles_x + tx) * 3u;
            int32_t pred[3];
            if (tx > 0) {
                plane2d_predict_from_left(prev_tile, tile_size, pred);
            } else if (ty > 0) {
                plane2d_predict_from_top(prev_row_first, tile_size, pred);
            } else {
                pred[0] = 0; pred[1] = 0; pred[2] = 0;
            }
            wp += plane2d_varint_write_i32(wp,
                (int32_t)((uint32_t)cf[0] - (uint32_t)pred[0]));
            wp += plane2d_varint_write_i32(wp,
                (int32_t)((uint32_t)cf[1] - (uint32_t)pred[1]));
            wp += plane2d_varint_write_i32(wp,
                (int32_t)((uint32_t)cf[2] - (uint32_t)pred[2]));

            prev_tile[0] = cf[0];
            prev_tile[1] = cf[1];
            prev_tile[2] = cf[2];
            if (tx == 0) {
                prev_row_first[0] = cf[0];
                prev_row_first[1] = cf[1];
                prev_row_first[2] = cf[2];
            }
        }
    }

    side_out->size = (size_t)(wp - base);
    return TDC_OK;
}

/* ----- Per-tile fit ------------------------------------------------------- */
/*
 * Closed-form 3-coefficient plane fit. Walks the tile once to accumulate
 * the symmetric normal-equation moments, solves the 3x3 system by
 * Cramer's rule, rounds to int32 fixed-point, and returns the resolved
 * coefficients via out_a/out_b/out_c.
 *
 * Degenerate cases (count < 3, det == 0): fall back to a constant plane
 * at the tile mean. count == 0 returns zeros (the tile is outside the
 * raster — should not happen given the tiles_x/tiles_y math, but is
 * handled defensively).
 */
static void plane2d_fit_tile(tdc_dtype dt, const uint8_t *src,
                             int64_t nx,
                             uint32_t x0, uint32_t y0,
                             uint32_t x1, uint32_t y1,
                             int32_t *out_a, int32_t *out_b, int32_t *out_c) {
    double s_1 = 0, s_x = 0, s_y = 0;
    double s_xx = 0, s_xy = 0, s_yy = 0;
    double s_v = 0, s_vx = 0, s_vy = 0;
    uint32_t count = 0;

    for (uint32_t py = y0; py < y1; ++py) {
        for (uint32_t px = x0; px < x1; ++px) {
            int64_t idx = (int64_t)py * nx + (int64_t)px;
            double v  = (double)plane2d_load(dt, src, idx);
            double lx = (double)(px - x0);
            double ly = (double)(py - y0);
            s_1  += 1.0;
            s_x  += lx;
            s_y  += ly;
            s_xx += lx * lx;
            s_xy += lx * ly;
            s_yy += ly * ly;
            s_v  += v;
            s_vx += v * lx;
            s_vy += v * ly;
            count++;
        }
    }

    double a = 0.0, b = 0.0, c = 0.0;
    if (count >= 3) {
        double det = s_1 * (s_xx * s_yy - s_xy * s_xy)
                   - s_x * (s_x  * s_yy - s_xy * s_y)
                   + s_y * (s_x  * s_xy - s_xx * s_y);
        if (det != 0.0 && det == det) {
            double inv_det = 1.0 / det;
            a = (s_v  * (s_xx * s_yy - s_xy * s_xy)
               - s_vx * (s_x  * s_yy - s_xy * s_y)
               + s_vy * (s_x  * s_xy - s_xx * s_y)) * inv_det;
            b = (s_1  * (s_vx * s_yy - s_vy * s_xy)
               - s_x  * (s_v  * s_yy - s_vy * s_y)
               + s_y  * (s_v  * s_xy - s_vx * s_y)) * inv_det;
            c = (s_1  * (s_xx * s_vy - s_xy * s_vx)
               - s_x  * (s_x  * s_vy - s_xy * s_v)
               + s_y  * (s_x  * s_vx - s_xx * s_v)) * inv_det;
        } else {
            a = s_v / s_1;
        }
    } else if (count > 0) {
        a = s_v / (double)count;
    }

    *out_a = (int32_t)round(a * PLANE2D_COEFF_SCALE_F);
    *out_b = (int32_t)round(b * PLANE2D_COEFF_SCALE_F);
    *out_c = (int32_t)round(c * PLANE2D_COEFF_SCALE_F);
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status plane2d_encode(const tdc_block *in,
                                 const void      *params,
                                 tdc_buffer      *residual_out,
                                 tdc_dtype       *residual_dtype,
                                 tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (in->shape.rank != 2)                return TDC_E_SHAPE;
    if (!plane2d_dtype_accepted(in->dtype)) return TDC_E_DTYPE;

    int64_t ny = in->shape.dim[0];
    int64_t nx = in->shape.dim[1];
    if (nx < 0 || ny < 0)                                          return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)                 return TDC_E_SHAPE;
    if (nx > (int64_t)UINT32_MAX || ny > (int64_t)UINT32_MAX)      return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    /* Resolve tile size. Default if no params or tile_size == 0. */
    uint16_t tile_size = PLANE2D_DEFAULT_TILE_SIZE;
    if (params) {
        const tdc_plane2d_params *p = (const tdc_plane2d_params *)params;
        if (p->tile_size != 0u) tile_size = p->tile_size;
    }

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * elem_size;

    /* Reserve residual output. Empty raster: write empty side_meta+residual
     * but still set residual_dtype so the chain plumbing is consistent. */
    if (residual_dtype) *residual_dtype = in->dtype;

    if (n == 0) {
        /* Self-describing empty record: header with n_tiles == 0, no
         * varint payload. Flags are zero — empty rasters don't need
         * the no-residual fast path because there's nothing to walk. */
        tdc_status st = plane2d_side_write(side_out, tile_size,
                                           0u, 0u, 0u, NULL);
        if (st != TDC_OK) return st;
        residual_out->size = 0;
        return TDC_OK;
    }
    if (!in->data) return TDC_E_INVAL;

    plane2d_tiling t = plane2d_tile_count(nx, ny, tile_size);
    if (t.tiles_x == 0u || t.tiles_y == 0u) return TDC_E_SHAPE;
    if (t.n_tiles / t.tiles_x != t.tiles_y) return TDC_E_SHAPE; /* mul overflow */

    /* Coefficients live in a scratch allocation for the lifetime of the
     * encode. We cannot reuse side_out any more because its final size
     * is variable (varint stream). The fit loop writes into this array,
     * the residual loop reads from it, and plane2d_side_write walks it
     * once to emit the delta-varint side_meta stream. */
    size_t coeff_bytes = (size_t)t.n_tiles * 3u * sizeof(int32_t);
    int32_t *coeffs = (int32_t *)residual_out->realloc_fn(
        residual_out->user, NULL, coeff_bytes);
    if (!coeffs) return TDC_E_NOMEM;
    memset(coeffs, 0, coeff_bytes);

    tdc_status st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) {
        (void)residual_out->realloc_fn(residual_out->user, coeffs, 0);
        return st;
    }

    const uint8_t *src_p = (const uint8_t *)in->data;
    uint8_t       *dst_p = residual_out->data;

    /* Fit and emit residuals one tile at a time. The two halves of each
     * tile share the same fixed-point arithmetic so the decoder
     * reproduces the exact same predictor. */
    for (uint32_t ty = 0; ty < t.tiles_y; ++ty) {
        for (uint32_t tx = 0; tx < t.tiles_x; ++tx) {
            uint32_t x0 = tx * tile_size;
            uint32_t y0 = ty * tile_size;
            uint32_t x1 = x0 + tile_size; if (x1 > (uint32_t)nx) x1 = (uint32_t)nx;
            uint32_t y1 = y0 + tile_size; if (y1 > (uint32_t)ny) y1 = (uint32_t)ny;

            uint32_t tidx = ty * t.tiles_x + tx;
            int32_t *cf   = coeffs + (size_t)tidx * 3u;
            plane2d_fit_tile(in->dtype, src_p, nx, x0, y0, x1, y1,
                             &cf[0], &cf[1], &cf[2]);

            for (uint32_t py = y0; py < y1; ++py) {
                for (uint32_t px = x0; px < x1; ++px) {
                    int64_t idx  = (int64_t)py * nx + (int64_t)px;
                    int64_t val  = plane2d_load(in->dtype, src_p, idx);
                    int64_t pred = plane2d_eval(cf, px - x0, py - y0);
                    plane2d_store(in->dtype, dst_p, idx, val - pred);
                }
            }
        }
    }

    /* Scan residual for all-zero. On piecewise-planar input the plane
     * fit is exact and every residual byte is zero; setting
     * PLANE2D_FLAG_NO_RESIDUAL signals two things:
     *   (a) the model side meta carries the flag so the decoder's
     *       tile inner loop can skip the per-pixel residual load (a
     *       ~12% speedup on its own), and
     *   (b) the model emits a zero-byte residual stream, which the
     *       driver recognizes as the whole-pipeline skip case: the
     *       block record sets TDC_BLOCK_FLAG_ZERO_RESIDUAL, and the
     *       xform + entropy chains are never invoked. On decode the
     *       driver reconstructs a zero-filled residual of the correct
     *       size before handing it to plane2d_decode.
     * First-nonzero early-out keeps the scan cost negligible on non-
     * planar inputs. */
    uint8_t flags = 0u;
    {
        size_t i = 0;
        for (; i < bytes; ++i) { if (dst_p[i]) break; }
        if (i == bytes) {
            flags |= PLANE2D_FLAG_NO_RESIDUAL;
        }
    }
    residual_out->size = (flags & PLANE2D_FLAG_NO_RESIDUAL) ? 0u : bytes;

    /* Serialize the side metadata as a delta-varint stream, then free
     * the scratch coefficient array. side_out->size is set by
     * plane2d_side_write to the exact stream length. */
    st = plane2d_side_write(side_out, tile_size, t.tiles_x, t.tiles_y,
                            flags, coeffs);
    (void)residual_out->realloc_fn(residual_out->user, coeffs, 0);
    if (st != TDC_OK) return st;

    /* Optional diagnostic: gated by env var TDC_PLANE2D_DEBUG. Reports
     * residual energy + side meta footprint so we can attribute the
     * compressed size between (a) tile fit quality, (b) per-tile meta
     * overhead. Persistent diagnostic, off in normal runs. */
    if (getenv("TDC_PLANE2D_DEBUG")) {
        uint64_t n_nonzero = 0;
        uint64_t energy    = 0;
        for (int64_t i = 0; i < n; ++i) {
            int64_t r = plane2d_load(in->dtype, dst_p, i);
            if (r != 0) {
                n_nonzero++;
                energy += (uint64_t)(r < 0 ? -r : r);
            }
        }
        TDC_LOG("[plane2d] tile_size=%u n_tiles=%u side_meta=%zu B "
                "residual_bytes=%zu n_nonzero=%llu energy=%llu\n",
                (unsigned)tile_size, (unsigned)t.n_tiles, side_out->size, bytes,
                (unsigned long long)n_nonzero, (unsigned long long)energy);
    }

    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status plane2d_decode(tdc_block      *out,
                                 const void     *params,
                                 tdc_dtype       residual_dtype,
                                 const uint8_t  *residuals, size_t residual_size,
                                 const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (out->shape.rank != 2)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!plane2d_dtype_accepted(out->dtype)) return TDC_E_DTYPE;

    int64_t ny = out->shape.dim[0];
    int64_t nx = out->shape.dim[1];
    if (nx < 0 || ny < 0)                                          return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)                 return TDC_E_SHAPE;
    if (nx > (int64_t)UINT32_MAX || ny > (int64_t)UINT32_MAX)      return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * elem_size;

    /* Empty raster is a special case: the side_meta is the 8-byte header
     * with tile_size > 0 and n_tiles == 0, and there is no varint stream
     * to walk. Residual must be empty. Handle inline. */
    if (n == 0) {
        if (side_size != (size_t)PLANE2D_META_HDR_BYTES) return TDC_E_CORRUPT;
        if (residual_size != 0)                          return TDC_E_CORRUPT;
        tdc_meta_reader hdr = tdc_meta_reader_init(side_meta, side_size);
        uint16_t ts = tdc_meta_read_u16(&hdr);
        uint32_t nt = tdc_meta_read_u32(&hdr);
        uint8_t  fl = tdc_meta_read_u8(&hdr);
        uint8_t  rs = tdc_meta_read_u8(&hdr);
        if (!hdr.ok || ts == 0u || nt != 0u || rs != 0u) return TDC_E_CORRUPT;
        (void)fl;   /* flags irrelevant on empty raster */
        return TDC_OK;
    }
    if (!out->data) return TDC_E_INVAL;

    /* Parse the header, then walk the side_meta and the raster tile by
     * tile. For each tile we read three zigzag-LEB128 deltas, apply the
     * 2D left/top prediction to recover the int32 coefficients, and
     * immediately reconstruct the pixels of that tile. This keeps the
     * decode allocation-free: we never materialize the full coefficient
     * table, only the current tile and the one-row-worth of "previous
     * column 0" state needed for top-prediction at row transitions. */
    tdc_meta_reader r = tdc_meta_reader_init(side_meta, side_size);
    uint16_t tile_size    = tdc_meta_read_u16(&r);
    uint32_t n_tiles_meta = tdc_meta_read_u32(&r);
    uint8_t  flags        = tdc_meta_read_u8(&r);
    uint8_t  reserved     = tdc_meta_read_u8(&r);
    if (!r.ok || tile_size == 0u || reserved != 0u) return TDC_E_CORRUPT;

    /* Residual is always the full uncompressed-size stream (same contract
     * as every other model). PLANE2D_FLAG_NO_RESIDUAL is a decoder-only
     * hint: it tells the tile loop the residual is all-zero so it can
     * skip the per-pixel load+add. The bytes still flow through the
     * entropy+xform chain. End-to-end residual-skip requires a driver
     * change (src/api/decode.c enforces walk_bytes == uncompressed_size)
     * and is out of scope for Phase 1 — see PLANE2D-DECODE-SPEEDUP.md. */
    if (residual_size != bytes) return TDC_E_CORRUPT;
    if (!residuals)             return TDC_E_INVAL;
    int no_residual = (flags & PLANE2D_FLAG_NO_RESIDUAL) != 0;

    plane2d_tiling t = plane2d_tile_count(nx, ny, tile_size);
    if (t.tiles_x == 0u || t.tiles_y == 0u) return TDC_E_CORRUPT;
    if (t.n_tiles / t.tiles_x != t.tiles_y) return TDC_E_CORRUPT;
    if (t.n_tiles != n_tiles_meta)          return TDC_E_CORRUPT;

    /* The first tile in each row needs the column-0 coefficients from
     * the previous row for its top-predictor. We store those in a small
     * stack triple that rolls over row by row. */
    int32_t prev_row_first[3] = {0, 0, 0};
    int32_t prev_tile[3]      = {0, 0, 0};

    /* Width-based dispatch: the tile inner loop reads a residual, adds the
     * fixed-point predictor evaluated at (lx, ly), and stores the result
     * with modular wrap. Sign is irrelevant — modular arithmetic on the
     * low N bits produces the same byte pattern for signed and unsigned
     * dtypes alike — so we only need three branches (1/2/4 byte width)
     * using unsigned typed pointers, instead of a per-pixel switch on
     * dtype that the previous version paid every iteration. The dispatch
     * happens once per tile, which is negligible against the inner pixel
     * loop.
     *
     * Strength reduction: plane2d_eval has the form
     *     pred(lx, ly) = round_div256(a + b*lx + c*ly)
     * which is 2 int64 multiplies per pixel if called directly. Per row
     * we hoist `a + c*ly` into `base` (kills the c*ly multiply, which is
     * row-constant), and within a row we replace `b*lx` with an additive
     * accumulator `acc` that starts at `base` and adds `b` per pixel
     * (kills the b*lx multiply). The rounding branch is kept identical
     * to plane2d_eval so encode and decode agree bit-for-bit; the
     * encoder still calls plane2d_eval so the two paths compute the
     * same rounded values. */
    /* AVX2 dispatch for the int32-safe NORES path. Falls back to scalar
     * when AVX2 is not compiled in or when T_U isn't one of the three
     * supported widths (can't happen for the three case labels, but the
     * macro is generic). */
#ifdef TDC_PLANE2D_HAVE_AVX2
    #define PLANE2D_NORES_I32_DISPATCH(T_U) do { \
        if (sizeof(T_U) == 4u) { \
            plane2d_nores_tile_avx2_u32((uint32_t *)out_typed, nx, \
                                        x0, y0, x1, y1, cf[0], cf[1], cf[2]); \
        } else if (sizeof(T_U) == 2u) { \
            plane2d_nores_tile_avx2_u16((uint16_t *)out_typed, nx, \
                                        x0, y0, x1, y1, cf[0], cf[1], cf[2]); \
        } else if (sizeof(T_U) == 1u) { \
            plane2d_nores_tile_avx2_u8((uint8_t *)out_typed, nx, \
                                       x0, y0, x1, y1, cf[0], cf[1], cf[2]); \
        } \
    } while (0)
    #define PLANE2D_RES_I32_DISPATCH(T_U) do { \
        if (sizeof(T_U) == 4u) { \
            plane2d_res_tile_avx2_u32((uint32_t *)out_typed, \
                                      (const uint32_t *)res_typed, nx, \
                                      x0, y0, x1, y1, cf[0], cf[1], cf[2]); \
        } else { \
            int32_t a32 = cf[0], b32 = cf[1], c32 = cf[2]; \
            for (uint32_t py = y0; py < y1; ++py) { \
                int64_t row_off = (int64_t)py * nx; \
                int32_t base = a32 + c32 * (int32_t)(py - y0); \
                int32_t acc  = base; \
                for (uint32_t px = x0; px < x1; ++px) { \
                    int64_t idx = row_off + (int64_t)px; \
                    int32_t pred = round_div256_i32(acc); \
                    out_typed[idx] = (T_U)((T_U)res_typed[idx] + (T_U)pred); \
                    acc += b32; \
                } \
            } \
        } \
    } while (0)
#else
    #define PLANE2D_NORES_I32_DISPATCH(T_U) do { \
        int32_t a32 = cf[0], b32 = cf[1], c32 = cf[2]; \
        for (uint32_t py = y0; py < y1; ++py) { \
            int64_t row_off = (int64_t)py * nx; \
            int32_t base = a32 + c32 * (int32_t)(py - y0); \
            int32_t acc  = base; \
            for (uint32_t px = x0; px < x1; ++px) { \
                out_typed[row_off + (int64_t)px] = \
                    (T_U)round_div256_i32(acc); \
                acc += b32; \
            } \
        } \
    } while (0)
    #define PLANE2D_RES_I32_DISPATCH(T_U) do { \
        int32_t a32 = cf[0], b32 = cf[1], c32 = cf[2]; \
        for (uint32_t py = y0; py < y1; ++py) { \
            int64_t row_off = (int64_t)py * nx; \
            int32_t base = a32 + c32 * (int32_t)(py - y0); \
            int32_t acc  = base; \
            for (uint32_t px = x0; px < x1; ++px) { \
                int64_t idx = row_off + (int64_t)px; \
                int32_t pred = round_div256_i32(acc); \
                out_typed[idx] = (T_U)((T_U)res_typed[idx] + (T_U)pred); \
                acc += b32; \
            } \
        } \
    } while (0)
#endif

    /* Phase 2: branch-free rounding, constant-tile + int32 fast paths.
     *
     * Three tiers:
     *   (a) cf_b == 0 && cf_c == 0: compute pred once, memset-style fill.
     *   (b) |cf_a| + |cf_b|*(tw-1) + |cf_c|*(th-1) <= INT32_MAX: int32
     *       accumulator, branch-free round_div256_i32. Compiler can
     *       auto-vectorize with 4-wide SSE2 or 8-wide AVX2.
     *   (c) Fallback: int64 accumulator, branch-free round_div256_i64.
     *
     * The guard (b) is tested once per tile. */

    #define PLANE2D_DEC_TILE_BODY(T_U) do { \
        T_U       *out_typed = (T_U *)out->data; \
        const T_U *res_typed = (const T_U *)residuals; \
        int64_t cf_a = (int64_t)cf[0]; \
        int64_t cf_b = (int64_t)cf[1]; \
        int64_t cf_c = (int64_t)cf[2]; \
        uint32_t tw = x1 - x0, th = y1 - y0; \
        int64_t abs_a = cf_a < 0 ? -cf_a : cf_a; \
        int64_t abs_b = cf_b < 0 ? -cf_b : cf_b; \
        int64_t abs_c = cf_c < 0 ? -cf_c : cf_c; \
        int64_t max_acc = abs_a + abs_b * (int64_t)(tw > 0 ? tw - 1 : 0) \
                                + abs_c * (int64_t)(th > 0 ? th - 1 : 0); \
        if (cf_b == 0 && cf_c == 0) { \
            T_U val = (T_U)round_div256_i64(cf_a); \
            for (uint32_t py = y0; py < y1; ++py) { \
                int64_t row_off = (int64_t)py * nx; \
                for (uint32_t px = x0; px < x1; ++px) \
                    out_typed[row_off + (int64_t)px] = \
                        (T_U)((T_U)res_typed[row_off + (int64_t)px] + val); \
            } \
        } else if (max_acc <= INT32_MAX) { \
            PLANE2D_RES_I32_DISPATCH(T_U); \
        } else { \
            for (uint32_t py = y0; py < y1; ++py) { \
                int64_t row_off = (int64_t)py * nx; \
                int64_t base = cf_a + cf_c * (int64_t)(py - y0); \
                int64_t acc  = base; \
                for (uint32_t px = x0; px < x1; ++px) { \
                    int64_t idx = row_off + (int64_t)px; \
                    int64_t pred = round_div256_i64(acc); \
                    out_typed[idx] = (T_U)((T_U)res_typed[idx] + (T_U)pred); \
                    acc += cf_b; \
                } \
            } \
        } \
    } while (0)

    #define PLANE2D_DEC_TILE_BODY_NORES(T_U) do { \
        T_U *out_typed = (T_U *)out->data; \
        int64_t cf_a = (int64_t)cf[0]; \
        int64_t cf_b = (int64_t)cf[1]; \
        int64_t cf_c = (int64_t)cf[2]; \
        uint32_t tw = x1 - x0, th = y1 - y0; \
        int64_t abs_a = cf_a < 0 ? -cf_a : cf_a; \
        int64_t abs_b = cf_b < 0 ? -cf_b : cf_b; \
        int64_t abs_c = cf_c < 0 ? -cf_c : cf_c; \
        int64_t max_acc = abs_a + abs_b * (int64_t)(tw > 0 ? tw - 1 : 0) \
                                + abs_c * (int64_t)(th > 0 ? th - 1 : 0); \
        if (cf_b == 0 && cf_c == 0) { \
            T_U val = (T_U)round_div256_i64(cf_a); \
            for (uint32_t py = y0; py < y1; ++py) { \
                int64_t row_off = (int64_t)py * nx; \
                for (uint32_t px = x0; px < x1; ++px) \
                    out_typed[row_off + (int64_t)px] = val; \
            } \
        } else if (max_acc <= INT32_MAX) { \
            PLANE2D_NORES_I32_DISPATCH(T_U); \
        } else { \
            for (uint32_t py = y0; py < y1; ++py) { \
                int64_t row_off = (int64_t)py * nx; \
                int64_t base = cf_a + cf_c * (int64_t)(py - y0); \
                int64_t acc  = base; \
                for (uint32_t px = x0; px < x1; ++px) { \
                    out_typed[row_off + (int64_t)px] = \
                        (T_U)round_div256_i64(acc); \
                    acc += cf_b; \
                } \
            } \
        } \
    } while (0)

    for (uint32_t ty = 0; ty < t.tiles_y; ++ty) {
        for (uint32_t tx = 0; tx < t.tiles_x; ++tx) {
            int32_t pred_coef[3];
            if (tx > 0) {
                plane2d_predict_from_left(prev_tile, tile_size, pred_coef);
            } else if (ty > 0) {
                plane2d_predict_from_top(prev_row_first, tile_size, pred_coef);
            } else {
                pred_coef[0] = 0; pred_coef[1] = 0; pred_coef[2] = 0;
            }

            int32_t da = 0, db = 0, dc = 0;
            plane2d_varint_read_i32(&r, &da);
            plane2d_varint_read_i32(&r, &db);
            plane2d_varint_read_i32(&r, &dc);
            if (!r.ok) return TDC_E_CORRUPT;

            int32_t cf[3];
            /* Unsigned wrap mirrors the encoder's (cf - pred) path. */
            cf[0] = (int32_t)((uint32_t)pred_coef[0] + (uint32_t)da);
            cf[1] = (int32_t)((uint32_t)pred_coef[1] + (uint32_t)db);
            cf[2] = (int32_t)((uint32_t)pred_coef[2] + (uint32_t)dc);

            uint32_t x0 = tx * tile_size;
            uint32_t y0 = ty * tile_size;
            uint32_t x1 = x0 + tile_size; if (x1 > (uint32_t)nx) x1 = (uint32_t)nx;
            uint32_t y1 = y0 + tile_size; if (y1 > (uint32_t)ny) y1 = (uint32_t)ny;

            if (no_residual) {
                switch (elem_size) {
                    case 1u: PLANE2D_DEC_TILE_BODY_NORES(uint8_t);  break;
                    case 2u: PLANE2D_DEC_TILE_BODY_NORES(uint16_t); break;
                    case 4u: PLANE2D_DEC_TILE_BODY_NORES(uint32_t); break;
                    default: return TDC_E_DTYPE;
                }
            } else switch (elem_size) {
                case 1u: PLANE2D_DEC_TILE_BODY(uint8_t);  break;
                case 2u: PLANE2D_DEC_TILE_BODY(uint16_t); break;
                case 4u: PLANE2D_DEC_TILE_BODY(uint32_t); break;
                default: return TDC_E_DTYPE;
            }

            prev_tile[0] = cf[0];
            prev_tile[1] = cf[1];
            prev_tile[2] = cf[2];
            if (tx == 0) {
                prev_row_first[0] = cf[0];
                prev_row_first[1] = cf[1];
                prev_row_first[2] = cf[2];
            }
        }
    }
    #undef PLANE2D_DEC_TILE_BODY
    #undef PLANE2D_DEC_TILE_BODY_NORES
    #undef PLANE2D_NORES_I32_DISPATCH
    #undef PLANE2D_RES_I32_DISPATCH

    /* The varint stream must consume the full side_meta exactly. */
    if (r.off != side_size) return TDC_E_CORRUPT;

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_plane2d_vt = {
    .id               = TDC_MODEL_PLANE_2D,
    .name             = "plane2d",
    .accepted_dtypes  = PLANE2D_ACCEPTED_DTYPES,
    .accepted_layouts = PLANE2D_ACCEPTED_LAYOUTS,
    .encode           = plane2d_encode,
    .decode           = plane2d_decode,
};
