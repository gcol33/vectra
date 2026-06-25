/*
 * src/model/fpc1d.c
 *
 * TDC_MODEL_FPC_1D — FPC-style dual predictor for float time series.
 *
 * Based on: Burtscher & Ratanaworabhan, "FPC: A High-Speed Compressor
 * for Double-Precision Floating-Point Data", IEEE Trans. Computers, 2009.
 *
 * Two hash-table predictors run in parallel:
 *   FCM  (Finite Context Method):  predicts x[i] = table_fcm[hash(x[i-1])]
 *   DFCM (Differential FCM):       predicts x[i] = x[i-1] + table_dfcm[hash(delta)]
 *
 * For each element the encoder picks the prediction whose XOR with the
 * actual value has more leading zero bytes. The residual stream is the
 * XOR of the winning prediction with the actual — same size and dtype as
 * the input. The 1-bit per-element selector (0=FCM, 1=DFCM) is stored
 * in side metadata.
 *
 * This fits the tdc stage-layering contract: the model produces a
 * fixed-width residual (n_elems * elem_size bytes) at the same dtype as
 * the input. Byte shuffle then groups the (many) leading zero bytes of
 * each residual together, and LZ/Huffman/LANE compresses them efficiently.
 *
 * Side metadata layout:
 *   u8   log2_table_size
 *   u8   selector_bits[ceil(n_elems / 8)]   (bit i = selector for element i)
 *
 * The selector bitstream is needed on decode because each element's
 * residual must be XORed with the same predictor that was chosen on encode.
 * The hash tables are rebuilt from the reconstructed data during decode
 * (same sequence of updates as encode), so they do not need to be stored.
 *
 * Hash table size: 1024 entries (log2 = 10). 8 KiB per table for f64.
 *
 * Float dtypes only: f16, f32, f64.
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>   /* calloc/free: decode-side scratch (no realloc_fn on
                       * model decode vtable — same pattern as lz_streams.c).
                       * Encode side uses residual_out->realloc_fn. */
#include <string.h>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

/* ----- Acceptance bitmasks ------------------------------------------------- */

#define FPC_ACCEPTED_DTYPES (              \
    TDC_DT_BIT(TDC_DT_F16) |              \
    TDC_DT_BIT(TDC_DT_F32) |              \
    TDC_DT_BIT(TDC_DT_F64))

#define FPC_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D)

static int fpc_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(FPC_ACCEPTED_DTYPES, dt);
}

/* ----- Configuration ------------------------------------------------------- */

#define FPC_LOG2_TABLE_DEFAULT 10  /* 1024 entries */
#define FPC_TABLE_SIZE(log2)   ((size_t)1u << (log2))
#define FPC_TABLE_MASK(log2)   (FPC_TABLE_SIZE(log2) - 1u)

/* ----- Hash function ------------------------------------------------------- */

static inline uint32_t fpc_hash(uint64_t bits, unsigned table_bits) {
    uint64_t h = bits * 0x9E3779B97F4A7C15ULL;
    return (uint32_t)(h >> (64u - table_bits));
}

/* ----- Leading zero byte count --------------------------------------------- *
 *
 * Hot path: called twice per input element on the encode side. Replacing
 * the scalar byte-shift loop with a hardware CLZ drops ~7 dependent ops
 * per call on f64 down to one instruction. Decode doesn't need it — the
 * winner is read from the selector bitstream.
 *
 * For width W < 8, the caller passes a uint64_t whose high (8-W) bytes
 * are zero. clz on a sub-W-byte value would return >= (8-W)*8; we subtract
 * that offset to recover the true count. The v == 0 case is guarded
 * explicitly because clz(0) is undefined on both intrinsics.
 */
static inline unsigned fpc_clz64(uint64_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return (unsigned)__builtin_clzll(v);
#elif defined(_MSC_VER)
    unsigned long idx;
    _BitScanReverse64(&idx, v);
    return 63u - (unsigned)idx;
#else
    unsigned n = 0;
    while ((v & 0x8000000000000000ULL) == 0) { v <<= 1; n++; }
    return n;
#endif
}

static inline unsigned count_lzb(uint64_t v, unsigned width) {
    if (v == 0) return width;
    unsigned shift_bits = (8u - width) * 8u;
    return (fpc_clz64(v) - shift_bits) >> 3;
}

/* ----- Encode kernel (macro-generated per width) --------------------------- */

#define DEFINE_FPC_ENCODE(SUFFIX, UT, W)                                      \
static void fpc_encode_##SUFFIX(const uint8_t *src, uint8_t *dst,             \
                                uint8_t *selectors, int64_t n,                \
                                unsigned log2_table,                          \
                                UT *fcm_table, UT *dfcm_table)               \
{                                                                             \
    const unsigned tbl_mask = FPC_TABLE_MASK(log2_table);                     \
    UT prev_bits = 0;                                                         \
    uint32_t fcm_idx = 0;                                                     \
    uint32_t dfcm_idx = 0;                                                    \
                                                                              \
    for (int64_t i = 0; i < n; ++i) {                                        \
        UT cur_bits;                                                          \
        memcpy(&cur_bits, src + (size_t)i * (W), (W));                       \
                                                                              \
        /* FCM prediction. */                                                \
        UT fcm_xor  = cur_bits ^ fcm_table[fcm_idx];                        \
        unsigned fcm_lzb = count_lzb((uint64_t)fcm_xor, (W));               \
                                                                              \
        /* DFCM prediction. */                                               \
        UT dfcm_pred = prev_bits + dfcm_table[dfcm_idx];                     \
        UT dfcm_xor  = cur_bits ^ dfcm_pred;                                \
        unsigned dfcm_lzb = count_lzb((uint64_t)dfcm_xor, (W));             \
                                                                              \
        /* Pick winner. Tie goes to DFCM (trend predictor). */               \
        unsigned use_dfcm = (unsigned)(dfcm_lzb >= fcm_lzb);                 \
        UT chosen_xor = use_dfcm ? dfcm_xor : fcm_xor;                      \
                                                                              \
        /* Write residual (same width as input). */                          \
        memcpy(dst + (size_t)i * (W), &chosen_xor, (W));                    \
                                                                              \
        /* Branchless selector write. OR'ing 0 is a no-op when FCM wins. */  \
        selectors[(size_t)i >> 3u] |=                                        \
            (uint8_t)(use_dfcm << ((size_t)i & 7u));                         \
                                                                              \
        /* Update tables. */                                                 \
        fcm_table[fcm_idx] = cur_bits;                                       \
        UT cur_delta = cur_bits - prev_bits;                                  \
        dfcm_table[dfcm_idx] = cur_delta;                                    \
                                                                              \
        fcm_idx  = fpc_hash((uint64_t)cur_bits,  log2_table) & tbl_mask;    \
        dfcm_idx = fpc_hash((uint64_t)cur_delta, log2_table) & tbl_mask;    \
                                                                              \
        prev_bits = cur_bits;                                                 \
    }                                                                         \
}

/* ----- Decode kernel ------------------------------------------------------- */

#define DEFINE_FPC_DECODE(SUFFIX, UT, W)                                      \
static void fpc_decode_##SUFFIX(const uint8_t *residuals,                     \
                                const uint8_t *selectors,                     \
                                uint8_t *dst, int64_t n,                      \
                                unsigned log2_table,                          \
                                UT *fcm_table, UT *dfcm_table)               \
{                                                                             \
    const unsigned tbl_mask = FPC_TABLE_MASK(log2_table);                     \
    UT prev_bits = 0;                                                         \
    uint32_t fcm_idx = 0;                                                     \
    uint32_t dfcm_idx = 0;                                                    \
                                                                              \
    for (int64_t i = 0; i < n; ++i) {                                        \
        UT chosen_xor;                                                        \
        memcpy(&chosen_xor, residuals + (size_t)i * (W), (W));              \
                                                                              \
        int use_dfcm = (selectors[(size_t)i / 8u] >> ((size_t)i % 8u)) & 1; \
                                                                              \
        UT pred;                                                              \
        if (use_dfcm) {                                                      \
            pred = prev_bits + dfcm_table[dfcm_idx];                         \
        } else {                                                              \
            pred = fcm_table[fcm_idx];                                       \
        }                                                                     \
        UT cur_bits = pred ^ chosen_xor;                                      \
        memcpy(dst + (size_t)i * (W), &cur_bits, (W));                       \
                                                                              \
        /* Update tables (same as encode). */                                \
        fcm_table[fcm_idx] = cur_bits;                                       \
        UT cur_delta = cur_bits - prev_bits;                                  \
        dfcm_table[dfcm_idx] = cur_delta;                                    \
                                                                              \
        fcm_idx  = fpc_hash((uint64_t)cur_bits,  log2_table) & tbl_mask;    \
        dfcm_idx = fpc_hash((uint64_t)cur_delta, log2_table) & tbl_mask;    \
                                                                              \
        prev_bits = cur_bits;                                                 \
    }                                                                         \
}

DEFINE_FPC_ENCODE(f16, uint16_t, 2u)
DEFINE_FPC_ENCODE(f32, uint32_t, 4u)
DEFINE_FPC_ENCODE(f64, uint64_t, 8u)

DEFINE_FPC_DECODE(f16, uint16_t, 2u)
DEFINE_FPC_DECODE(f32, uint32_t, 4u)
DEFINE_FPC_DECODE(f64, uint64_t, 8u)

#undef DEFINE_FPC_ENCODE
#undef DEFINE_FPC_DECODE

/* ----- Scratch table allocation helpers ------------------------------------ */

/*
 * Allocate two zero-initialized hash tables via realloc_fn (encode) or
 * libc calloc (decode). Returns TDC_OK or TDC_E_NOMEM.
 */
static tdc_status fpc_alloc_tables_enc(tdc_buffer *buf,
                                       size_t elem_size, unsigned log2_table,
                                       void **fcm_out, void **dfcm_out) {
    size_t tbl_bytes = FPC_TABLE_SIZE(log2_table) * elem_size;
    uint8_t *p = (uint8_t *)buf->realloc_fn(buf->user, NULL, tbl_bytes * 2u);
    if (!p) return TDC_E_NOMEM;
    memset(p, 0, tbl_bytes * 2u);
    *fcm_out  = p;
    *dfcm_out = p + tbl_bytes;
    return TDC_OK;
}

static void fpc_free_tables_enc(tdc_buffer *buf, void *tables) {
    if (tables) buf->realloc_fn(buf->user, tables, 0);
}

static tdc_status fpc_alloc_tables_dec(size_t elem_size, unsigned log2_table,
                                       void **fcm_out, void **dfcm_out) {
    size_t tbl_bytes = FPC_TABLE_SIZE(log2_table) * elem_size;
    uint8_t *p = (uint8_t *)calloc(1, tbl_bytes * 2u);
    if (!p) return TDC_E_NOMEM;
    *fcm_out  = p;
    *dfcm_out = p + tbl_bytes;
    return TDC_OK;
}

/* ----- Encode entry point -------------------------------------------------- */

static tdc_status fpc_encode(const tdc_block *in,
                             const void      *params,
                             tdc_buffer      *residual_out,
                             tdc_dtype       *residual_dtype,
                             tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;
    if (!fpc_dtype_accepted(in->dtype))     return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    if (residual_dtype) *residual_dtype = in->dtype;

    if (n == 0) {
        residual_out->size = 0;
        side_out->size = 0;
        return TDC_OK;
    }
    if (!in->data) return TDC_E_INVAL;

    const unsigned log2_table = FPC_LOG2_TABLE_DEFAULT;

    /* Residual: same size as input. */
    size_t residual_bytes = (size_t)n * elem_size;
    tdc_status st = tdc_buf_reserve(residual_out, residual_bytes);
    if (st != TDC_OK) return st;

    /* Side metadata: 1 byte log2_table + ceil(n/8) selector bytes. */
    size_t sel_bytes = ((size_t)n + 7u) / 8u;
    size_t side_bytes = 1u + sel_bytes;
    st = tdc_buf_reserve(side_out, side_bytes);
    if (st != TDC_OK) return st;

    uint8_t *side = side_out->data;
    side[0] = (uint8_t)log2_table;
    memset(side + 1, 0, sel_bytes);

    /* Allocate hash tables via realloc_fn. */
    void *fcm_raw = NULL, *dfcm_raw = NULL;
    st = fpc_alloc_tables_enc(residual_out, elem_size, log2_table,
                              &fcm_raw, &dfcm_raw);
    if (st != TDC_OK) return st;

    const uint8_t *src = (const uint8_t *)in->data;
    uint8_t       *dst = residual_out->data;
    uint8_t       *sel = side + 1;

    switch (in->dtype) {
        case TDC_DT_F16:
            fpc_encode_f16(src, dst, sel, n, log2_table,
                           (uint16_t *)fcm_raw, (uint16_t *)dfcm_raw);
            break;
        case TDC_DT_F32:
            fpc_encode_f32(src, dst, sel, n, log2_table,
                           (uint32_t *)fcm_raw, (uint32_t *)dfcm_raw);
            break;
        case TDC_DT_F64:
            fpc_encode_f64(src, dst, sel, n, log2_table,
                           (uint64_t *)fcm_raw, (uint64_t *)dfcm_raw);
            break;
        default:
            fpc_free_tables_enc(residual_out, fcm_raw);
            return TDC_E_DTYPE;
    }

    fpc_free_tables_enc(residual_out, fcm_raw);

    residual_out->size = residual_bytes;
    side_out->size     = side_bytes;
    return TDC_OK;
}

/* ----- Decode entry point -------------------------------------------------- */

static tdc_status fpc_decode(tdc_block      *out,
                             const void     *params,
                             tdc_dtype       residual_dtype,
                             const uint8_t  *residuals, size_t residual_size,
                             const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (out->shape.rank != 1)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!fpc_dtype_accepted(out->dtype))     return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t residual_bytes = (size_t)n * elem_size;
    if (residual_size != residual_bytes) return TDC_E_CORRUPT;

    if (n == 0) {
        if (side_size != 0) return TDC_E_CORRUPT;
        return TDC_OK;
    }
    if (!out->data || !residuals) return TDC_E_INVAL;

    /* Validate side metadata. */
    size_t sel_bytes = ((size_t)n + 7u) / 8u;
    size_t expected_side = 1u + sel_bytes;
    if (side_size != expected_side || !side_meta) return TDC_E_CORRUPT;

    unsigned log2_table = side_meta[0];
    if (log2_table > 20) return TDC_E_CORRUPT;
    const uint8_t *selectors = side_meta + 1;

    /* Allocate hash tables. */
    void *fcm_raw = NULL, *dfcm_raw = NULL;
    tdc_status st = fpc_alloc_tables_dec(elem_size, log2_table,
                                         &fcm_raw, &dfcm_raw);
    if (st != TDC_OK) return st;

    uint8_t *dst = (uint8_t *)out->data;

    switch (out->dtype) {
        case TDC_DT_F16:
            fpc_decode_f16(residuals, selectors, dst, n, log2_table,
                           (uint16_t *)fcm_raw, (uint16_t *)dfcm_raw);
            break;
        case TDC_DT_F32:
            fpc_decode_f32(residuals, selectors, dst, n, log2_table,
                           (uint32_t *)fcm_raw, (uint32_t *)dfcm_raw);
            break;
        case TDC_DT_F64:
            fpc_decode_f64(residuals, selectors, dst, n, log2_table,
                           (uint64_t *)fcm_raw, (uint64_t *)dfcm_raw);
            break;
        default:
            free(fcm_raw);
            return TDC_E_DTYPE;
    }

    free(fcm_raw);
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_fpc1d_vt = {
    .id               = TDC_MODEL_FPC_1D,
    .name             = "fpc1d",
    .accepted_dtypes  = FPC_ACCEPTED_DTYPES,
    .accepted_layouts = FPC_ACCEPTED_LAYOUTS,
    .encode           = fpc_encode,
    .decode           = fpc_decode,
};
