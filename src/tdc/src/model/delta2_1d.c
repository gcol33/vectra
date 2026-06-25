/*
 * src/model/delta2_1d.c
 *
 * TDC_MODEL_DELTA2_1D — second-order XOR differencing along VECTOR_1D blocks.
 *
 * Float dtypes only (f16/f32/f64). For a smooth curve y = a + b*x + c*x^2,
 * first-order XOR-delta produces near-constant d1 values; second-order
 * XOR-delta of those produces many leading zero bytes because d2 ≈ 0.
 *
 * Encode:
 *   d1[0] = bits(data[0])                     (seed 0)
 *   d1[1] = bits(data[1]) XOR bits(data[0])   (seed 1)
 *   d1[i] = bits(data[i]) XOR bits(data[i-1]) (i >= 2)
 *   d2[i] = d1[i] XOR d1[i-1]                (i >= 2)
 *
 * Output stream:
 *   [0]  = d1[0]   (raw seed 0)
 *   [1]  = d1[1]   (raw seed 1)
 *   [i]  = d2[i]   (second-order XOR delta, i >= 2)
 *
 * Decode (self-inverse):
 *   d1[0] = residual[0]
 *   d1[1] = residual[1]
 *   d1[i] = d1[i-1] XOR residual[i]           (recover first-order deltas)
 *   bits[0] = d1[0]
 *   bits[i] = bits[i-1] XOR d1[i]             (recover original bit patterns)
 *
 * Side metadata: NONE. Seeds are residual[0] and residual[1].
 *
 * Integer dtypes are NOT supported — first-order modular subtraction is
 * already optimal for integers (second-order subtraction would need two
 * seeds of side metadata and doesn't produce meaningfully better residuals
 * on integer time series). Use DELTA_1D for integers.
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ------------------------------------------------- */

#define DELTA2_ACCEPTED_DTYPES (           \
    TDC_DT_BIT(TDC_DT_F16) |              \
    TDC_DT_BIT(TDC_DT_F32) |              \
    TDC_DT_BIT(TDC_DT_F64))

#define DELTA2_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D)

static int delta2_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(DELTA2_ACCEPTED_DTYPES, dt);
}

/* ----- Encode/decode kernels (macro-generated per width) ------------------- */

#define DEFINE_DELTA2_ENCODE(SUFFIX, UT, W)                                   \
static void delta2_encode_##SUFFIX(const uint8_t *src, uint8_t *dst, int64_t n) { \
    /* n >= 2 guaranteed by caller */                                         \
    UT prev_bits, cur_bits, prev_d1;                                          \
    memcpy(&prev_bits, src, (W));                                             \
    memcpy(dst, &prev_bits, (W));            /* residual[0] = seed 0 */       \
                                                                              \
    memcpy(&cur_bits, src + (W), (W));                                        \
    prev_d1 = cur_bits ^ prev_bits;                                           \
    memcpy(dst + (W), &prev_d1, (W));        /* residual[1] = seed 1 */       \
    prev_bits = cur_bits;                                                     \
                                                                              \
    for (int64_t i = 2; i < n; ++i) {                                        \
        memcpy(&cur_bits, src + (size_t)i * (W), (W));                       \
        UT d1 = cur_bits ^ prev_bits;                                         \
        UT d2 = d1 ^ prev_d1;                                                \
        memcpy(dst + (size_t)i * (W), &d2, (W));                             \
        prev_bits = cur_bits;                                                 \
        prev_d1 = d1;                                                         \
    }                                                                         \
}

#define DEFINE_DELTA2_DECODE(SUFFIX, UT, W)                                   \
static void delta2_decode_##SUFFIX(const uint8_t *res, uint8_t *dst, int64_t n) { \
    /* n >= 2 guaranteed by caller */                                         \
    UT prev_d1, prev_bits;                                                    \
    memcpy(&prev_bits, res, (W));                                             \
    memcpy(dst, &prev_bits, (W));            /* bits[0] */                    \
                                                                              \
    memcpy(&prev_d1, res + (W), (W));                                         \
    prev_bits = prev_bits ^ prev_d1;                                          \
    memcpy(dst + (W), &prev_bits, (W));      /* bits[1] */                    \
                                                                              \
    for (int64_t i = 2; i < n; ++i) {                                        \
        UT d2;                                                                \
        memcpy(&d2, res + (size_t)i * (W), (W));                             \
        UT d1 = prev_d1 ^ d2;               /* recover d1[i] */              \
        prev_bits = prev_bits ^ d1;          /* recover bits[i] */            \
        memcpy(dst + (size_t)i * (W), &prev_bits, (W));                      \
        prev_d1 = d1;                                                         \
    }                                                                         \
}

DEFINE_DELTA2_ENCODE(f16, uint16_t, 2u)
DEFINE_DELTA2_ENCODE(f32, uint32_t, 4u)
DEFINE_DELTA2_ENCODE(f64, uint64_t, 8u)

DEFINE_DELTA2_DECODE(f16, uint16_t, 2u)
DEFINE_DELTA2_DECODE(f32, uint32_t, 4u)
DEFINE_DELTA2_DECODE(f64, uint64_t, 8u)

#undef DEFINE_DELTA2_ENCODE
#undef DEFINE_DELTA2_DECODE

/* ----- Encode ------------------------------------------------------------- */

static tdc_status delta2_encode(const tdc_block *in,
                                const void      *params,
                                tdc_buffer      *residual_out,
                                tdc_dtype       *residual_dtype,
                                tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;
    if (!delta2_dtype_accepted(in->dtype))  return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    tdc_status st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;
    if (side_out)        side_out->size  = 0;

    if (n == 0) { residual_out->size = 0; return TDC_OK; }
    if (!in->data) return TDC_E_INVAL;

    const uint8_t *src = (const uint8_t *)in->data;
    uint8_t       *dst = residual_out->data;

    if (n == 1) {
        /* Single element: just copy the raw bits. */
        memcpy(dst, src, elem_size);
        residual_out->size = bytes;
        return TDC_OK;
    }

    /* n >= 2: run the second-order XOR-delta kernel. */
    switch (in->dtype) {
        case TDC_DT_F16: delta2_encode_f16(src, dst, n); break;
        case TDC_DT_F32: delta2_encode_f32(src, dst, n); break;
        case TDC_DT_F64: delta2_encode_f64(src, dst, n); break;
        default: return TDC_E_DTYPE;
    }

    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status delta2_decode(tdc_block      *out,
                                const void     *params,
                                tdc_dtype       residual_dtype,
                                const uint8_t  *residuals, size_t residual_size,
                                const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    (void)side_meta;
    if (side_size != 0) return TDC_E_CORRUPT;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (out->shape.rank != 1)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!delta2_dtype_accepted(out->dtype))  return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    uint8_t *dst = (uint8_t *)out->data;

    if (n == 1) {
        memcpy(dst, residuals, elem_size);
        return TDC_OK;
    }

    switch (out->dtype) {
        case TDC_DT_F16: delta2_decode_f16(residuals, dst, n); break;
        case TDC_DT_F32: delta2_decode_f32(residuals, dst, n); break;
        case TDC_DT_F64: delta2_decode_f64(residuals, dst, n); break;
        default: return TDC_E_DTYPE;
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_delta2_1d_vt = {
    .id               = TDC_MODEL_DELTA2_1D,
    .name             = "delta2_1d",
    .accepted_dtypes  = DELTA2_ACCEPTED_DTYPES,
    .accepted_layouts = DELTA2_ACCEPTED_LAYOUTS,
    .encode           = delta2_encode,
    .decode           = delta2_decode,
};
