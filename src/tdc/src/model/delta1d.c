/*
 * src/model/delta1d.c
 *
 * TDC_MODEL_DELTA_1D — first-order differencing along VECTOR_1D blocks.
 *
 *   residual[0] = data[0]
 *   residual[i] = data[i] - data[i-1]      (i >= 1, modular at width N)
 *
 *   data[0]     = residual[0]
 *   data[i]     = data[i-1] + residual[i]  (i >= 1, modular at width N)
 *
 * Accepts every fixed-width integer dtype (i8/i16/i32/i64/u8/u16/u32/u64).
 * The residual_dtype reported to the chain is the input dtype unchanged
 * — the residual stream has the same width and signedness as the input.
 * Slowly varying or monotonic columns produce small-magnitude residuals
 * which feed zigzag → byte-shuffle → LZ2 well.
 *
 * Side metadata: NONE. The seed value lives in residual[0]. side_out is
 * left at size = 0 on encode and a non-zero side_size on decode is
 * rejected as TDC_E_CORRUPT. (No side metadata is the cleanest possible
 * extraction of vectra's delta path: there is genuinely nothing to
 * carry — the encoder writes deltas directly into a contiguous output
 * stream and the seed is the first element of that stream.)
 *
 * Modular arithmetic note:
 *   The kernel is written entirely in unsigned arithmetic of the matching
 *   width. Subtraction and addition wrap modulo 2^N, which is well-defined
 *   in C for unsigned types and is the only formulation that round-trips
 *   correctly across the full range of any input — including the i64 case
 *   where (data[i] - data[i-1]) would otherwise overflow signed range.
 *   The signed → unsigned reinterpretation is bit-preserving on every
 *   two's-complement target (which tdc requires).
 *
 * Validity bitmap:
 *   Ignored. The residual stream covers all n_elems regardless of the
 *   per-element validity flag. NA-aware delta is a future model concern;
 *   for now, vectra's caller carries the validity bitmap separately and
 *   the model just reproduces whatever bytes were in the input.
 *
 * Source today: vectra/src/vtr_codec.c:1380-1408 (delta_encode/delta_decode).
 * The vectra path was i64-only, single-threaded, and longjmp'd on alloc
 * failure. tdc generalizes to all integer widths, uses realloc_fn, and
 * returns status codes.
 *
 * Properties:
 *   accepted_dtypes  = I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64
 *   accepted_layouts = VECTOR_1D
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define DELTA1D_DT_BIT(dt) (1u << (uint32_t)(dt))

#define DELTA1D_ACCEPTED_DTYPES (         \
    DELTA1D_DT_BIT(TDC_DT_I8)  |          \
    DELTA1D_DT_BIT(TDC_DT_I16) |          \
    DELTA1D_DT_BIT(TDC_DT_I32) |          \
    DELTA1D_DT_BIT(TDC_DT_I64) |          \
    DELTA1D_DT_BIT(TDC_DT_U8)  |          \
    DELTA1D_DT_BIT(TDC_DT_U16) |          \
    DELTA1D_DT_BIT(TDC_DT_U32) |          \
    DELTA1D_DT_BIT(TDC_DT_U64))

#define DELTA1D_ACCEPTED_LAYOUTS (1u << (uint32_t)TDC_LAYOUT_VECTOR_1D)

static int delta1d_dtype_accepted(tdc_dtype dt) {
    return (DELTA1D_ACCEPTED_DTYPES & DELTA1D_DT_BIT(dt)) != 0u;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status delta1d_encode(const tdc_block *in,
                                 const void      *params,
                                 tdc_buffer      *residual_out,
                                 tdc_dtype       *residual_dtype,
                                 tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;
    if (!delta1d_dtype_accepted(in->dtype)) return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    tdc_status st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;
    if (side_out)        side_out->size  = 0; /* no side metadata */

    if (n == 0) {
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    const uint8_t *src = (const uint8_t *)in->data;
    uint8_t       *dst = residual_out->data;

    /* residual[0] = data[0] (seed) */
    memcpy(dst, src, elem_size);

    switch (elem_size) {
        case 1: {
            uint8_t prev;
            memcpy(&prev, src, 1u);
            for (int64_t i = 1; i < n; ++i) {
                uint8_t cur;
                memcpy(&cur, src + (size_t)i, 1u);
                uint8_t d = (uint8_t)(cur - prev);
                memcpy(dst + (size_t)i, &d, 1u);
                prev = cur;
            }
            break;
        }
        case 2: {
            uint16_t prev;
            memcpy(&prev, src, 2u);
            for (int64_t i = 1; i < n; ++i) {
                uint16_t cur;
                memcpy(&cur, src + (size_t)i * 2u, 2u);
                uint16_t d = (uint16_t)(cur - prev);
                memcpy(dst + (size_t)i * 2u, &d, 2u);
                prev = cur;
            }
            break;
        }
        case 4: {
            uint32_t prev;
            memcpy(&prev, src, 4u);
            for (int64_t i = 1; i < n; ++i) {
                uint32_t cur;
                memcpy(&cur, src + (size_t)i * 4u, 4u);
                uint32_t d = cur - prev;
                memcpy(dst + (size_t)i * 4u, &d, 4u);
                prev = cur;
            }
            break;
        }
        case 8: {
            uint64_t prev;
            memcpy(&prev, src, 8u);
            for (int64_t i = 1; i < n; ++i) {
                uint64_t cur;
                memcpy(&cur, src + (size_t)i * 8u, 8u);
                uint64_t d = cur - prev;
                memcpy(dst + (size_t)i * 8u, &d, 8u);
                prev = cur;
            }
            break;
        }
        default:
            return TDC_E_DTYPE; /* unreachable: filtered by accepted_dtypes */
    }

    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status delta1d_decode(tdc_block      *out,
                                 const void     *params,
                                 tdc_dtype       residual_dtype,
                                 const uint8_t  *residuals, size_t residual_size,
                                 const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    (void)side_meta;
    if (side_size != 0) return TDC_E_CORRUPT; /* delta1d carries no side meta */
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (out->shape.rank != 1)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!delta1d_dtype_accepted(out->dtype)) return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    uint8_t *dst = (uint8_t *)out->data;

    /* data[0] = residual[0] (seed) */
    memcpy(dst, residuals, elem_size);

    switch (elem_size) {
        case 1: {
            uint8_t acc;
            memcpy(&acc, residuals, 1u);
            for (int64_t i = 1; i < n; ++i) {
                uint8_t d;
                memcpy(&d, residuals + (size_t)i, 1u);
                acc = (uint8_t)(acc + d);
                memcpy(dst + (size_t)i, &acc, 1u);
            }
            break;
        }
        case 2: {
            uint16_t acc;
            memcpy(&acc, residuals, 2u);
            for (int64_t i = 1; i < n; ++i) {
                uint16_t d;
                memcpy(&d, residuals + (size_t)i * 2u, 2u);
                acc = (uint16_t)(acc + d);
                memcpy(dst + (size_t)i * 2u, &acc, 2u);
            }
            break;
        }
        case 4: {
            uint32_t acc;
            memcpy(&acc, residuals, 4u);
            for (int64_t i = 1; i < n; ++i) {
                uint32_t d;
                memcpy(&d, residuals + (size_t)i * 4u, 4u);
                acc = acc + d;
                memcpy(dst + (size_t)i * 4u, &acc, 4u);
            }
            break;
        }
        case 8: {
            uint64_t acc;
            memcpy(&acc, residuals, 8u);
            for (int64_t i = 1; i < n; ++i) {
                uint64_t d;
                memcpy(&d, residuals + (size_t)i * 8u, 8u);
                acc = acc + d;
                memcpy(dst + (size_t)i * 8u, &acc, 8u);
            }
            break;
        }
        default:
            return TDC_E_DTYPE; /* unreachable */
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_delta1d_vt = {
    .id               = TDC_MODEL_DELTA_1D,
    .name             = "delta1d",
    .accepted_dtypes  = DELTA1D_ACCEPTED_DTYPES,
    .accepted_layouts = DELTA1D_ACCEPTED_LAYOUTS,
    .encode           = delta1d_encode,
    .decode           = delta1d_decode,
};
