/*
 * src/model/raw.c
 *
 * TDC_MODEL_RAW — identity model.
 *
 *   residual = data        (memcpy)
 *   data     = residual    (memcpy on decode)
 *
 * RAW exists for two reasons:
 *   1. It is the baseline that every other model is compared against.
 *   2. The api/encode.c driver always needs *some* model to dispatch to.
 *      A user who wants to skip prediction (already-random data, raw
 *      sensor counts, etc.) selects RAW and the pipeline degenerates to
 *      transform-chain → entropy.
 *
 * Acceptance:
 *   - any rank-consistent layout (VECTOR_1D, RASTER_2D, STACK_2D, VOLUME_3D)
 *   - any fixed-width numeric dtype (i8/i16/i32/i64/u8/u16/u32/u64/f32/f64)
 *
 * TDC_DT_STRING is rejected. Strings carry an offsets[] sidecar that the
 * model would have to serialize through side_meta, and v0 is numeric-only
 * (see types.h). When DICT_1D lands it will own the string path; RAW stays
 * fixed-width forever.
 *
 * Side metadata: NONE. Same convention as delta1d — `side_out->size = 0`
 * on encode and a non-zero `side_size` on decode is rejected as
 * TDC_E_CORRUPT.
 *
 * residual_dtype: equal to in->dtype. RAW does not change width or
 * signedness; the downstream transform chain sees exactly the bytes the
 * caller handed in.
 *
 * Validity bitmap: ignored, same convention as every other model in v0.
 * The validity bitmap is a tdc_block-level concept that the encode driver
 * carries around the model stage; the model itself only round-trips bytes.
 *
 * No vectra source: vectra had no explicit "raw" model — its codec spec
 * branched on the compression-tag enum and a "no-op" tag was just a write
 * of the literal column buffer. tdc treats RAW as a first-class model so
 * the api driver can use one dispatch path for everything.
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define RAW_ACCEPTED_DTYPES (         \
    TDC_DT_BIT(TDC_DT_I8)  |         \
    TDC_DT_BIT(TDC_DT_I16) |         \
    TDC_DT_BIT(TDC_DT_I32) |         \
    TDC_DT_BIT(TDC_DT_I64) |         \
    TDC_DT_BIT(TDC_DT_U8)  |         \
    TDC_DT_BIT(TDC_DT_U16) |         \
    TDC_DT_BIT(TDC_DT_U32) |         \
    TDC_DT_BIT(TDC_DT_U64) |         \
    TDC_DT_BIT(TDC_DT_F32) |         \
    TDC_DT_BIT(TDC_DT_F64))

#define RAW_ACCEPTED_LAYOUTS (                    \
    TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D) |         \
    TDC_LAYOUT_BIT(TDC_LAYOUT_RASTER_2D) |         \
    TDC_LAYOUT_BIT(TDC_LAYOUT_STACK_2D)  |         \
    TDC_LAYOUT_BIT(TDC_LAYOUT_VOLUME_3D))

static int raw_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(RAW_ACCEPTED_DTYPES, dt);
}

static int raw_layout_accepted(tdc_layout lo) {
    return tdc_model_layout_accepted(RAW_ACCEPTED_LAYOUTS, lo);
}

/* Expected rank for a given layout. Used to validate that the caller
 * filled in shape.rank consistently with the layout enum. */
static int raw_expected_rank(tdc_layout lo) {
    switch (lo) {
        case TDC_LAYOUT_VECTOR_1D: return 1;
        case TDC_LAYOUT_RASTER_2D: return 2;
        case TDC_LAYOUT_STACK_2D:  return 3;
        case TDC_LAYOUT_VOLUME_3D: return 3;
        default:                   return -1;
    }
}

/* Compute n_elems = product of shape.dim[0..rank-1], guarding against
 * negative dims and against overflow of int64_t. Returns -1 on overflow
 * or invalid input. */
static int64_t raw_n_elems(const tdc_shape *s) {
    int64_t n = 1;
    for (uint8_t i = 0; i < s->rank; ++i) {
        if (s->dim[i] < 0) return -1;
        if (s->dim[i] != 0 && n > INT64_MAX / s->dim[i]) return -1;
        n *= s->dim[i];
    }
    return n;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status raw_encode(const tdc_block *in,
                             const void      *params,
                             tdc_buffer      *residual_out,
                             tdc_dtype       *residual_dtype,
                             tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!raw_layout_accepted(in->layout))                  return TDC_E_LAYOUT;
    if ((int)in->shape.rank != raw_expected_rank(in->layout)) return TDC_E_SHAPE;
    if (!raw_dtype_accepted(in->dtype))                    return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = raw_n_elems(&in->shape);
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

    memcpy(residual_out->data, in->data, bytes);
    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status raw_decode(tdc_block      *out,
                             const void     *params,
                             tdc_dtype       residual_dtype,
                             const uint8_t  *residuals, size_t residual_size,
                             const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    (void)side_meta;
    if (side_size != 0) return TDC_E_CORRUPT; /* RAW carries no side meta */
    if (!out) return TDC_E_INVAL;
    if (!raw_layout_accepted(out->layout))                 return TDC_E_LAYOUT;
    if ((int)out->shape.rank != raw_expected_rank(out->layout)) return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)                      return TDC_E_DTYPE;
    if (!raw_dtype_accepted(out->dtype))                   return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = raw_n_elems(&out->shape);
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    memcpy(out->data, residuals, bytes);
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_raw_vt = {
    .id               = TDC_MODEL_RAW,
    .name             = "raw",
    .accepted_dtypes  = RAW_ACCEPTED_DTYPES,
    .accepted_layouts = RAW_ACCEPTED_LAYOUTS,
    .encode           = raw_encode,
    .decode           = raw_decode,
};
