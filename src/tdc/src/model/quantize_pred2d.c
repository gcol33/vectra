/*
 * src/model/quantize_pred2d.c
 *
 * TDC_MODEL_QUANTIZE_PRED_2D — fused quantize-then-PRED_2D for float
 * RASTER_2D blocks.
 *
 * Why fused:
 *   The naive composition "model = PRED_2D, xform[0] = QUANTIZE" runs
 *   PRED_2D on the raw f64 raster first, producing an f64 residual, and
 *   then QUANTIZE re-quantizes that residual into garbage values that no
 *   longer round-trip through the dequantize step. Fusing the two
 *   guarantees QUANTIZE happens BEFORE the predictor sees the data, so
 *   PRED_2D operates on a clean integer raster and emits a same-dtype
 *   integer residual that downstream ZIGZAG/BYTE_SHUFFLE/LZ stages
 *   handle the same way they do for any other integer pipeline.
 *
 * Pipeline (encode):
 *   1. F32/F64 RASTER_2D in
 *   2. element-wise quantize into a scratch integer raster of
 *      params->target dtype (I8/I16/I32) — same arithmetic as
 *      tdc_xform_quantize_vt, with NaN -> tmin and clamp on overflow
 *   3. PRED_2D encode on the integer raster -> integer residual
 *   4. Side meta: target (1) + kind (1) + scale (8) + offset (8) = 18 bytes
 *      The pred2d kind written to side meta is always the resolved one
 *      (LEFT/UP/AVERAGE/PAETH, never AUTO) so decode dispatch matches the
 *      stand-alone PRED_2D convention.
 *   5. Residual dtype = params->target
 *
 * Pipeline (decode):
 *   1. Parse side meta -> (target, kind, scale, offset)
 *   2. PRED_2D decode on the residual into a scratch integer raster
 *   3. Dequantize each element back into the user-facing F32/F64 raster
 *
 * Reuses pred2d_encode_sweep / pred2d_decode_sweep (declared in
 * pred2d_internal.h) so the predictor implementation lives in one place.
 *
 * Allocation:
 *   The encode path needs a scratch integer raster (n_elems *
 *   sizeof(target)) for the quantized values before pred2d sees them.
 *   We alloc/free this through residual_out->realloc_fn so the model
 *   stays inside the project's "no bare malloc/free" rule.
 *   Decode does the symmetric: a scratch integer raster reconstructed by
 *   pred2d_decode_sweep, then dequantized into out->data. The decode
 *   vtable doesn't get a tdc_buffer for scratch — it goes through the
 *   internal driver path. We reach for libc realloc directly, gated by a
 *   single #include and a comment, in keeping with how the other
 *   variable-scratch decode paths handle it. (A future refactor will
 *   plumb a scratch tdc_buffer through model.decode for all backends.)
 */

#include "tdc/model.h"
#include "tdc/codec.h"
#include "tdc/types.h"

#include "model_internal.h"
#include "model_load_store.h"
#include "pred2d_internal.h"
#include "../core/buffer.h"

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#define QP2_SIDE_META_SIZE 18u

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define QP2_ACCEPTED_DTYPES (              \
    TDC_DT_BIT(TDC_DT_F32) |               \
    TDC_DT_BIT(TDC_DT_F64))

#define QP2_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_RASTER_2D)

static int qp2_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(QP2_ACCEPTED_DTYPES, dt);
}

/* ----- Quantize bounds (mirrors transform/quantize.c) -------------------- */

static int qp2_target_bounds(tdc_dtype target, double *tmin, double *tmax) {
    switch (target) {
        case TDC_DT_I8:  *tmin = (double)INT8_MIN;  *tmax = (double)INT8_MAX;  return 1;
        case TDC_DT_I16: *tmin = (double)INT16_MIN; *tmax = (double)INT16_MAX; return 1;
        case TDC_DT_I32: *tmin = (double)INT32_MIN; *tmax = (double)INT32_MAX; return 1;
        default: return 0;
    }
}

/* Read element i of `src` (F32 or F64) as a double. */
static inline double qp2_load_double(tdc_dtype in_dt, const uint8_t *src, size_t i) {
    if (in_dt == TDC_DT_F64) {
        double v;
        memcpy(&v, src + i * 8u, 8u);
        return v;
    }
    float f;
    memcpy(&f, src + i * 4u, 4u);
    return (double)f;
}

/* Write integer iv into element i of `dst` of dtype `target`. */
static inline void qp2_store_int(tdc_dtype target, uint8_t *dst, size_t i, int64_t iv) {
    switch (target) {
        case TDC_DT_I8:  { int8_t  v = (int8_t)iv;  memcpy(dst + i,       &v, 1u); break; }
        case TDC_DT_I16: { int16_t v = (int16_t)iv; memcpy(dst + i * 2u,  &v, 2u); break; }
        case TDC_DT_I32: { int32_t v = (int32_t)iv; memcpy(dst + i * 4u,  &v, 4u); break; }
        default: break;
    }
}

static inline int64_t qp2_load_int(tdc_dtype target, const uint8_t *src, size_t i) {
    switch (target) {
        case TDC_DT_I8:  { int8_t  v; memcpy(&v, src + i,       1u); return (int64_t)v; }
        case TDC_DT_I16: { int16_t v; memcpy(&v, src + i * 2u,  2u); return (int64_t)v; }
        case TDC_DT_I32: { int32_t v; memcpy(&v, src + i * 4u,  4u); return (int64_t)v; }
        default: return 0;
    }
}

static inline void qp2_store_double_as(tdc_dtype out_dt, uint8_t *dst, size_t i, double v) {
    if (out_dt == TDC_DT_F64) {
        memcpy(dst + i * 8u, &v, 8u);
    } else {
        float f = (float)v;
        memcpy(dst + i * 4u, &f, 4u);
    }
}

/* ----- Side meta packing ------------------------------------------------- */
/* Layout (LE):
 *   [0]    : target dtype
 *   [1]    : pred2d kind (resolved)
 *   [2..9] : scale (double)
 *   [10..17]: offset (double)
 */

static void qp2_pack_side_meta(uint8_t *out,
                               tdc_dtype target,
                               tdc_pred2d_kind kind,
                               double scale, double offset) {
    out[0] = (uint8_t)target;
    out[1] = (uint8_t)kind;
    memcpy(out + 2,  &scale,  8u);
    memcpy(out + 10, &offset, 8u);
}

static int qp2_unpack_side_meta(const uint8_t *in, size_t in_size,
                                tdc_dtype *target,
                                tdc_pred2d_kind *kind,
                                double *scale, double *offset) {
    if (in_size != QP2_SIDE_META_SIZE || in == NULL) return 0;
    *target = (tdc_dtype)in[0];
    *kind   = (tdc_pred2d_kind)in[1];
    memcpy(scale,  in + 2,  8u);
    memcpy(offset, in + 10, 8u);
    return 1;
}

/* Driver hook: peek at the side meta to determine the residual dtype.
 * Called by driver_model_residual_dtype before model.decode runs so the
 * forward dtype walk can size the entropy/transform stages correctly. */
tdc_dtype qp2_residual_dtype_from_side_meta(const uint8_t *side_meta,
                                            size_t side_meta_size) {
    if (side_meta_size < 1u || side_meta == NULL) return (tdc_dtype)0;
    tdc_dtype t = (tdc_dtype)side_meta[0];
    if (t != TDC_DT_I8 && t != TDC_DT_I16 && t != TDC_DT_I32) return (tdc_dtype)0;
    return t;
}

/* ----- Encode ------------------------------------------------------------ */

static tdc_status qp2_encode(const tdc_block *in,
                             const void      *params,
                             tdc_buffer      *residual_out,
                             tdc_dtype       *residual_dtype,
                             tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (!params)                                            return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (in->shape.rank != 2)                return TDC_E_SHAPE;
    if (!qp2_dtype_accepted(in->dtype))     return TDC_E_DTYPE;

    const tdc_quantize_pred2d_params *qp =
        (const tdc_quantize_pred2d_params *)params;

    double tmin, tmax;
    if (!qp2_target_bounds(qp->target, &tmin, &tmax)) return TDC_E_INVAL;
    if (!isfinite(qp->scale) || qp->scale == 0.0) return TDC_E_INVAL;
    if (!isfinite(qp->offset))                    return TDC_E_INVAL;

    int64_t ny = in->shape.dim[0];
    int64_t nx = in->shape.dim[1];
    if (nx < 0 || ny < 0)                                  return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)         return TDC_E_SHAPE;

    size_t target_size = tdc_dtype_size(qp->target);
    if (target_size == 0) return TDC_E_DTYPE;

    /* Side meta first — even an empty raster carries (target, kind,
     * scale, offset) so decode is parameterless besides the record. */
    tdc_status st = tdc_buf_reserve(side_out, QP2_SIDE_META_SIZE);
    if (st != TDC_OK) return st;
    side_out->size = QP2_SIDE_META_SIZE;

    int64_t n = nx * ny;
    size_t  bytes = (size_t)n * target_size;

    st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;
    if (residual_dtype) *residual_dtype = qp->target;

    if (n == 0) {
        /* Side meta still uses the caller's nominal kind (clamped to
         * AVERAGE for AUTO since we have nothing to score). */
        tdc_pred2d_kind kind = (qp->kind == TDC_PRED2D_AUTO)
            ? TDC_PRED2D_AVERAGE : qp->kind;
        qp2_pack_side_meta(side_out->data, qp->target, kind,
                           qp->scale, qp->offset);
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    /* Step 1: quantize the float raster into a scratch integer raster.
     * Allocate via residual_out->realloc_fn to honor the no-bare-malloc
     * rule. The scratch is freed before return (success or error). */
    uint8_t *qbuf = (uint8_t *)residual_out->realloc_fn(
        residual_out->user, NULL, bytes);
    if (!qbuf) return TDC_E_NOMEM;

    const double scale  = qp->scale;
    const double offset = qp->offset;
    for (size_t i = 0; i < (size_t)n; ++i) {
        double v = qp2_load_double(in->dtype, (const uint8_t *)in->data, i);
        double r;
        if (isnan(v)) {
            r = tmin;
        } else {
            r = round((v - offset) * scale);
            if (r < tmin)      r = tmin;
            else if (r > tmax) r = tmax;
        }
        qp2_store_int(qp->target, qbuf, i, (int64_t)r);
    }

    /* Step 2: resolve pred2d kind on the QUANTIZED raster (so AUTO sees
     * the data the predictor will actually run against). */
    tdc_pred2d_kind kind = qp->kind;
    if (kind == TDC_PRED2D_AUTO) {
        kind = pred2d_auto_select(qp->target, qbuf, nx, ny);
    } else if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
               kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH) {
        residual_out->realloc_fn(residual_out->user, qbuf, 0);
        return TDC_E_INVAL;
    }

    /* Step 3: PRED_2D encode on the integer raster -> integer residual. */
    pred2d_encode_sweep(qp->target, kind, qbuf, residual_out->data, nx, ny);
    residual_out->size = bytes;

    qp2_pack_side_meta(side_out->data, qp->target, kind, scale, offset);

    residual_out->realloc_fn(residual_out->user, qbuf, 0);
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------ */

static tdc_status qp2_decode(tdc_block      *out,
                             const void     *params,
                             tdc_dtype       residual_dtype,
                             const uint8_t  *residuals, size_t residual_size,
                             const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out)                                return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (out->shape.rank != 2)                return TDC_E_SHAPE;
    if (!qp2_dtype_accepted(out->dtype))     return TDC_E_DTYPE;

    tdc_dtype       target;
    tdc_pred2d_kind kind;
    double          scale, offset;
    if (!qp2_unpack_side_meta(side_meta, side_size, &target, &kind, &scale, &offset))
        return TDC_E_CORRUPT;
    if (target != TDC_DT_I8 && target != TDC_DT_I16 && target != TDC_DT_I32)
        return TDC_E_CORRUPT;
    if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
        kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH)
        return TDC_E_CORRUPT;
    if (residual_dtype != target) return TDC_E_DTYPE;
    if (!isfinite(scale) || scale == 0.0) return TDC_E_CORRUPT;
    if (!isfinite(offset))                return TDC_E_CORRUPT;

    int64_t ny = out->shape.dim[0];
    int64_t nx = out->shape.dim[1];
    if (nx < 0 || ny < 0) return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny) return TDC_E_SHAPE;

    size_t target_size = tdc_dtype_size(target);
    if (target_size == 0) return TDC_E_DTYPE;

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * target_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    /* Decode-side scratch: the existing model.decode vtable doesn't
     * carry a tdc_buffer for scratch, so this path uses libc realloc
     * directly. Same convention as the other variable-scratch model
     * decoders (see plane2d.c, dict.c). A future refactor that hangs a
     * scratch buffer off the model.decode signature replaces this with
     * a realloc_fn call. */
    uint8_t *qbuf = (uint8_t *)malloc(bytes);
    if (!qbuf) return TDC_E_NOMEM;

    pred2d_decode_sweep(target, kind, residuals, qbuf, nx, ny);

    const double inv_scale = 1.0 / scale;
    for (size_t i = 0; i < (size_t)n; ++i) {
        int64_t iv = qp2_load_int(target, qbuf, i);
        double  v  = (double)iv * inv_scale + offset;
        qp2_store_double_as(out->dtype, (uint8_t *)out->data, i, v);
    }

    free(qbuf);
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------ */

const tdc_model_vt tdc_model_quantize_pred2d_vt = {
    .id               = TDC_MODEL_QUANTIZE_PRED_2D,
    .name             = "quantize_pred2d",
    .accepted_dtypes  = QP2_ACCEPTED_DTYPES,
    .accepted_layouts = QP2_ACCEPTED_LAYOUTS,
    .encode           = qp2_encode,
    .decode           = qp2_decode,
};
