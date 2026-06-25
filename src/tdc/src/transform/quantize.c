/*
 * src/transform/quantize.c
 *
 * TDC_XFORM_QUANTIZE — lossy float-to-narrow-int quantization.
 *
 *   stored = round((value - offset) * scale)        (encode)
 *   value  = (double)stored / scale + offset        (decode)
 *
 * Forward input dtype: F32 or F64.
 * Forward output dtype: tdc_quantize_params::target, which must be one of
 * I8, I16, I32. I64 is rejected because round-trip through double has only
 * 52 bits of mantissa, so quantizing into a 64-bit target is meaningless.
 *
 * Source: extracted from vectra/src/vtr_codec.c lines 1826-1873
 *         (quantize_float_to_int, vtr_dequantize). The arithmetic kernel
 *         is byte-identical; the outer wrapping is rewritten for tdc
 *         allocation, error conventions, and the dtype-derived sizing.
 *
 * Differences vs vectra:
 *   - No validity bitmap. tdc transforms run on a flat byte buffer; the
 *     validity bitmap is a tdc_block-level concept that the model layer
 *     handles before/after the transform chain.
 *   - Encoder is INFALLIBLE per the tdc convention: out-of-range values
 *     are silently clamped to the target's [min, max], NaN is encoded as
 *     the target's min (sentinel), +/-Inf clamps to max/min. There is no
 *     overflow_count return value — the encoded stream is always valid.
 *   - Rounding mode: round() (half away from zero), same as vectra.
 *   - Scratch goes through tdc_buffer::realloc_fn (encode side). Decode
 *     writes directly into the caller-supplied dst buffer.
 *
 * On-disk shape:
 *   No header, no length field. The output is exactly n_elems *
 *   sizeof(target) bytes. The chain driver knows n_elems from src_size /
 *   sizeof(in_dtype), and the inverse uses the same arithmetic with
 *   params->target to derive byte counts.
 *
 * Decode dtype convention:
 *   in_dtype passed to decode is the *original* float dtype (the dtype the
 *   user wants back), mirroring encode. The encoded byte width comes from
 *   params->target. This matches the chain driver convention that each
 *   transform's in_dtype is fixed across encode/decode.
 */

#include "tdc/transform.h"
#include "transform_internal.h"
#include "../core/buffer.h"

#include "../core/float_order.h"

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>

/* ----- Target bounds ------------------------------------------------------ */
/*
 * INT8_MIN..INT32_MAX all fit exactly in IEEE-754 double, so storing the
 * bounds as double avoids float-conversion warnings in the hot loop and
 * lets the clamp run as a pair of double comparisons.
 */
static int quantize_target_bounds(tdc_dtype target,
                                  double *tmin, double *tmax) {
    switch (target) {
        case TDC_DT_I8:  *tmin = (double)INT8_MIN;  *tmax = (double)INT8_MAX;  return 1;
        case TDC_DT_I16: *tmin = (double)INT16_MIN; *tmax = (double)INT16_MAX; return 1;
        case TDC_DT_I32: *tmin = (double)INT32_MIN; *tmax = (double)INT32_MAX; return 1;
        default: return 0;
    }
}

/* Output-buffer growth uses the shared tdc_buf_reserve helper from
 * src/core/buffer.h (lifted in session 4 — see PORTING.md). */

/* ----- Encode ------------------------------------------------------------- */

static tdc_status quantize_encode(const uint8_t *src, size_t src_size,
                                  tdc_dtype      in_dtype,
                                  const void    *params,
                                  tdc_buffer    *dst,
                                  tdc_dtype     *out_dtype) {
    const tdc_quantize_params *qp = (const tdc_quantize_params *)params;
    if (!qp) return TDC_E_INVAL;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    if (in_dtype != TDC_DT_F16 && in_dtype != TDC_DT_F32 &&
        in_dtype != TDC_DT_F64) return TDC_E_DTYPE;

    double tmin, tmax;
    if (!quantize_target_bounds(qp->target, &tmin, &tmax)) return TDC_E_INVAL;
    if (!isfinite(qp->scale) || qp->scale == 0.0) return TDC_E_INVAL;
    if (!isfinite(qp->offset)) return TDC_E_INVAL;

    size_t in_elem  = tdc_dtype_size(in_dtype);
    size_t out_elem = tdc_dtype_size(qp->target);
    if (in_elem == 0 || out_elem == 0) return TDC_E_DTYPE;
    if (src_size % in_elem != 0) return TDC_E_INVAL;

    size_t n = src_size / in_elem;
    size_t out_size = n * out_elem;

    tdc_status st = tdc_buf_reserve(dst, out_size);
    if (st != TDC_OK) return st;

    if (out_dtype) *out_dtype = qp->target;

    if (n == 0) {
        dst->size = 0;
        return TDC_OK;
    }

    const double scale  = qp->scale;
    const double offset = qp->offset;

    for (size_t i = 0; i < n; i++) {
        double v;
        if (in_dtype == TDC_DT_F64) {
            memcpy(&v, src + i * 8u, 8u);
        } else if (in_dtype == TDC_DT_F32) {
            float f;
            memcpy(&f, src + i * 4u, 4u);
            v = (double)f;
        } else {
            uint16_t h;
            memcpy(&h, src + i * 2u, 2u);
            v = (double)tdc_f16_to_f32(h);
        }

        double r;
        if (isnan(v)) {
            r = tmin;  /* NaN sentinel */
        } else {
            r = round((v - offset) * scale);
            if (r < tmin) r = tmin;
            else if (r > tmax) r = tmax;
        }
        int64_t iv = (int64_t)r;

        switch (qp->target) {
            case TDC_DT_I8: {
                int8_t v8 = (int8_t)iv;
                memcpy(dst->data + i * 1u, &v8, 1u);
                break;
            }
            case TDC_DT_I16: {
                int16_t v16 = (int16_t)iv;
                memcpy(dst->data + i * 2u, &v16, 2u);
                break;
            }
            case TDC_DT_I32: {
                int32_t v32 = (int32_t)iv;
                memcpy(dst->data + i * 4u, &v32, 4u);
                break;
            }
            default:
                return TDC_E_INVAL; /* unreachable: filtered above */
        }
    }

    dst->size = out_size;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status quantize_decode(const uint8_t *src, size_t src_size,
                                  tdc_dtype      in_dtype,
                                  const void    *params,
                                  uint8_t       *dst, size_t dst_size,
                                  tdc_dtype     *out_dtype) {
    const tdc_quantize_params *qp = (const tdc_quantize_params *)params;
    if (!qp) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    if (in_dtype != TDC_DT_F16 && in_dtype != TDC_DT_F32 &&
        in_dtype != TDC_DT_F64) return TDC_E_DTYPE;

    /* Validate target is one of I8/I16/I32 (same set as encoder accepts). */
    if (qp->target != TDC_DT_I8 &&
        qp->target != TDC_DT_I16 &&
        qp->target != TDC_DT_I32) {
        return TDC_E_INVAL;
    }
    if (!isfinite(qp->scale) || qp->scale == 0.0) return TDC_E_INVAL;
    if (!isfinite(qp->offset)) return TDC_E_INVAL;

    size_t out_elem    = tdc_dtype_size(in_dtype);
    size_t target_size = tdc_dtype_size(qp->target);
    if (out_elem == 0 || target_size == 0) return TDC_E_DTYPE;
    if (src_size % target_size != 0) return TDC_E_CORRUPT;

    size_t n = src_size / target_size;
    if (dst_size != n * out_elem) return TDC_E_CORRUPT;

    if (out_dtype) *out_dtype = in_dtype;
    if (n == 0) return TDC_OK;

    const double inv_scale = 1.0 / qp->scale;
    const double offset    = qp->offset;

    for (size_t i = 0; i < n; i++) {
        int64_t iv;
        switch (qp->target) {
            case TDC_DT_I8: {
                int8_t v8;
                memcpy(&v8, src + i * 1u, 1u);
                iv = (int64_t)v8;
                break;
            }
            case TDC_DT_I16: {
                int16_t v16;
                memcpy(&v16, src + i * 2u, 2u);
                iv = (int64_t)v16;
                break;
            }
            case TDC_DT_I32: {
                int32_t v32;
                memcpy(&v32, src + i * 4u, 4u);
                iv = (int64_t)v32;
                break;
            }
            default:
                return TDC_E_INVAL; /* unreachable */
        }

        double v = (double)iv * inv_scale + offset;
        if (in_dtype == TDC_DT_F64) {
            memcpy(dst + i * 8u, &v, 8u);
        } else if (in_dtype == TDC_DT_F32) {
            float f = (float)v;
            memcpy(dst + i * 4u, &f, 4u);
        } else {
            uint16_t h = tdc_f32_to_f16((float)v);
            memcpy(dst + i * 2u, &h, 2u);
        }
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

#define QUANTIZE_DTYPE_BIT(dt) (1u << (uint32_t)(dt))
#define QUANTIZE_ACCEPTED_DTYPES (              \
    QUANTIZE_DTYPE_BIT(TDC_DT_F16) |            \
    QUANTIZE_DTYPE_BIT(TDC_DT_F32) |            \
    QUANTIZE_DTYPE_BIT(TDC_DT_F64))

const tdc_xform_vt tdc_xform_quantize_vt = {
    .id              = TDC_XFORM_QUANTIZE,
    .name            = "quantize",
    .accepted_dtypes = QUANTIZE_ACCEPTED_DTYPES,
    .can_inplace     = 0,
    .is_lossy        = 1,
    .encode          = quantize_encode,
    .decode          = quantize_decode,
};
