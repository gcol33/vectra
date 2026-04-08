/*
 * src/model/pred2d.c
 *
 * TDC_MODEL_PRED_2D — 2D spatial predictor family for RASTER_2D blocks.
 *
 * Predictor kinds (tdc_pred2d_kind):
 *   LEFT     — pred = val[r][c-1]
 *   UP       — pred = val[r-1][c]
 *   AVERAGE  — pred = (left + up) / 2     (C truncation, vectra-compatible)
 *   PAETH    — PNG-style: of {left, up, upleft}, pick the one closest to
 *              the linear predictor p = left + up - upleft
 *   AUTO     — encoder picks the LEFT/UP/AVERAGE/PAETH variant that
 *              minimizes sum of |residual| on a sample of up to 10000
 *              elements
 *
 * PLANE is intentionally NOT in this file. It needs side metadata
 * (per-tile coefficients), a different params struct (tdc_plane2d_params),
 * and is large enough to live in its own src/model/plane2d.c when it
 * lands.
 *
 * Accepted dtypes: i8, i16, i32, u8, u16, u32. (No 64-bit: 64-bit raster
 * imagery is vanishingly rare and the predictor's int64 internal
 * arithmetic cannot guard against overflow at that width.) Floats are
 * rejected — quantize first.
 *
 * Accepted layouts: RASTER_2D only. shape.rank must be 2 with row-major
 * contiguous storage:
 *   ny = shape.dim[0]   (number of rows)
 *   nx = shape.dim[1]   (row length)
 *   idx = row * nx + col
 *
 * Residual dtype: same as input. The kernel does internal arithmetic in
 * int64 (which fits any of the supported widths) but writes residuals
 * back at the input width via modular wrap. Decode is the modular
 * inverse: it loads residuals and previously decoded neighbors,
 * recomputes the predictor, and stores val mod 2^N. This round-trips
 * because every operation involved is modular at width N once written
 * back through the typed store.
 *
 * Side metadata: 1 byte = the resolved predictor kind (LEFT, UP, AVERAGE,
 * or PAETH — never AUTO). Even when the caller passes a non-AUTO kind,
 * the resolved kind is recorded so the decoder dispatches identically
 * regardless of how the encoder selected it. This is the simplest
 * forward-compatible shape and matches the design rule that decoders
 * never re-derive encoder choices.
 *
 * Validity bitmap: ignored, same convention as every other v0 model. The
 * encode driver carries the validity bitmap around the model stage; the
 * model itself only round-trips bytes.
 *
 * Source: vectra/src/vtr_codec.c lines 1572-1813 (paeth_predict,
 * spatial_encode_int, spatial_decode_int, auto_select_predictor). The
 * predictor kernels and the auto-select scoring loop are preserved
 * conceptually; the outer wrapping is rewritten for tdc allocation,
 * error returns, dtype generality, and the side metadata convention.
 * Vectra's path was int64-only and longjmp'd on alloc failure.
 */

#include "tdc/model.h"
#include "tdc/codec.h"
#include "model_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define PRED2D_DT_BIT(dt) (1u << (uint32_t)(dt))

#define PRED2D_ACCEPTED_DTYPES (         \
    PRED2D_DT_BIT(TDC_DT_I8)  |          \
    PRED2D_DT_BIT(TDC_DT_I16) |          \
    PRED2D_DT_BIT(TDC_DT_I32) |          \
    PRED2D_DT_BIT(TDC_DT_U8)  |          \
    PRED2D_DT_BIT(TDC_DT_U16) |          \
    PRED2D_DT_BIT(TDC_DT_U32))

#define PRED2D_ACCEPTED_LAYOUTS (1u << (uint32_t)TDC_LAYOUT_RASTER_2D)

static int pred2d_dtype_accepted(tdc_dtype dt) {
    return (PRED2D_ACCEPTED_DTYPES & PRED2D_DT_BIT(dt)) != 0u;
}

/* ----- Type-generic load / modular store --------------------------------- */
/*
 * Load index `i` from a flat row-major buffer of dtype `dt` into int64.
 * Signed dtypes sign-extend; unsigned dtypes zero-extend. memcpy is used
 * to avoid alignment hazards on strict targets.
 */
static int64_t pred2d_load(tdc_dtype dt, const uint8_t *base, int64_t i) {
    switch (dt) {
        case TDC_DT_I8:  { int8_t  v; memcpy(&v, base + (size_t)i,           1u); return (int64_t)v; }
        case TDC_DT_I16: { int16_t v; memcpy(&v, base + (size_t)i * 2u,      2u); return (int64_t)v; }
        case TDC_DT_I32: { int32_t v; memcpy(&v, base + (size_t)i * 4u,      4u); return (int64_t)v; }
        case TDC_DT_U8:  { uint8_t  v; memcpy(&v, base + (size_t)i,          1u); return (int64_t)v; }
        case TDC_DT_U16: { uint16_t v; memcpy(&v, base + (size_t)i * 2u,     2u); return (int64_t)v; }
        case TDC_DT_U32: { uint32_t v; memcpy(&v, base + (size_t)i * 4u,     4u); return (int64_t)v; }
        default:         return 0;
    }
}

/*
 * Store the low N bits of int64 `v` at index `i` of a flat row-major
 * buffer of dtype `dt`. The route through the unsigned counterpart is
 * deliberate: int -> unsigned conversion is well-defined modular
 * truncation in C, while the signed -> signed narrowing of an
 * out-of-range value is only implementation-defined. tdc requires
 * two's-complement targets, so the bit pattern is preserved either
 * way, but going through the unsigned form avoids the IDB.
 */
static void pred2d_store(tdc_dtype dt, uint8_t *base, int64_t i, int64_t v) {
    switch (dt) {
        case TDC_DT_I8:
        case TDC_DT_U8:  { uint8_t  x = (uint8_t)(uint64_t)v;  memcpy(base + (size_t)i,           &x, 1u); break; }
        case TDC_DT_I16:
        case TDC_DT_U16: { uint16_t x = (uint16_t)(uint64_t)v; memcpy(base + (size_t)i * 2u,      &x, 2u); break; }
        case TDC_DT_I32:
        case TDC_DT_U32: { uint32_t x = (uint32_t)(uint64_t)v; memcpy(base + (size_t)i * 4u,      &x, 4u); break; }
        default: break;
    }
}

/* ----- Predictor kernels ------------------------------------------------- */

static inline int64_t paeth_predict(int64_t a, int64_t b, int64_t c) {
    int64_t p  = a + b - c;
    int64_t pa = p > a ? p - a : a - p;
    int64_t pb = p > b ? p - b : b - p;
    int64_t pc = p > c ? p - c : c - p;
    if (pa <= pb && pa <= pc) return a;
    if (pb <= pc)             return b;
    return c;
}

static inline int64_t pred2d_compute(tdc_pred2d_kind kind,
                                     int64_t left, int64_t up, int64_t upleft) {
    switch (kind) {
        case TDC_PRED2D_LEFT:    return left;
        case TDC_PRED2D_UP:      return up;
        case TDC_PRED2D_AVERAGE: return (left + up) / 2; /* C trunc; encode and decode use same form */
        case TDC_PRED2D_PAETH:   return paeth_predict(left, up, upleft);
        default:                 return 0;
    }
}

/* ----- Forward sweep ----------------------------------------------------- */
/*
 * Encode one full pass: read from `src` (typed), write residuals to
 * `dst_residuals` (same dtype, modular). Both buffers are rank-2
 * row-major: ny rows of nx elements.
 */
static void pred2d_encode_sweep(tdc_dtype dt, tdc_pred2d_kind kind,
                                const uint8_t *src,
                                uint8_t *dst_residuals,
                                int64_t nx, int64_t ny) {
    for (int64_t row = 0; row < ny; ++row) {
        for (int64_t col = 0; col < nx; ++col) {
            int64_t i      = row * nx + col;
            int64_t val    = pred2d_load(dt, src, i);
            int64_t left   = (col > 0)              ? pred2d_load(dt, src, i - 1)      : 0;
            int64_t up     = (row > 0)              ? pred2d_load(dt, src, i - nx)     : 0;
            int64_t upleft = (col > 0 && row > 0)   ? pred2d_load(dt, src, i - nx - 1) : 0;
            int64_t pred   = pred2d_compute(kind, left, up, upleft);
            pred2d_store(dt, dst_residuals, i, val - pred);
        }
    }
}

static void pred2d_decode_sweep(tdc_dtype dt, tdc_pred2d_kind kind,
                                const uint8_t *residuals,
                                uint8_t *dst,
                                int64_t nx, int64_t ny) {
    for (int64_t row = 0; row < ny; ++row) {
        for (int64_t col = 0; col < nx; ++col) {
            int64_t i      = row * nx + col;
            int64_t r      = pred2d_load(dt, residuals, i);
            int64_t left   = (col > 0)              ? pred2d_load(dt, dst, i - 1)      : 0;
            int64_t up     = (row > 0)              ? pred2d_load(dt, dst, i - nx)     : 0;
            int64_t upleft = (col > 0 && row > 0)   ? pred2d_load(dt, dst, i - nx - 1) : 0;
            int64_t pred   = pred2d_compute(kind, left, up, upleft);
            pred2d_store(dt, dst, i, r + pred);
        }
    }
}

/* ----- Auto-select ------------------------------------------------------- */
/*
 * Score each of LEFT/UP/AVERAGE/PAETH on a prefix of up to 10000 elements
 * and return the kind with the smallest sum of absolute residuals. Same
 * heuristic as vectra (vtr_codec.c:auto_select_predictor) — cheap, makes
 * no allocations, and "good enough" for the AUTO case in v0.
 */
#define PRED2D_AUTO_SAMPLE 10000

static tdc_pred2d_kind pred2d_auto_select(tdc_dtype dt, const uint8_t *src,
                                          int64_t nx, int64_t ny) {
    int64_t n = nx * ny;
    int64_t sample_n = n < PRED2D_AUTO_SAMPLE ? n : PRED2D_AUTO_SAMPLE;
    /* sample is a row-aligned prefix so the predictor sees the same
     * neighborhood structure it will see at full size. */
    int64_t sample_rows = sample_n / nx;
    if (sample_rows < 2) sample_rows = ny < 2 ? ny : 2; /* always score at least 2 rows when possible */
    if (sample_rows > ny) sample_rows = ny;

    tdc_pred2d_kind best_kind = TDC_PRED2D_AVERAGE;
    uint64_t        best_sum  = UINT64_MAX;

    static const tdc_pred2d_kind candidates[4] = {
        TDC_PRED2D_LEFT, TDC_PRED2D_UP, TDC_PRED2D_AVERAGE, TDC_PRED2D_PAETH
    };

    for (int k = 0; k < 4; ++k) {
        tdc_pred2d_kind kind = candidates[k];
        uint64_t sum = 0;
        for (int64_t row = 0; row < sample_rows; ++row) {
            for (int64_t col = 0; col < nx; ++col) {
                int64_t i      = row * nx + col;
                int64_t val    = pred2d_load(dt, src, i);
                int64_t left   = (col > 0)              ? pred2d_load(dt, src, i - 1)      : 0;
                int64_t up     = (row > 0)              ? pred2d_load(dt, src, i - nx)     : 0;
                int64_t upleft = (col > 0 && row > 0)   ? pred2d_load(dt, src, i - nx - 1) : 0;
                int64_t pred   = pred2d_compute(kind, left, up, upleft);
                int64_t res    = val - pred;
                sum += (uint64_t)(res < 0 ? -res : res);
            }
        }
        if (sum < best_sum) {
            best_sum  = sum;
            best_kind = kind;
        }
    }

    return best_kind;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status pred2d_encode(const tdc_block *in,
                                const void      *params,
                                tdc_buffer      *residual_out,
                                tdc_dtype       *residual_dtype,
                                tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (in->shape.rank != 2)                return TDC_E_SHAPE;
    if (!pred2d_dtype_accepted(in->dtype))  return TDC_E_DTYPE;

    int64_t ny = in->shape.dim[0];
    int64_t nx = in->shape.dim[1];
    if (nx < 0 || ny < 0)                                     return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)            return TDC_E_SHAPE;

    size_t  elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    /* Resolve predictor kind. */
    tdc_pred2d_kind kind = TDC_PRED2D_AUTO;
    if (params) {
        const tdc_pred2d_params *p = (const tdc_pred2d_params *)params;
        kind = p->kind;
    }

    int64_t n = nx * ny;
    if (kind == TDC_PRED2D_AUTO) {
        if (n > 0) {
            if (!in->data) return TDC_E_INVAL;
            kind = pred2d_auto_select(in->dtype, (const uint8_t *)in->data, nx, ny);
        } else {
            kind = TDC_PRED2D_AVERAGE; /* arbitrary; nothing to encode */
        }
    } else if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
               kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH) {
        return TDC_E_INVAL; /* PLANE / unknown — not handled by this file */
    }

    /* Side metadata: 1 byte = resolved kind. */
    tdc_status st = tdc_buf_reserve(side_out, 1u);
    if (st != TDC_OK) return st;
    side_out->data[0] = (uint8_t)kind;
    side_out->size    = 1u;

    /* Reserve residual output. */
    size_t bytes = (size_t)n * elem_size;
    st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;

    if (n == 0) {
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    pred2d_encode_sweep(in->dtype, kind,
                        (const uint8_t *)in->data,
                        residual_out->data,
                        nx, ny);
    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status pred2d_decode(tdc_block      *out,
                                const void     *params,
                                tdc_dtype       residual_dtype,
                                const uint8_t  *residuals, size_t residual_size,
                                const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (out->shape.rank != 2)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!pred2d_dtype_accepted(out->dtype))  return TDC_E_DTYPE;

    int64_t ny = out->shape.dim[0];
    int64_t nx = out->shape.dim[1];
    if (nx < 0 || ny < 0)                                     return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)            return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    /* Side metadata: exactly 1 byte = the resolved predictor kind. */
    if (side_size != 1u || side_meta == NULL) return TDC_E_CORRUPT;
    tdc_pred2d_kind kind = (tdc_pred2d_kind)side_meta[0];
    if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
        kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH) {
        return TDC_E_CORRUPT;
    }

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    pred2d_decode_sweep(out->dtype, kind, residuals, (uint8_t *)out->data, nx, ny);
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_pred2d_vt = {
    .id               = TDC_MODEL_PRED_2D,
    .name             = "pred2d",
    .accepted_dtypes  = PRED2D_ACCEPTED_DTYPES,
    .accepted_layouts = PRED2D_ACCEPTED_LAYOUTS,
    .encode           = pred2d_encode,
    .decode           = pred2d_decode,
};
