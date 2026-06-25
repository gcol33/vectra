/*
 * src/model/stack2d.c
 *
 * TDC_MODEL_STACK_2D — per-slice 2D predictor over a STACK_2D block
 * (rank 3, shape = {n_slices, ny, nx}).
 *
 * Strategy: loop over slices, dispatch each (ny, nx) plane to the
 * pred2d model vtable. Optionally a per-pixel inter-slice residual
 * (slice[i] - slice[i-1]) before the in-plane prediction, controlled
 * by tdc_stack2d_params::inter_slice.
 *
 * Side metadata: 2 bytes: [resolved_pred2d_kind, inter_slice_flag].
 *
 * Residual stream: concatenation of per-slice pred2d residuals (each
 * slice is ny*nx*elem_size bytes). The pred2d side metadata (1 byte
 * per slice, always the same kind) is NOT included per-slice — the
 * stack2d model owns its own 2-byte side metadata.
 *
 * Accepted dtypes: same as pred2d (i8/i16/i32/u8/u16/u32/f16/f32/f64).
 * Accepted layouts: STACK_2D only (rank 3).
 *
 * Reuses pred2d through the vtable to avoid duplicating prediction logic.
 */

#include "tdc/model.h"
#include "tdc/codec.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"
#include "../layout/layout_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define STACK2D_ACCEPTED_DTYPES (        \
    TDC_DT_BIT(TDC_DT_I8)  |             \
    TDC_DT_BIT(TDC_DT_I16) |             \
    TDC_DT_BIT(TDC_DT_I32) |             \
    TDC_DT_BIT(TDC_DT_U8)  |             \
    TDC_DT_BIT(TDC_DT_U16) |             \
    TDC_DT_BIT(TDC_DT_U32) |             \
    TDC_DT_BIT(TDC_DT_F16) |             \
    TDC_DT_BIT(TDC_DT_F32) |             \
    TDC_DT_BIT(TDC_DT_F64))

#define STACK2D_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_STACK_2D)

static int stack2d_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(STACK2D_ACCEPTED_DTYPES, dt);
}

/* ----- Inter-slice delta ------------------------------------------------- */
/*
 * Subtracts the previous slice from the current slice element-wise.
 * The subtraction is modular at the dtype width (unsigned cast).
 * Operates on raw bytes via memcpy-based load/store.
 */

static void inter_slice_subtract(uint8_t *cur, const uint8_t *prev,
                                 size_t n_bytes, size_t elem_size) {
    size_t n = n_bytes / elem_size;
    switch (elem_size) {
        case 1:
            for (size_t i = 0; i < n; ++i)
                cur[i] = (uint8_t)(cur[i] - prev[i]);
            break;
        case 2:
            for (size_t i = 0; i < n; ++i) {
                uint16_t a, b;
                memcpy(&a, cur  + i * 2u, 2u);
                memcpy(&b, prev + i * 2u, 2u);
                a = (uint16_t)(a - b);
                memcpy(cur + i * 2u, &a, 2u);
            }
            break;
        case 4:
            for (size_t i = 0; i < n; ++i) {
                uint32_t a, b;
                memcpy(&a, cur  + i * 4u, 4u);
                memcpy(&b, prev + i * 4u, 4u);
                a = a - b;
                memcpy(cur + i * 4u, &a, 4u);
            }
            break;
        case 8:
            for (size_t i = 0; i < n; ++i) {
                uint64_t a, b;
                memcpy(&a, cur  + i * 8u, 8u);
                memcpy(&b, prev + i * 8u, 8u);
                a = a - b;
                memcpy(cur + i * 8u, &a, 8u);
            }
            break;
        default: break;
    }
}

static void inter_slice_add(uint8_t *cur, const uint8_t *prev,
                            size_t n_bytes, size_t elem_size) {
    size_t n = n_bytes / elem_size;
    switch (elem_size) {
        case 1:
            for (size_t i = 0; i < n; ++i)
                cur[i] = (uint8_t)(cur[i] + prev[i]);
            break;
        case 2:
            for (size_t i = 0; i < n; ++i) {
                uint16_t a, b;
                memcpy(&a, cur  + i * 2u, 2u);
                memcpy(&b, prev + i * 2u, 2u);
                a = (uint16_t)(a + b);
                memcpy(cur + i * 2u, &a, 2u);
            }
            break;
        case 4:
            for (size_t i = 0; i < n; ++i) {
                uint32_t a, b;
                memcpy(&a, cur  + i * 4u, 4u);
                memcpy(&b, prev + i * 4u, 4u);
                a = a + b;
                memcpy(cur + i * 4u, &a, 4u);
            }
            break;
        case 8:
            for (size_t i = 0; i < n; ++i) {
                uint64_t a, b;
                memcpy(&a, cur  + i * 8u, 8u);
                memcpy(&b, prev + i * 8u, 8u);
                a = a + b;
                memcpy(cur + i * 8u, &a, 8u);
            }
            break;
        default: break;
    }
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status stack2d_encode(const tdc_block *in,
                                 const void      *params,
                                 tdc_buffer      *residual_out,
                                 tdc_dtype       *residual_dtype,
                                 tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_STACK_2D) return TDC_E_LAYOUT;
    if (in->shape.rank != 3)               return TDC_E_SHAPE;
    if (!stack2d_dtype_accepted(in->dtype)) return TDC_E_DTYPE;

    int64_t nz = in->shape.dim[0];  /* n_slices */
    int64_t ny = in->shape.dim[1];
    int64_t nx = in->shape.dim[2];
    if (nz < 0 || ny < 0 || nx < 0) return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t slice_elems = ny * nx;
    size_t  slice_bytes = (size_t)slice_elems * elem_size;
    int64_t total_elems = nz * slice_elems;
    size_t  total_bytes = (size_t)total_elems * elem_size;

    /* Parse params. */
    tdc_pred2d_kind kind = TDC_PRED2D_AUTO;
    int inter_slice = 0;
    if (params) {
        const tdc_stack2d_params *p = (const tdc_stack2d_params *)params;
        kind = p->kind;
        inter_slice = p->inter_slice ? 1 : 0;
    }

    /* Get pred2d vtable for delegation. */
    const tdc_model_vt *pred2d_vt = &tdc_model_pred2d_vt;

    /* Side metadata: 2 bytes [pred2d_kind, inter_slice_flag]. */
    tdc_status st = tdc_buf_reserve(side_out, 2u);
    if (st != TDC_OK) return st;

    /* Reserve residual output. */
    st = tdc_buf_reserve(residual_out, total_bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;

    if (total_elems == 0) {
        /* Resolve kind for empty blocks. */
        if (kind == TDC_PRED2D_AUTO) kind = TDC_PRED2D_PAETH;
        side_out->data[0] = (uint8_t)kind;
        side_out->data[1] = (uint8_t)inter_slice;
        side_out->size    = 2u;
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    /* Scratch buffer for inter-slice delta (one slice). */
    uint8_t *scratch = NULL;
    if (inter_slice && nz > 1) {
        scratch = (uint8_t *)residual_out->realloc_fn(
            residual_out->user, NULL, slice_bytes);
        if (!scratch) return TDC_E_NOMEM;
    }

    /* Auto-select pred2d kind from the first slice. */
    const uint8_t *first_slice = tdc_stack2d_slice_const(in->data, 0, ny, nx, elem_size);
    if (kind == TDC_PRED2D_AUTO) {
        /* Encode slice 0 via pred2d with AUTO to let it pick. */
        tdc_block s0_blk = {0};
        s0_blk.data   = (void *)first_slice;
        s0_blk.dtype  = in->dtype;
        s0_blk.layout = TDC_LAYOUT_RASTER_2D;
        s0_blk.shape.rank = 2;
        s0_blk.shape.dim[0] = ny;
        s0_blk.shape.dim[1] = nx;
        s0_blk.shape.stride[0] = nx;
        s0_blk.shape.stride[1] = 1;

        tdc_buffer tmp_res  = {0};
        tdc_buffer tmp_side = {0};
        tmp_res.realloc_fn  = residual_out->realloc_fn;
        tmp_res.user        = residual_out->user;
        tmp_side.realloc_fn = side_out->realloc_fn;
        tmp_side.user       = side_out->user;

        tdc_dtype  tmp_rdt = (tdc_dtype)0;
        tdc_pred2d_params auto_params = { .kind = TDC_PRED2D_AUTO };

        st = pred2d_vt->encode(&s0_blk, &auto_params, &tmp_res, &tmp_rdt, &tmp_side);
        if (st != TDC_OK) {
            if (scratch) residual_out->realloc_fn(residual_out->user, scratch, 0);
            if (tmp_res.data)  residual_out->realloc_fn(residual_out->user, tmp_res.data, 0);
            if (tmp_side.data) side_out->realloc_fn(side_out->user, tmp_side.data, 0);
            return st;
        }
        kind = (tdc_pred2d_kind)tmp_side.data[0];

        if (tmp_res.data)  residual_out->realloc_fn(residual_out->user, tmp_res.data, 0);
        if (tmp_side.data) side_out->realloc_fn(side_out->user, tmp_side.data, 0);
    }

    /* Write stack2d side metadata. */
    side_out->data[0] = (uint8_t)kind;
    side_out->data[1] = (uint8_t)inter_slice;
    side_out->size    = 2u;

    /* Per-slice encode via pred2d. */
    tdc_pred2d_params slice_params = { .kind = kind };

    /* Temporary buffers for per-slice pred2d encode. */
    tdc_buffer slice_res  = {0};
    tdc_buffer slice_side = {0};
    slice_res.realloc_fn  = residual_out->realloc_fn;
    slice_res.user        = residual_out->user;
    slice_side.realloc_fn = side_out->realloc_fn;
    slice_side.user       = side_out->user;

    for (int64_t s = 0; s < nz; ++s) {
        const uint8_t *slice_data = tdc_stack2d_slice_const(in->data, s, ny, nx, elem_size);
        const uint8_t *pred_input = slice_data;

        /* Inter-slice delta: slice[s] - slice[s-1]. */
        if (inter_slice && s > 0) {
            const uint8_t *prev_slice = tdc_stack2d_slice_const(in->data, s - 1, ny, nx, elem_size);
            memcpy(scratch, slice_data, slice_bytes);
            inter_slice_subtract(scratch, prev_slice, slice_bytes, elem_size);
            pred_input = scratch;
        }

        tdc_block s_blk = {0};
        s_blk.data   = (void *)pred_input;
        s_blk.dtype  = in->dtype;
        s_blk.layout = TDC_LAYOUT_RASTER_2D;
        s_blk.shape.rank = 2;
        s_blk.shape.dim[0] = ny;
        s_blk.shape.dim[1] = nx;
        s_blk.shape.stride[0] = nx;
        s_blk.shape.stride[1] = 1;

        slice_res.size = 0;
        slice_side.size = 0;
        tdc_dtype s_rdt = (tdc_dtype)0;

        st = pred2d_vt->encode(&s_blk, &slice_params, &slice_res, &s_rdt, &slice_side);
        if (st != TDC_OK) {
            if (scratch) residual_out->realloc_fn(residual_out->user, scratch, 0);
            if (slice_res.data) residual_out->realloc_fn(residual_out->user, slice_res.data, 0);
            if (slice_side.data) side_out->realloc_fn(side_out->user, slice_side.data, 0);
            return st;
        }

        /* Copy per-slice residuals into the output at the right offset. */
        memcpy(residual_out->data + (size_t)s * slice_bytes,
               slice_res.data, slice_bytes);
    }

    if (scratch) residual_out->realloc_fn(residual_out->user, scratch, 0);
    if (slice_res.data) residual_out->realloc_fn(residual_out->user, slice_res.data, 0);
    if (slice_side.data) side_out->realloc_fn(side_out->user, slice_side.data, 0);

    residual_out->size = total_bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status stack2d_decode(tdc_block      *out,
                                 const void     *params,
                                 tdc_dtype       residual_dtype,
                                 const uint8_t  *residuals, size_t residual_size,
                                 const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_STACK_2D) return TDC_E_LAYOUT;
    if (out->shape.rank != 3)               return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)       return TDC_E_DTYPE;
    if (!stack2d_dtype_accepted(out->dtype)) return TDC_E_DTYPE;

    int64_t nz = out->shape.dim[0];
    int64_t ny = out->shape.dim[1];
    int64_t nx = out->shape.dim[2];
    if (nz < 0 || ny < 0 || nx < 0) return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t slice_elems = ny * nx;
    size_t  slice_bytes = (size_t)slice_elems * elem_size;
    int64_t total_elems = nz * slice_elems;
    size_t  total_bytes = (size_t)total_elems * elem_size;

    if (residual_size != total_bytes) return TDC_E_CORRUPT;
    if (side_size != 2u || side_meta == NULL) return TDC_E_CORRUPT;

    tdc_pred2d_kind kind = (tdc_pred2d_kind)side_meta[0];
    int inter_slice = (int)side_meta[1];

    if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
        kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH) {
        return TDC_E_CORRUPT;
    }
    if (inter_slice != 0 && inter_slice != 1) return TDC_E_CORRUPT;

    if (total_elems == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    /* Get pred2d vtable for delegation. */
    const tdc_model_vt *pred2d_vt = &tdc_model_pred2d_vt;
    uint8_t pred2d_side[1] = { (uint8_t)kind };

    /* Decode each slice via pred2d, then undo inter-slice delta. */
    for (int64_t s = 0; s < nz; ++s) {
        uint8_t *slice_dst = tdc_stack2d_slice(out->data, s, ny, nx, elem_size);
        const uint8_t *slice_res = residuals + (size_t)s * slice_bytes;

        tdc_block s_blk = {0};
        s_blk.data   = slice_dst;
        s_blk.dtype  = out->dtype;
        s_blk.layout = TDC_LAYOUT_RASTER_2D;
        s_blk.shape.rank = 2;
        s_blk.shape.dim[0] = ny;
        s_blk.shape.dim[1] = nx;
        s_blk.shape.stride[0] = nx;
        s_blk.shape.stride[1] = 1;

        tdc_status st = pred2d_vt->decode(&s_blk, NULL, residual_dtype,
                                          slice_res, slice_bytes,
                                          pred2d_side, 1u);
        if (st != TDC_OK) return st;

        /* Undo inter-slice delta: slice[s] += slice[s-1]. */
        if (inter_slice && s > 0) {
            const uint8_t *prev_slice = tdc_stack2d_slice_const(out->data, s - 1, ny, nx, elem_size);
            inter_slice_add(slice_dst, prev_slice, slice_bytes, elem_size);
        }
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_stack2d_vt = {
    .id               = TDC_MODEL_STACK_2D,
    .name             = "stack2d",
    .accepted_dtypes  = STACK2D_ACCEPTED_DTYPES,
    .accepted_layouts = STACK2D_ACCEPTED_LAYOUTS,
    .encode           = stack2d_encode,
    .decode           = stack2d_decode,
};
