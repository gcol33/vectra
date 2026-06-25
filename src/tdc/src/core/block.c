/*
 * src/core/block.c
 *
 * tdc_block helpers: validation, contiguous-stride setup, n_elems math.
 * Pure plumbing — knows nothing about compression.
 *
 * Implements:
 *   tdc_shape_set_contiguous   (declared in tdc/types.h)
 *   tdc_block_validate         (declared in tdc/types.h)
 */

#include "tdc/types.h"

#include <stddef.h>
#include <stdint.h>

/* Row-major contiguous strides (in elements). For rank R, the rightmost
 * stride is 1 and each step left multiplies by the next dim. Trailing
 * entries past `rank` are zeroed for forward compatibility (the format
 * docs require strides[rank..MAX) to be 0). */
void tdc_shape_set_contiguous(tdc_shape *s) {
    if (!s) return;
    if (s->rank == 0 || s->rank > TDC_MAX_RANK) {
        for (uint8_t i = 0; i < TDC_MAX_RANK; ++i) s->stride[i] = 0;
        return;
    }
    int64_t acc = 1;
    for (int i = (int)s->rank - 1; i >= 0; --i) {
        s->stride[i] = acc;
        if (s->dim[i] > 0) {
            /* Guard against overflow in accumulation. Caller will catch
             * unrealistic shapes via tdc_block_validate first. */
            if (acc != 0 && s->dim[i] > INT64_MAX / acc) {
                acc = INT64_MAX;
            } else {
                acc *= s->dim[i];
            }
        } else {
            acc = 0;
        }
    }
    for (uint8_t i = s->rank; i < TDC_MAX_RANK; ++i) s->stride[i] = 0;
}

/* Expected rank for a given semantic layout. Returns -1 if the layout id
 * is not one of the four v0 enums. */
static int block_expected_rank(tdc_layout lo) {
    switch (lo) {
        case TDC_LAYOUT_VECTOR_1D: return 1;
        case TDC_LAYOUT_RASTER_2D: return 2;
        case TDC_LAYOUT_STACK_2D:  return 3;
        case TDC_LAYOUT_VOLUME_3D: return 3;
        default:                   return -1;
    }
}

/* Cheap structural validator. Does NOT check that data/validity pointers
 * are non-NULL — n_elems == 0 is legal and skips data dereference, and
 * the encode/decode call sites already guard against NULL data on
 * non-empty blocks. */
tdc_status tdc_block_validate(const tdc_block *blk) {
    if (!blk) return TDC_E_INVAL;

    /* dtype id must be in the known range. v0 has no STRING model so
     * STRING is rejected here unless someone wires DICT_1D later. The
     * model dispatcher will reject string blocks fed to numeric models
     * via TDC_E_DTYPE; this gate is purely structural. */
    if ((int)blk->dtype < TDC_DT_I8 ||
        ((int)blk->dtype > TDC_DT_STRING && blk->dtype != TDC_DT_F16)) {
        return TDC_E_DTYPE;
    }

    int expected = block_expected_rank(blk->layout);
    if (expected < 0) return TDC_E_LAYOUT;
    if ((int)blk->shape.rank != expected) return TDC_E_SHAPE;

    /* Dim sanity: non-negative, no overflow under the n_elems product. */
    int64_t n = 1;
    for (uint8_t i = 0; i < blk->shape.rank; ++i) {
        int64_t d = blk->shape.dim[i];
        if (d < 0) return TDC_E_SHAPE;
        if (d != 0 && n > INT64_MAX / d) return TDC_E_SHAPE;
        n *= d;
    }

    /* Strings carry an offsets[] sidecar; numeric dtypes must NOT.
     * The contract is documented at the offsets field in tdc/types.h. */
    if (blk->dtype == TDC_DT_STRING) {
        if (blk->shape.rank != 1) return TDC_E_LAYOUT;
        /* offsets may be NULL only if n == 0; otherwise required. */
        if (n > 0 && blk->offsets == NULL) return TDC_E_INVAL;
    } else {
        if (blk->offsets != NULL) return TDC_E_INVAL;
    }

    return TDC_OK;
}
