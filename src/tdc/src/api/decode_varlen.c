/*
 * src/api/decode_varlen.c
 *
 * Implements: tdc_decode_block_varlen (declared in tdc/codec.h).
 *
 * Variable-width companion to tdc_decode_block_into. Fixed-width entry
 * points let the caller pre-size dst->data from the 80-byte block header
 * alone (n_elems * dtype_size). Variable-width blocks (TDC_DT_STRING in
 * v0) cannot do that: the output heap byte count depends on the
 * dictionary indices in the residual stream, which only become available
 * after the entropy + transform stages have run.
 *
 * Solution: hook the shared decode pipeline. We hand
 * driver_decode_block_impl a callback that runs once after the residual
 * is in hand and before the model decode. The callback walks the
 * residual + side metadata via a model-specific size-query helper
 * (dict1d_compute_output_size), allocates dst->offsets and dst->data
 * from the caller's realloc_fn, and the model.decode then writes into
 * those buffers in place.
 *
 * On any failure (pipeline error or hook OOM) the wrapper frees whatever
 * the hook allocated and returns a NULL dst->data / dst->offsets to the
 * caller — so the contract is "either both are set on TDC_OK, or both
 * are NULL on error".
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/types.h"

#include "driver_internal.h"
#include "../model/model_internal.h"

#include <stddef.h>
#include <stdint.h>

/* ----- Hook context ------------------------------------------------------- */
/*
 * Carried through driver_decode_block_impl as the opaque user pointer.
 * Holds the caller's allocator template; the hook frees nothing on
 * failure — the wrapper does that, after seeing the non-OK status.
 */
typedef struct {
    tdc_buffer *alloc;
} varlen_hook_ctx;

static tdc_status varlen_alloc_hook(tdc_block      *dst,
                                    const uint8_t  *residual_data,
                                    size_t          residual_size,
                                    const uint8_t  *side_meta,
                                    size_t          side_size,
                                    tdc_dtype       residual_dtype,
                                    tdc_model_id    model_id,
                                    void           *user) {
    (void)residual_dtype;
    varlen_hook_ctx *ctx = (varlen_hook_ctx *)user;
    tdc_buffer      *al  = ctx->alloc;

    int64_t n = (dst->shape.rank > 0) ? dst->shape.dim[0] : 0;
    if (n < 0) return TDC_E_SHAPE;

    /* Empty block: still allocate a single-entry offsets sentinel so the
     * caller can read offsets[0] == 0 without a NULL check. dst->data is
     * left NULL in this case — there is no heap. */
    if (n == 0) {
        uint32_t *offs = (uint32_t *)al->realloc_fn(al->user, NULL,
                                                    sizeof(uint32_t));
        if (!offs) return TDC_E_NOMEM;
        offs[0]      = 0u;
        dst->offsets = offs;
        dst->data    = NULL;
        return TDC_OK;
    }

    /* Compute the exact heap size for the requested model. v0 only has
     * one variable-width producer (DICT_1D); future producers will add
     * a case here and ship their own *_compute_output_size helper. */
    size_t heap_bytes = 0;
    tdc_status st;
    switch (model_id) {
        case TDC_MODEL_DICT_1D:
            st = dict1d_compute_output_size(residual_data, residual_size,
                                            side_meta, side_size,
                                            n, &heap_bytes);
            if (st != TDC_OK) return st;
            break;
        default:
            return TDC_E_UNSUPPORTED;
    }

    /* offsets[]: always (n+1) entries, even for zero heap_bytes — the
     * caller relies on offsets[i+1] - offsets[i] for every row. */
    uint32_t *offs = (uint32_t *)al->realloc_fn(
        al->user, NULL, sizeof(uint32_t) * (size_t)(n + 1));
    if (!offs) return TDC_E_NOMEM;

    void *heap = NULL;
    if (heap_bytes > 0) {
        heap = al->realloc_fn(al->user, NULL, heap_bytes);
        if (!heap) {
            al->realloc_fn(al->user, offs, 0);
            return TDC_E_NOMEM;
        }
    }

    dst->offsets = offs;
    dst->data    = heap;
    return TDC_OK;
}

/* ----- Public entry ------------------------------------------------------- */

tdc_status tdc_decode_block_varlen(const uint8_t *src, size_t src_size,
                                   tdc_block     *dst, tdc_buffer *alloc) {
    if (!src || !dst || !alloc)       return TDC_E_INVAL;
    if (!alloc->realloc_fn)           return TDC_E_INVAL;
    if (!tdc_dtype_is_variable_length(dst->dtype)) return TDC_E_DTYPE;

    /* The wrapper owns the dst->data and dst->offsets allocations. If
     * the caller already populated them we would either leak (overwrite)
     * or alias (re-free); refuse rather than guess. */
    if (dst->data != NULL || dst->offsets != NULL) return TDC_E_INVAL;

    varlen_hook_ctx ctx = { .alloc = alloc };

    tdc_status st = driver_decode_block_impl(src, src_size, dst, alloc,
                                             "decvl",
                                             varlen_alloc_hook, &ctx, 1);

    if (st != TDC_OK) {
        /* Free anything the hook allocated. The pipeline never touches
         * dst->data / dst->offsets directly, so on failure these are
         * either NULL (hook never ran or returned early) or hold valid
         * caller-owned pointers from a successful hook call followed by
         * a later pipeline failure. */
        if (dst->data) {
            alloc->realloc_fn(alloc->user, dst->data, 0);
            dst->data = NULL;
        }
        if (dst->offsets) {
            alloc->realloc_fn(alloc->user, dst->offsets, 0);
            dst->offsets = NULL;
        }
    }
    return st;
}
