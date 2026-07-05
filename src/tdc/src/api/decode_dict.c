/*
 * src/api/decode_dict.c
 *
 * Implements: tdc_decode_block_dict (declared in tdc/codec.h).
 *
 * Dictionary-preserving companion to tdc_decode_block_varlen. Where varlen
 * reconstructs the full n-element value column (repeating each dictionary
 * entry once per occurrence), this entry point runs the same entropy +
 * transform pipeline but stops at the residual and hands back the raw
 * dictionary + u32 index stream. A consumer that interns unique values
 * (an R STRSXP, an Arrow dictionary array) then materializes each unique
 * value once instead of once per row.
 *
 * It reuses driver_decode_block_impl with run_model = 0 so the shared
 * entropy/transform reversal stays the single source of truth; the model's
 * flattening step is skipped. A pre-model hook copies the residual (the
 * index stream) and parses + copies the DICT_1D side meta (the dictionary)
 * into caller-owned buffers.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/types.h"
#include "tdc/format.h"

#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* DICT_1D side-meta header: u32 dict_count, u32 dict_total_bytes, then
 * u32 dict_offsets[dict_count + 1], then dict_total_bytes of value data. */
#define DICT_SIDE_HEADER 8u

typedef struct {
    tdc_buffer     *alloc;
    tdc_dict_block *out;
} dict_hook_ctx;

static tdc_status dict_capture_hook(tdc_block      *dst,
                                    const uint8_t  *residual_data,
                                    size_t          residual_size,
                                    const uint8_t  *side_meta,
                                    size_t          side_size,
                                    tdc_dtype       residual_dtype,
                                    tdc_model_id    model_id,
                                    void           *user) {
    dict_hook_ctx  *ctx = (dict_hook_ctx *)user;
    tdc_buffer     *al  = ctx->alloc;
    tdc_dict_block *out = ctx->out;

    if (model_id != TDC_MODEL_DICT_1D) return TDC_E_UNSUPPORTED;
    if (residual_dtype != TDC_DT_U32)  return TDC_E_CORRUPT;

    int64_t n = (dst->shape.rank > 0) ? dst->shape.dim[0] : 0;
    if (n < 0) return TDC_E_SHAPE;
    if ((uint64_t)residual_size != (uint64_t)n * 4u) return TDC_E_CORRUPT;

    /* Parse + validate the side-meta dictionary header. */
    if (side_meta == NULL || side_size < DICT_SIDE_HEADER) return TDC_E_CORRUPT;
    uint32_t dict_count, dict_total;
    memcpy(&dict_count, side_meta + 0, 4u);
    memcpy(&dict_total, side_meta + 4, 4u);

    size_t offs_bytes = (size_t)(dict_count + 1u) * 4u;
    if (side_size != (size_t)DICT_SIDE_HEADER + offs_bytes + (size_t)dict_total)
        return TDC_E_CORRUPT;

    const uint8_t *offs_p = side_meta + DICT_SIDE_HEADER;
    const uint8_t *data_p = offs_p + offs_bytes;

    /* Offsets must be non-decreasing and terminate at dict_total. */
    uint32_t prev = 0u;
    for (uint32_t d = 0; d <= dict_count; ++d) {
        uint32_t o;
        memcpy(&o, offs_p + (size_t)d * 4u, 4u);
        if (o < prev) return TDC_E_CORRUPT;
        prev = o;
    }
    if (prev != dict_total) return TDC_E_CORRUPT;

    /* Every index must fall inside the dictionary so the caller can index
     * without bounds checks. */
    for (int64_t i = 0; i < n; ++i) {
        uint32_t idx;
        memcpy(&idx, residual_data + (size_t)i * 4u, 4u);
        if (idx >= dict_count) return TDC_E_CORRUPT;
    }

    /* Allocate + copy the three caller-owned arrays. On any failure the
     * public wrapper frees whatever landed in out. */
    uint32_t *offs_out = (uint32_t *)al->realloc_fn(al->user, NULL, offs_bytes);
    if (!offs_out) return TDC_E_NOMEM;
    memcpy(offs_out, offs_p, offs_bytes);
    out->dict_offsets = offs_out;

    if (dict_total > 0u) {
        uint8_t *data_out = (uint8_t *)al->realloc_fn(al->user, NULL, dict_total);
        if (!data_out) return TDC_E_NOMEM;
        memcpy(data_out, data_p, dict_total);
        out->dict_data = data_out;
    }

    if (residual_size > 0u) {
        uint32_t *idx_out = (uint32_t *)al->realloc_fn(al->user, NULL, residual_size);
        if (!idx_out) return TDC_E_NOMEM;
        memcpy(idx_out, residual_data, residual_size);
        out->indices = idx_out;
    }

    out->n              = n;
    out->dict_count     = dict_count;
    out->dict_data_size = dict_total;
    return TDC_OK;
}

tdc_status tdc_decode_block_dict(const uint8_t *src, size_t src_size,
                                 tdc_dict_block *out, tdc_buffer *alloc) {
    if (!src || !out || !alloc)     return TDC_E_INVAL;
    if (!alloc->realloc_fn)         return TDC_E_INVAL;
    if (out->indices || out->dict_offsets || out->dict_data) return TDC_E_INVAL;

    out->n = 0;
    out->dict_count = 0;
    out->dict_data_size = 0;

    if (src_size < TDC_BLOCK_HEADER_SIZE) return TDC_E_CORRUPT;

    /* Peek the header so dst carries the exact dtype/layout/shape the
     * pipeline validates against the record (it refuses to rewrite them).
     * Reject non-DICT_1D up front for a clean TDC_E_UNSUPPORTED contract. */
    tdc_block_record hdr;
    memcpy(&hdr, src, TDC_BLOCK_HEADER_SIZE);
    if ((tdc_model_id)hdr.model_id != TDC_MODEL_DICT_1D) return TDC_E_UNSUPPORTED;

    /* dst carries the shape/dtype the pipeline validates against the record.
     * The dict path never writes dst->data, so leave it NULL. */
    tdc_block dst = {0};
    dst.dtype      = (tdc_dtype)hdr.dtype;
    dst.layout     = (tdc_layout)hdr.layout;
    dst.shape.rank = hdr.rank;
    for (uint8_t i = 0; i < hdr.rank && i < TDC_MAX_RANK; ++i) {
        dst.shape.dim[i] = hdr.dim[i];
    }
    tdc_shape_set_contiguous(&dst.shape);

    dict_hook_ctx ctx = { .alloc = alloc, .out = out };

    tdc_status st = driver_decode_block_impl(src, src_size, &dst, alloc,
                                             "decdt",
                                             dict_capture_hook, &ctx, 0);

    if (st != TDC_OK) {
        if (out->indices)      { alloc->realloc_fn(alloc->user, out->indices, 0);      out->indices = NULL; }
        if (out->dict_offsets) { alloc->realloc_fn(alloc->user, out->dict_offsets, 0); out->dict_offsets = NULL; }
        if (out->dict_data)    { alloc->realloc_fn(alloc->user, out->dict_data, 0);    out->dict_data = NULL; }
        out->n = 0; out->dict_count = 0; out->dict_data_size = 0;
    }
    return st;
}
