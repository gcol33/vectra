/*
 * src/api/driver_internal.h
 *
 * Internal helpers shared between src/api/encode.c and src/api/decode.c.
 * Not part of the public ABI; lives under src/, like the other
 * *_internal.h headers.
 *
 * Three responsibilities:
 *
 *   1. Scratch tdc_buffer lifecycle. The driver allocates two ping-pong
 *      buffers per encode/decode call, plus one each for model side
 *      metadata, transform params TLV, and entropy payload, all backed
 *      by the caller's realloc_fn. These helpers wrap the init/free
 *      idiom so every error path can call driver_scratch_free() and bail.
 *
 *   2. Per-slot transform params (de)serialization.
 *      Encode appends a TLV entry (slot_index, length, blob) for each
 *      transform that has params. Decode walks the same TLV section and
 *      gathers a per-slot pointer table. v0 only has QUANTIZE actually
 *      using this; ZIGZAG / BYTE_SHUFFLE are stateless.
 *
 *   3. Static dtype mapping for known transforms.
 *      Decode walks the chain in reverse, and each transform's decode
 *      wants the encoder-side input dtype. The driver derives the dtype
 *      sequence forward from the model output dtype using a small
 *      switch on tdc_xform_id. For QUANTIZE the output dtype lives in
 *      the per-slot params blob, so this helper takes the params
 *      pointer and casts it.
 */

#ifndef TDC_API_DRIVER_INTERNAL_H
#define TDC_API_DRIVER_INTERNAL_H

#include "tdc/types.h"
#include "tdc/codec.h"

#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Shared decode pipeline --------------------------------------------- */
/*
 * Single source of truth for the block-decode pipeline. Used by
 * tdc_decode_block, tdc_decode_block_ex, tdc_decode_block_into, and
 * tdc_decode_block_varlen. Public entry points differ only in where
 * they source the scratch allocator, how strict they are about
 * dst->data, and (for varlen) whether they install a pre-model hook
 * to allocate dst->data + dst->offsets from the decoded residual.
 *
 * scratch_parent supplies realloc_fn/user for internal ping-pong buffers;
 * its data/size/capacity fields are ignored. dst->data is NOT allocated
 * here — fixed-width callers must do it before calling; variable-width
 * callers install a hook that allocates after the residual is in hand.
 *
 * timer_tag is a short label (<= 5 chars) used by the stage-timer prints
 * so the entry points can be told apart in decode_profile output.
 *
 * Pre-model hook: invoked exactly once after the entropy + transform
 * stages have produced the residual stream and before the model decode
 * runs. The hook may populate dst->data and dst->offsets via any
 * allocator the caller has access to. Pass NULL for fixed-width entry
 * points that already pre-sized dst->data. Returning non-OK from the
 * hook aborts the decode and forwards the status back to the caller.
 */
typedef tdc_status (*driver_pre_model_hook)(tdc_block      *dst,
                                            const uint8_t  *residual_data,
                                            size_t          residual_size,
                                            const uint8_t  *side_meta,
                                            size_t          side_size,
                                            tdc_dtype       residual_dtype,
                                            tdc_model_id    model_id,
                                            void           *user);

tdc_status driver_decode_block_impl(const uint8_t        *src,
                                    size_t                src_size,
                                    tdc_block            *dst,
                                    const tdc_buffer     *scratch_parent,
                                    const char           *timer_tag,
                                    driver_pre_model_hook hook,
                                    void                 *hook_user);

/* ----- Libc-backed realloc shim ------------------------------------------- */
/*
 * Decode entry points that do NOT take a caller-supplied allocator
 * (tdc_decode_block, tdc_decode_block_into) wrap the libc allocator in a
 * tdc_buffer so the pipeline still allocates via realloc_fn. This is the
 * only place in tdc that calls realloc/free directly.
 */
static inline void *driver_libc_realloc(void *user, void *ptr, size_t new_size) {
    (void)user;
    if (new_size == 0) { free(ptr); return NULL; }
    return realloc(ptr, new_size);
}

static inline tdc_buffer driver_make_libc_scratch_parent(void) {
    tdc_buffer b;
    b.data       = NULL;
    b.size       = 0;
    b.capacity   = 0;
    b.realloc_fn = driver_libc_realloc;
    b.user       = NULL;
    return b;
}

/* ----- Scratch buffer plumbing -------------------------------------------- */

static inline void driver_scratch_init(tdc_buffer *s, const tdc_buffer *parent) {
    s->data       = NULL;
    s->size       = 0;
    s->capacity   = 0;
    s->realloc_fn = parent->realloc_fn;
    s->user       = parent->user;
}

static inline void driver_scratch_free(tdc_buffer *s) {
    if (s && s->data && s->realloc_fn) {
        s->realloc_fn(s->user, s->data, 0);
    }
    if (s) {
        s->data     = NULL;
        s->size     = 0;
        s->capacity = 0;
    }
}

/* ----- Per-slot transform params: TLV (de)serialization ------------------- */
/*
 * The TLV section in the on-disk block record is a sequence of entries:
 *
 *     u16 slot_index    (0..TDC_MAX_TRANSFORMS-1)
 *     u16 blob_length
 *     blob_length bytes (transform-specific, little-endian)
 *
 * Encode appends one entry per slot whose transform has serializable
 * params. Decode walks the section once and fills a per-slot pointer
 * table; the pointers are into a small driver-side scratch struct so
 * the buffer that holds the TLV bytes can be freed after parsing.
 *
 * v0 only QUANTIZE has params: f64 scale, f64 offset, u8 target = 17 bytes.
 */

#define DRIVER_TLV_HDR_BYTES         4u
#define DRIVER_QUANTIZE_BLOB_BYTES   17u

/*
 * Append the params blob for `xid` (located at `params`, may be NULL) to
 * the TLV scratch buffer `out`, tagged with `slot_index`. Stateless
 * transforms (and NULL params) are no-ops.
 */
static inline tdc_status driver_xform_params_append(tdc_buffer  *out,
                                                    uint16_t     slot_index,
                                                    tdc_xform_id xid,
                                                    const void  *params) {
    if (!params) return TDC_OK;

    uint16_t blob_len = 0;
    uint8_t  scratch[DRIVER_QUANTIZE_BLOB_BYTES];

    switch (xid) {
        case TDC_XFORM_QUANTIZE: {
            const tdc_quantize_params *qp = (const tdc_quantize_params *)params;
            double s = qp->scale;
            double o = qp->offset;
            uint8_t t = (uint8_t)qp->target;
            memcpy(scratch + 0, &s, 8u);
            memcpy(scratch + 8, &o, 8u);
            scratch[16] = t;
            blob_len = DRIVER_QUANTIZE_BLOB_BYTES;
            break;
        }
        case TDC_XFORM_NONE:
        case TDC_XFORM_ZIGZAG:
        case TDC_XFORM_BYTE_SHUFFLE:
        case TDC_XFORM_BIT_SHUFFLE:
        default:
            /* No on-disk params for these. */
            return TDC_OK;
    }

    size_t need = out->size + DRIVER_TLV_HDR_BYTES + blob_len;
    tdc_status st = tdc_buf_reserve(out, need);
    if (st != TDC_OK) return st;

    uint8_t *p = out->data + out->size;
    memcpy(p + 0, &slot_index, 2u);
    memcpy(p + 2, &blob_len,   2u);
    memcpy(p + 4, scratch, blob_len);
    out->size = need;
    return TDC_OK;
}

/*
 * Decoded TLV view: per-slot params pointer table + the parsed
 * tdc_quantize_params structs that the pointers point into. Stateful
 * (held on the decode-side stack) so callers do not need to free
 * anything separately.
 */
typedef struct {
    const void           *xform_params[TDC_MAX_TRANSFORMS];
    tdc_quantize_params   quantize[TDC_MAX_TRANSFORMS];
} driver_xform_params_table;

static inline void driver_xform_params_table_init(driver_xform_params_table *t) {
    for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
        t->xform_params[i] = NULL;
    }
}

/*
 * Parse the TLV section pointed to by (tlv, tlv_size) into `t`. Validates
 * length fields, slot bounds, and that each entry's xid (looked up via
 * `xform_ids`) matches the expected blob length. Returns TDC_E_CORRUPT
 * on any inconsistency.
 */
static inline tdc_status driver_xform_params_parse(driver_xform_params_table *t,
                                                   const uint16_t *xform_ids,
                                                   const uint8_t  *tlv,
                                                   size_t          tlv_size) {
    driver_xform_params_table_init(t);
    if (tlv_size == 0) return TDC_OK;
    if (!tlv) return TDC_E_CORRUPT;

    size_t off = 0;
    while (off < tlv_size) {
        if (tlv_size - off < DRIVER_TLV_HDR_BYTES) return TDC_E_CORRUPT;
        uint16_t slot, blob_len;
        memcpy(&slot,     tlv + off + 0, 2u);
        memcpy(&blob_len, tlv + off + 2, 2u);
        off += DRIVER_TLV_HDR_BYTES;
        if (slot >= (uint16_t)TDC_MAX_TRANSFORMS) return TDC_E_CORRUPT;
        if (tlv_size - off < (size_t)blob_len)    return TDC_E_CORRUPT;
        if (t->xform_params[slot] != NULL)        return TDC_E_CORRUPT; /* duplicate slot */

        tdc_xform_id xid = (tdc_xform_id)xform_ids[slot];
        switch (xid) {
            case TDC_XFORM_QUANTIZE: {
                if (blob_len != DRIVER_QUANTIZE_BLOB_BYTES) return TDC_E_CORRUPT;
                tdc_quantize_params *qp = &t->quantize[slot];
                double s, o;
                memcpy(&s, tlv + off + 0, 8u);
                memcpy(&o, tlv + off + 8, 8u);
                qp->scale  = s;
                qp->offset = o;
                qp->target = (tdc_dtype)tlv[off + 16];
                t->xform_params[slot] = qp;
                break;
            }
            default:
                /* TLV entry refers to a transform that has no on-disk
                 * params shape — corrupt by definition (encode would not
                 * have written it). */
                return TDC_E_CORRUPT;
        }
        off += blob_len;
    }
    return TDC_OK;
}

/* ----- Static model residual dtype mapping -------------------------------- */
/*
 * Returns the residual dtype that model `mid` produces from input dtype
 * `in_dtype`. Most v0 models pass dtype through unchanged (residual dtype
 * == in_dtype). The dictionary models are the exception: both DICT_1D
 * (string) and DICT_NUMERIC_1D (fixed-width numeric) always emit u32
 * indices regardless of the input dtype.
 *
 * The encoder learns the residual dtype directly from the model via the
 * out-parameter on encode(); only the decoder needs this static mapping,
 * because it has to seed the forward chain dtype walk before any of the
 * runtime stages have run.
 *
 * Composite models whose residual dtype is chosen per-block (recorded in
 * side meta rather than fixed by the model id) take side_meta + side_size
 * and dispatch to a per-model peek helper. QUANTIZE_PRED_2D is the only
 * such model in v0; its target dtype lives in side_meta[0]. side_meta
 * may be NULL for the static cases that don't need it.
 */
tdc_dtype qp2_residual_dtype_from_side_meta(const uint8_t *side_meta,
                                            size_t         side_meta_size);

static inline tdc_dtype driver_model_residual_dtype(tdc_model_id   mid,
                                                    tdc_dtype      in_dtype,
                                                    const uint8_t *side_meta,
                                                    size_t         side_meta_size) {
    switch (mid) {
        case TDC_MODEL_DICT_1D:         return TDC_DT_U32;
        case TDC_MODEL_DICT_NUMERIC_1D: return TDC_DT_U32;
        case TDC_MODEL_SPARSE_ZERO_1D:  return TDC_DT_U32;
        case TDC_MODEL_QUANTIZE_PRED_2D:
            return qp2_residual_dtype_from_side_meta(side_meta, side_meta_size);
        default:                        return in_dtype;
    }
}

/* ----- Static transform dtype walk ---------------------------------------- */
/*
 * Returns the encoder OUTPUT dtype of `xid` given input dtype `in` and
 * (optional) per-slot params pointer.
 *
 * For QUANTIZE the output dtype is the params target field. For
 * ZIGZAG/BYTE_SHUFFLE/BIT_SHUFFLE it is statically derivable from `in`.
 *
 * Returns 0 if `xid` is unknown or QUANTIZE is missing required params.
 */
static inline tdc_dtype driver_xform_out_dtype(tdc_xform_id xid,
                                               tdc_dtype    in,
                                               const void  *params) {
    switch (xid) {
        case TDC_XFORM_BYTE_SHUFFLE:
        case TDC_XFORM_BIT_SHUFFLE:
        case TDC_XFORM_COMPLEMENT:
            return in;
        case TDC_XFORM_ZIGZAG:
            switch (in) {
                case TDC_DT_I8:  return TDC_DT_U8;
                case TDC_DT_I16: return TDC_DT_U16;
                case TDC_DT_I32: return TDC_DT_U32;
                case TDC_DT_I64: return TDC_DT_U64;
                default:         return (tdc_dtype)0;
            }
        case TDC_XFORM_QUANTIZE: {
            if (!params) return (tdc_dtype)0;
            const tdc_quantize_params *qp = (const tdc_quantize_params *)params;
            return qp->target;
        }
        case TDC_XFORM_NONE:
        default:
            return (tdc_dtype)0;
    }
}

#ifdef __cplusplus
}
#endif

#endif /* TDC_API_DRIVER_INTERNAL_H */
