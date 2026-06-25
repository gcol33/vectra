/*
 * src/format/block_record.c
 *
 * Per-block record read/write/validate.
 *
 * Implements:
 *   tdc_block_record_validate (declared in tdc/format.h)
 *
 * The block record is fixed at 80 bytes (TDC_BLOCK_HEADER_SIZE) and is
 * SELF-DESCRIBING — every field needed to decode the block lives in the
 * record header. See tdc/format.h for the on-disk layout.
 *
 * On-disk order is:
 *   [ tdc_block_record    ]   80 bytes, fixed
 *   [ side metadata       ]   side_meta_size bytes (model state)
 *   [ xform params TLV    ]   xform_params_size bytes
 *   [ entropy payload     ]   payload_size bytes
 *   [ validity bitmap     ]   validity_size bytes (also flagged by HAS_VALIDITY)
 *
 * tdc_block_record_validate is intentionally CHEAP. It does not touch
 * side_meta, xform_params, payload, or validity bytes — those are validated
 * by the model, transform, and entropy stages during decode. The job here
 * is to confirm that the 80-byte header is internally consistent before any
 * further work.
 *
 * Both chains (xform_ids, entropy_ids) use a STICKY terminator: once a
 * NONE slot is seen, every slot after it must also be NONE. An all-NONE
 * entropy chain is legal and means "passthrough" — the encode driver
 * writes residuals verbatim into the payload in that case.
 */

#include "tdc/format.h"

#include <stddef.h>
#include <stdint.h>

static int blk_dtype_known(uint8_t dt) {
    return (dt >= TDC_DT_I8 && dt <= TDC_DT_STRING) || dt == TDC_DT_F16;
}

static int blk_layout_known(uint8_t lo) {
    return lo >= TDC_LAYOUT_VECTOR_1D && lo <= TDC_LAYOUT_VOLUME_3D;
}

static int blk_layout_rank(uint8_t lo) {
    switch (lo) {
        case TDC_LAYOUT_VECTOR_1D: return 1;
        case TDC_LAYOUT_RASTER_2D: return 2;
        case TDC_LAYOUT_STACK_2D:  return 3;
        case TDC_LAYOUT_VOLUME_3D: return 3;
        default:                   return -1;
    }
}

/* Permitted ids for each stage. NONE/0 is the chain terminator for
 * transforms; for the model and entropy slots NONE is invalid because
 * the encoder always selects a concrete backend.
 *
 * Reserved ranges (from tdc/codec.h):
 *   0x0001-0x00FF   core (shipped with tdc)
 *   0x0100-0x01FF   experimental (statically compiled, may change
 *                    without format version bump)
 *   0xFF00-0xFFFF   user-defined (plugin registry)
 *
 * All three ranges are accepted by the validators so blocks encoded with
 * any registered backend decode cleanly. The 0x0200-0xFEFF reserved range
 * is rejected — no existing backend uses it. */
static int blk_model_id_valid(uint16_t id) {
    return (id >= TDC_MODEL_RAW && id <= TDC_MODEL_QUANTIZE_PRED_2D) ||
           (id >= 0x0100u && id <= 0x01FFu) ||
           (id >= 0xFF00u && id <= 0xFFFFu);
}

static int blk_xform_id_valid(uint16_t id) {
    /* 0 is allowed (chain terminator); otherwise must be a known core id,
     * an experimental id, or a user-defined id. */
    return id == TDC_XFORM_NONE ||
           (id >= TDC_XFORM_QUANTIZE && id <= TDC_XFORM_BIT_SHUFFLE) ||
           (id >= 0x0100u && id <= 0x01FFu) ||
           (id >= 0xFF00u && id <= 0xFFFFu);
}

static int blk_entropy_id_valid(uint16_t id) {
    /* NONE is allowed as chain terminator; otherwise must be a known id,
     * an experimental id, or a user-defined id. */
    return id == TDC_ENTROPY_NONE ||
           (id >= TDC_ENTROPY_LZ && id <= TDC_ENTROPY_HUFFMAN4) ||
           (id >= 0x0100u && id <= 0x01FFu) ||
           (id >= 0xFF00u && id <= 0xFFFFu);
}

tdc_status tdc_block_record_validate(const tdc_block_record *r) {
    if (!r) return TDC_E_INVAL;

    if (r->magic   != TDC_BLOCK_MAGIC)   return TDC_E_CORRUPT;
    if (r->version != TDC_BLOCK_VERSION) return TDC_E_VERSION;
    if (r->_reserved0 != 0)              return TDC_E_CORRUPT;
    if (r->_reserved_pad != 0)           return TDC_E_CORRUPT;

    /* Validity bitmap presence flag must agree with the size field. The
     * two paths exist for self-description: the flag is the cheap "is
     * there one?" check; the size is the actual byte count. They must
     * not disagree, or callers reading either field get inconsistent
     * answers. */
    int has_validity_flag = (r->flags & TDC_BLOCK_FLAG_HAS_VALIDITY) != 0;
    if (has_validity_flag != (r->validity_size != 0u)) return TDC_E_CORRUPT;

    if (!blk_model_id_valid(r->model_id))     return TDC_E_CORRUPT;

    /* Walk the transform chain. Once a NONE is seen, every subsequent
     * slot must also be NONE — the chain terminator is sticky. */
    {
        int seen_terminator = 0;
        for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
            uint16_t id = r->xform_ids[i];
            if (!blk_xform_id_valid(id)) return TDC_E_CORRUPT;
            if (id == TDC_XFORM_NONE)    seen_terminator = 1;
            else if (seen_terminator)    return TDC_E_CORRUPT;
        }
    }

    /* Walk the entropy chain. Same sticky-terminator rule. An all-NONE
     * chain is legal and is interpreted by the driver as "passthrough". */
    {
        int seen_terminator = 0;
        for (int i = 0; i < TDC_MAX_ENTROPY; ++i) {
            uint16_t id = r->entropy_ids[i];
            if (!blk_entropy_id_valid(id)) return TDC_E_CORRUPT;
            if (id == TDC_ENTROPY_NONE)    seen_terminator = 1;
            else if (seen_terminator)      return TDC_E_CORRUPT;
        }
    }

    if (!blk_dtype_known(r->dtype))   return TDC_E_CORRUPT;
    if (!blk_layout_known(r->layout)) return TDC_E_CORRUPT;
    if ((int)r->rank != blk_layout_rank(r->layout)) return TDC_E_CORRUPT;
    if (r->rank == 0 || r->rank > TDC_MAX_RANK)     return TDC_E_CORRUPT;

    /* Dim sanity: non-negative, no n_elems overflow. */
    int64_t n = 1;
    for (uint8_t i = 0; i < r->rank; ++i) {
        int64_t d = r->dim[i];
        if (d < 0) return TDC_E_CORRUPT;
        if (d != 0 && n > INT64_MAX / d) return TDC_E_CORRUPT;
        n *= d;
    }

    /* Trailing dim slots beyond rank must be 0 per the format docs. */
    for (uint8_t i = r->rank; i < TDC_MAX_RANK; ++i) {
        if (r->dim[i] != 0) return TDC_E_CORRUPT;
    }

    return TDC_OK;
}
