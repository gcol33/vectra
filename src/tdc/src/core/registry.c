/*
 * src/core/registry.c
 *
 * Static id -> vtable lookup tables for the three pluggable stages:
 *   tdc_model_get   (tdc/model.h)
 *   tdc_xform_get   (tdc/transform.h)
 *   tdc_entropy_get (tdc/entropy.h)
 *
 * Core backends are dispatched by the static switches below. User-defined
 * backends (id range 0xFF00-0xFFFF) are registered at runtime via the
 * plugin API in tdc/plugin.h and looked up in the default case of each
 * switch.
 *
 * Adding a CORE backend means adding a vtable to the appropriate
 * src/{model,transform,entropy}/ file, declaring it in the matching
 * internal header, and listing it in the switch below.
 */

#include "tdc/model.h"
#include "tdc/transform.h"
#include "tdc/entropy.h"
#include "tdc/plugin.h"

#include "../entropy/entropy_internal.h"
#include "../model/model_internal.h"
#include "../transform/transform_internal.h"

#include <string.h>

/* ----- Plugin registry (user-defined backends) ----------------------------- */
/*
 * Each stage has a fixed-capacity array of (id, vtable pointer) pairs.
 * Registration appends; lookup is a linear scan (n <= TDC_PLUGIN_MAX_SLOTS).
 * Not thread-safe — all registration must precede concurrent encode/decode.
 */

typedef struct { uint16_t id; const void *vt; } plugin_slot;

static plugin_slot s_model_slots[TDC_PLUGIN_MAX_SLOTS];
static int         s_model_count;

static plugin_slot s_xform_slots[TDC_PLUGIN_MAX_SLOTS];
static int         s_xform_count;

static plugin_slot s_entropy_slots[TDC_PLUGIN_MAX_SLOTS];
static int         s_entropy_count;

/* Shared validation + insert. Returns TDC_OK on success. */
static tdc_status plugin_register(uint16_t id, const void *vt,
                                  plugin_slot *slots, int *count) {
    if (!vt)
        return TDC_E_INVAL;
    if (id < TDC_PLUGIN_ID_MIN || id > TDC_PLUGIN_ID_MAX)
        return TDC_E_INVAL;
    for (int i = 0; i < *count; ++i) {
        if (slots[i].id == id)
            return TDC_E_INVAL;     /* duplicate */
    }
    if (*count >= TDC_PLUGIN_MAX_SLOTS)
        return TDC_E_NOMEM;
    slots[*count].id = id;
    slots[*count].vt = vt;
    ++(*count);
    return TDC_OK;
}

/* Shared lookup. Returns NULL if not found. */
static const void *plugin_lookup(uint16_t id,
                                 const plugin_slot *slots, int count) {
    for (int i = 0; i < count; ++i) {
        if (slots[i].id == id)
            return slots[i].vt;
    }
    return NULL;
}

/* ----- Public registration functions -------------------------------------- */

tdc_status tdc_model_register(tdc_model_id id, const tdc_model_vt *vt) {
    if (vt && (uint16_t)vt->id != (uint16_t)id)
        return TDC_E_INVAL;
    return plugin_register((uint16_t)id, vt, s_model_slots, &s_model_count);
}

tdc_status tdc_xform_register(tdc_xform_id id, const tdc_xform_vt *vt) {
    if (vt && (uint16_t)vt->id != (uint16_t)id)
        return TDC_E_INVAL;
    return plugin_register((uint16_t)id, vt, s_xform_slots, &s_xform_count);
}

tdc_status tdc_entropy_register(tdc_entropy_id id, const tdc_entropy_vt *vt) {
    if (vt && (uint16_t)vt->id != (uint16_t)id)
        return TDC_E_INVAL;
    return plugin_register((uint16_t)id, vt, s_entropy_slots, &s_entropy_count);
}

void tdc_plugin_clear(void) {
    s_model_count   = 0;
    s_xform_count   = 0;
    s_entropy_count = 0;
    memset(s_model_slots,   0, sizeof(s_model_slots));
    memset(s_xform_slots,   0, sizeof(s_xform_slots));
    memset(s_entropy_slots, 0, sizeof(s_entropy_slots));
}

/* ----- Core lookup functions ---------------------------------------------- */

const tdc_model_vt *tdc_model_get(tdc_model_id id) {
    switch (id) {
        case TDC_MODEL_DELTA_1D: return &tdc_model_delta1d_vt;
        case TDC_MODEL_NONE:     return NULL;
        case TDC_MODEL_RAW:      return &tdc_model_raw_vt;
        case TDC_MODEL_DICT_1D:  return &tdc_model_dict1d_vt;
        case TDC_MODEL_PRED_2D:  return &tdc_model_pred2d_vt;
        case TDC_MODEL_STACK_2D: return &tdc_model_stack2d_vt;
        case TDC_MODEL_PRED_3D:  return &tdc_model_pred3d_vt;
        case TDC_MODEL_PLANE_2D: return &tdc_model_plane2d_vt;
        case TDC_MODEL_DELTA2_1D: return &tdc_model_delta2_1d_vt;
        case TDC_MODEL_FPC_1D:   return &tdc_model_fpc1d_vt;
        case TDC_MODEL_DICT_NUMERIC_1D: return &tdc_model_dict_numeric_1d_vt;
        case TDC_MODEL_SPARSE_ZERO_1D:  return &tdc_model_sparse_zero_1d_vt;
        case TDC_MODEL_QUANTIZE_PRED_2D: return &tdc_model_quantize_pred2d_vt;
        default:
            return (const tdc_model_vt *)plugin_lookup(
                (uint16_t)id, s_model_slots, s_model_count);
    }
}

const tdc_xform_vt *tdc_xform_get(tdc_xform_id id) {
    switch (id) {
        case TDC_XFORM_BYTE_SHUFFLE: return &tdc_xform_byte_shuffle_vt;
        case TDC_XFORM_NONE:         return NULL;
        case TDC_XFORM_QUANTIZE:     return &tdc_xform_quantize_vt;
        case TDC_XFORM_ZIGZAG:       return &tdc_xform_zigzag_vt;
        case TDC_XFORM_BIT_SHUFFLE:  return &tdc_xform_bit_shuffle_vt;
        case TDC_XFORM_COMPLEMENT:   return &tdc_xform_complement_vt;
        default:
            return (const tdc_xform_vt *)plugin_lookup(
                (uint16_t)id, s_xform_slots, s_xform_count);
    }
}

const tdc_entropy_vt *tdc_entropy_get(tdc_entropy_id id) {
    switch (id) {
        case TDC_ENTROPY_LZ:         return &tdc_entropy_lz_vt;
        case TDC_ENTROPY_LZ_OPT:     return &tdc_entropy_lz_opt_vt;
        case TDC_ENTROPY_LZ_SPLIT:   return &tdc_entropy_lz_split_vt;
        case TDC_ENTROPY_LZ_STREAMS: return &tdc_entropy_lz_streams_vt;
        case TDC_ENTROPY_NONE:    return &tdc_entropy_none_vt;
#ifdef TDC_HAVE_ZLIB
        case TDC_ENTROPY_DEFLATE: return &tdc_entropy_deflate_vt;
#else
        case TDC_ENTROPY_DEFLATE: return NULL;
#endif
        case TDC_ENTROPY_HUFFMAN: return &tdc_entropy_huffman_vt;
        case TDC_ENTROPY_HUFFMAN4: return &tdc_entropy_huffman4_vt;
        case TDC_ENTROPY_FSE:     return &tdc_entropy_fse_vt;
        case TDC_ENTROPY_LANE:    return &tdc_entropy_lane_vt;
        default:
            return (const tdc_entropy_vt *)plugin_lookup(
                (uint16_t)id, s_entropy_slots, s_entropy_count);
    }
}
