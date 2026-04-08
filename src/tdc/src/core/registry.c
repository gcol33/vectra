/*
 * src/core/registry.c
 *
 * Static id -> vtable lookup tables for the three pluggable stages:
 *   tdc_model_get   (tdc/model.h)
 *   tdc_xform_get   (tdc/transform.h)
 *   tdc_entropy_get (tdc/entropy.h)
 *
 * v0 has no public registration API. Adding a model/transform/entropy
 * means adding a vtable to the appropriate src/{model,transform,entropy}/
 * file, declaring it in the matching internal header, and listing it in
 * the switch below.
 *
 * Stages with no implementations yet (model, transform) return NULL for
 * every id; the encode/decode driver translates that into TDC_E_UNSUPPORTED.
 */

#include "tdc/model.h"
#include "tdc/transform.h"
#include "tdc/entropy.h"

#include "../entropy/entropy_internal.h"
#include "../model/model_internal.h"
#include "../transform/transform_internal.h"

const tdc_model_vt *tdc_model_get(tdc_model_id id) {
    switch (id) {
        case TDC_MODEL_DELTA_1D: return &tdc_model_delta1d_vt;
        case TDC_MODEL_NONE:     return NULL;
        case TDC_MODEL_RAW:      return &tdc_model_raw_vt;
        case TDC_MODEL_DICT_1D:  return &tdc_model_dict1d_vt;
        case TDC_MODEL_PRED_2D:  return &tdc_model_pred2d_vt;
        case TDC_MODEL_STACK_2D: return NULL; /* not yet extracted */
        case TDC_MODEL_PRED_3D:  return NULL; /* not yet extracted */
        case TDC_MODEL_PLANE_2D: return &tdc_model_plane2d_vt;
        default:                 return NULL;
    }
}

const tdc_xform_vt *tdc_xform_get(tdc_xform_id id) {
    switch (id) {
        case TDC_XFORM_BYTE_SHUFFLE: return &tdc_xform_byte_shuffle_vt;
        case TDC_XFORM_NONE:         return NULL;
        case TDC_XFORM_QUANTIZE:     return &tdc_xform_quantize_vt;
        case TDC_XFORM_ZIGZAG:       return &tdc_xform_zigzag_vt;
        case TDC_XFORM_BIT_SHUFFLE:  return NULL; /* reserved, post-v0 */
        default:                     return NULL;
    }
}

const tdc_entropy_vt *tdc_entropy_get(tdc_entropy_id id) {
    switch (id) {
        case TDC_ENTROPY_LZ2:     return &tdc_entropy_lz2_vt;
        case TDC_ENTROPY_NONE:    return &tdc_entropy_none_vt;
        case TDC_ENTROPY_DEFLATE: return NULL; /* not yet implemented */
        default:                  return NULL;
    }
}
