/*
 * src/model/model_internal.h
 *
 * Internal header listing extern declarations for every model vtable.
 * Not part of the public include/ tree. The single consumer is
 * src/core/registry.c, which dispatches a tdc_model_id to the matching
 * vtable address.
 *
 * Adding a new model backend means:
 *   1. defining `const tdc_model_vt tdc_model_<name>_vt` in the
 *      backend's .c file (no `static`),
 *   2. adding an `extern` declaration here,
 *   3. adding the case to tdc_model_get() in src/core/registry.c.
 */

#ifndef TDC_MODEL_INTERNAL_H
#define TDC_MODEL_INTERNAL_H

#include "tdc/model.h"

#ifdef __cplusplus
extern "C" {
#endif

extern const tdc_model_vt tdc_model_raw_vt;
extern const tdc_model_vt tdc_model_delta1d_vt;
extern const tdc_model_vt tdc_model_dict1d_vt;
extern const tdc_model_vt tdc_model_pred2d_vt;
extern const tdc_model_vt tdc_model_plane2d_vt;

#ifdef __cplusplus
}
#endif

#endif /* TDC_MODEL_INTERNAL_H */
